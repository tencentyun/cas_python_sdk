# -*- coding: utf-8 -*-

from __future__ import division

import sys
import logging
import random
import time
import cas.conf.common_conf

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from multiprocessing.pool import ThreadPool

from cas.utils.merkle import *
from cas.utils.file_utils import *
from cas.conf.common_conf import MEGABYTE
from cas.conf.common_conf import GIGABYTE
from cas.conf import multi_task_conf
from cas.exceptions.cas_server_error import CASServerError
from cas.exceptions.cas_client_error import UploadArchiveError
from cas.exceptions.cas_client_error import HashDoesNotMatchError

log = logging.getLogger(__name__)


class MultipartUpload(object):

    _MinimumPartSize = cas.conf.common_conf.MultipartUpload_MinimumPartSize
    _MaximumNumberOfParts = cas.conf.common_conf.Multipart_Upload_MaximumNumberOfParts
    _NumberThread = multi_task_conf.MultipartUpload_NumberThread
    _NumberRetry = multi_task_conf.MultipartUpload_NumberRetry

    ResponseDataParser = (('ArchiveDescription', 'description', None),
                          ('CreationDate', 'creation_date', None),
                          ('MultipartUploadId', 'id', None),
                          ('PartSizeInBytes', 'part_size', 0))

    def __init__(self, vault, cas_response, file_path=None):
        self.vault = vault
        for response_name, attr_name, default in self.ResponseDataParser:
            value = cas_response.get(response_name)
            setattr(self, attr_name, value or default)
        self.parts = OrderedDict()
        self.file_path = file_path
        self.size_total = 0
        if self.file_path is not None:
            self._prepare(self.file_path)

    def __repr__(self):
        return 'Multipart Upload: %s' % self.id

    @classmethod
    def calc_part_size(cls, size_total, part_size=_MinimumPartSize):
        if size_total > 4 * 10000 * GIGABYTE:                       # 单文件最大支持40TB
            raise ValueError('file too big: %d' % size_total)

        if size_total < cls._MinimumPartSize:
            raise ValueError('file too small: %d, '
                             'please use vault.upload_archive' % size_total)

        if part_size % MEGABYTE != 0 \
                or part_size < cls._MinimumPartSize \
                or part_size > size_total:
            part_size = cls._MinimumPartSize

        number_parts = calc_num_part(part_size, size_total)
        if number_parts > cls._MaximumNumberOfParts:
            part_size_refer = math.ceil(
                size_total / cls._MaximumNumberOfParts / MEGABYTE)
            part_size_refer = int(part_size_refer * MEGABYTE)
            part_bit_len = part_size_refer.bit_length() - 1
            part_size = 1 << part_bit_len
            while True:
                if part_size >= part_size_refer:
                    break
                part_bit_len += 1
                part_size = 1 << part_bit_len
        return part_size

    @classmethod
    def parse_range_from_str(cls, string):
        return tuple([int(num) for num in string.split('-')])

    def _prepare(self, file_path):
        self.file_path = file_path
        f = open_file(self.file_path)
        with f:
            self.size_total = content_length(f)
        for byte_range in calc_ranges(self.part_size, self.size_total):
            self.parts[byte_range] = None

    def resume(self, file_path):
        self._prepare(file_path)
        result = self.vault.api.list_all_parts(self.vault.name, self.id)

        for part in result['Parts']:
            byte_range = self.parse_range_from_str(part['RangeInBytes'])
            self.parts[byte_range] = part['SHA256TreeHash']
            tree_etag = compute_tree_etag_from_file(
                self.file_path, offset=byte_range[0],
                size=range_size(byte_range))
            if tree_etag != self.parts[byte_range]:
                raise HashDoesNotMatchError(
                    'Hash does not match for %d-%d: %s, which %s excepted' %
                    (byte_range[0], byte_range[1], tree_etag,
                     self.parts[byte_range]))

        return self.start()

    def start(self):
        def upload_part(byte_range):
            try:
                time.sleep(random.randint(256, 4096) / 1000)
                offset = byte_range[0]
                size = range_size(byte_range)
                etag = compute_etag_from_file(self.file_path, offset=offset, size=size)
                tree_etag = compute_tree_etag_from_file(self.file_path, offset=offset, size=size)

                f = open_file(self.file_path)
                with f:
                    for cnt in xrange(self._NumberRetry):
                        try:
                            if self.part_size % mmap.ALLOCATIONGRANULARITY == 0:
                                target = mmap.mmap(f.fileno(), length=size,
                                                   offset=offset,
                                                   access=mmap.ACCESS_READ)
                            else:
                                f.seek(offset)
                                target = f

                            if range_size(byte_range) > content_length(target):
                                raise ValueError('Byte range exceeded : %d-%d', byte_range)
                            self.vault.api.post_multipart_from_reader(self.vault.name, self.id, target,
                                                                      range_size(byte_range), '%d-%d' % byte_range,
                                                                      etag, tree_etag)
                            self.parts[byte_range] = tree_etag
                            log.info('Range %d-%d upload success.' % byte_range)
                            return
                        except CASServerError as e:
                            log.error('upload %s range %d-%d upload failed. etag: %s. Reason: %s' %
                                      (self.id, byte_range[0], byte_range[1], etag, e))
                            if e.code != 'InvalidDigest' or e.type != 'client':
                                return
                        except IOError as e:
                            log.error('uploadid %s range %d-%d upload failed. Reason: %s' %
                                      (self.id, byte_range[0], byte_range[1], e))
                            continue
                        except Exception as e:
                            log.error('upload %s range %d-%d upload failed. etag: %s. Error: %s' %
                                      (self.id, byte_range[0], byte_range[1], etag, e))
                            continue
                        finally:
                            if 'target' in locals() and target is not f:
                                target.close()
            except Exception as e:
                log.error('Upload %s range %d-%d upload finally failed. Reason: %s' %
                               (self.id, byte_range[0], byte_range[1], e))
                raise

        log.info('Start upload %s from %s.' % (self.id, self.file_path))
        try:
            pool = ThreadPool(processes=min(self._NumberThread, len(self.parts)))
            pool.map(upload_part, [byte_range
                                   for byte_range, tag in self.parts.items()
                                   if tag is None])

            size = self.size_completed
            if size != self.size_total:
                raise UploadArchiveError('Incomplete upload %s : %d / %d' % (self.id, size, self.size_total))
            response = self.vault.api.complete_multipart_upload(
                self.vault.name, self.id, self.size_total,
                self.tree_hash)
            log.debug('debug: send complete part res: %s\n' % response)
            log.info('Upload %s finish.' % (self.id))
            return response.get('x-cas-archive-id')
        except UploadArchiveError as e:
            error_info = 'upload %s failed, cause:%s' % (self.id, e)
            log.error(error_info)
            raise e
        except Exception as e:
            error_info = 'upload %s failed, cause:%s' % (self.id, e)
            log.error(error_info)
            raise ValueError(error_info)

    def cancel(self):
        return self.vault.api.abort_multipart_upload(self.vault.name, self.id)

    @property
    def size_completed(self):
        size_list = [range_size(byte_range)
                     for byte_range, tag in self.parts.items()
                     if tag is not None]
        return sum(size_list)

    @property
    def tree_hash(self):
        # need binary data, not ascii char !
        etag_list = [binascii.a2b_hex(tag) for _, tag in self.parts.items() if tag is not None]
        return MerkleTree().load(etag_list).digest() \
            if len(etag_list) == len(self.parts) else ''
