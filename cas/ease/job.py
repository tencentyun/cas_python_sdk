# -*- coding: utf-8 -*-
# Copyright (C) 2011  Alibaba Cloud Computing
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#
import logging
import random
import threading
import binascii
import time
import yaml
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from multiprocessing.pool import ThreadPool

from exceptions import *
from merkle import MerkleTree, TreeHashGenerator
from utils import *


log = logging.getLogger(__name__)


class Job(object):
    _MEGABYTE = 1024 * 1024

    NumberThread = 16
    NumRetry = 3

    ResponseDataParser = (('Action', 'action', None),
                          ('ArchiveSHA256TreeHash', 'archive_etag', None),
                          ('ArchiveId', 'archive_id', None),
                          ('ArchiveSizeInBytes', 'archive_size', 0),
                          ('Completed', 'completed', False),
                          ('CompletionDate', 'completion_date', None),
                          ('CreationDate', 'creation_date', None),
                          ('InventorySizeInBytes', 'inventory_size', 0),
                          ('JobDescription', 'description', None),
                          ('JobId', 'id', None),
                          ('RetrievalByteRange', 'job_range', None),
                          ('StatusCode', 'status_code', None),
                          ('StatusMessage', 'status_message', None),
                          ('Tier', 'tier', None),
                          ('SHA256TreeHash', 'retrieval_etag', None))

    def __init__(self, vault, response):
        self.vault = vault
        self.parts = OrderedDict()
        self._update(response)

    def __repr__(self):
        return 'Job: %s' % self.id

    def _parse_job_range(self):
        return tuple([int(v) for v in self.job_range.split('-')])

    def _is_complete_download(self):
        byte_range = self._parse_job_range()
        return byte_range == (0, self.archive_size - 1)

    def _is_tree_hash_align(self):
        return self.retrieval_etag is not None

    def calc_part_size(self, size_total):
        part_size = 32*1024*1024

        number_parts = calc_num_part(part_size, size_total)
        if number_parts > 10000:
            part_size_refer = math.ceil(
                size_total / 10000 / (1024*1024))
            part_size_refer = int(part_size_refer * 1024*1024)
            part_bit_len = part_size_refer.bit_length() - 1
            part_size = 1<< part_bit_len
            while True:
                if(part_size >= part_size_refer):
                    break
                part_bit_len += 1
                part_size = 1<< part_bit_len 
        return part_size

    def _update(self, response):
        for response_name, attr_name, default in self.ResponseDataParser:
            value = response.get(response_name)
            setattr(self, attr_name, value or default)

        self.parts = OrderedDict()
        if self.archive_size > 0 and self.action == "ArchiveRetrieval":
            tmp_byte_range = self._parse_job_range()
            size = range_size(tmp_byte_range)
            if size > 32*1024*1024:
                part_size = self.calc_part_size(size)
                for byte_range in calc_ranges(part_size, size):
                    self.parts[byte_range] = None
            else:
                self.parts[(0, size - 1)] = None

    def update_status(self):
        response = self.vault.api.describe_job(self.vault.name, self.id)
        self._update(response)

    def _check_status(self, block=False):
        self.update_status()
        if not block:
            if not self.completed:
                raise DownloadArchiveError('Job not ready')
            elif self.status_code.lower() == 'failed':
                raise DownloadArchiveError('Job process failed')
        elif block:
            while not self.completed:
                log.info('Job :'+ self.id +' status: ' + self.status_code)
                time.sleep(random.randint(6, 9))
                self.update_status()

    def download_by_range(self, byte_range, file_path=None, file_obj=None,
                          chunk_size=None, block=True):
        if self.action == "PullFromOSS" or self.action == "PushToOSS":
            raise DownloadArchiveError('Job not ready')

        self._check_status(block)

        chunk_size = chunk_size or self._MEGABYTE
        f = open_file(file_path=file_path, file_obj=file_obj, mode='wb+')
        offset = f.tell() if file_obj is not None else 0
        size = range_size(byte_range)

        try:
            for cnt in xrange(self.NumRetry):
                pos = 0
                try:
                    response = self.vault.api.get_job_output(
                        self.vault.name, self.id, byte_range=byte_range)
                    while True:
                        data = response.read(chunk_size)
                        if not data:
                            break
                        f.write(data)
                        pos += len(data)

                    if pos == size:
                        return
                except CASServerError as e:
                    log.error('Range %d-%d download failed. CASServerError Reason: %s' %
                              (byte_range[0], byte_range[1], e))
                    if e.type != 'client':
                        f.seek(offset)
                        continue
                    else:
                        raise e
                except IOError as e:
                    log.error('Range %d-%d download failed. IOError Reason: %s' %
                              (byte_range[0], byte_range[1], e))
                    f.seek(offset)
                    continue
                except Exception as e:
                    log.error('Range %d-%d download failed. Exception Reason: %s' %
                              (byte_range[0], byte_range[1], e))
                    f.seek(offset)
                    continue
        finally:
            f.flush()
            if f is not file_obj:
                f.close()

        raise DownloadArchiveError(
            'Incomplete download: %d / %d' % (pos, size))

    def download_to_file(self, file_path, chunk_size=None, block=True):
        if self.action == "PullFromCOS" or self.action == "PushToCOS":
            raise DownloadArchiveError('Job not ready')

        self._check_status(block)
        chunk_size = chunk_size or self._MEGABYTE
        if self.inventory_size > 0:
            return self.download_by_range(
                file_path=file_path,
                byte_range=(0, self.inventory_size - 1))

        file_dir, file_name = os.path.split(file_path)
        log_file = os.path.join(file_dir, file_name + '.cas')
        try:
            self._load(log_file)
        except IOError:
            pass
        log_lock = threading.RLock()
        file_lock = threading.RLock()

        def download_part(byte_range):
            for cnt in xrange(self.NumRetry):
                time.sleep(random.randint(256, 4096) / 1000.)
                try:
                    response = self.vault.api.get_job_output(
                        self.vault.name, self.id, byte_range=byte_range)

                    generator = TreeHashGenerator()
                    offset = byte_range[0]
                    while True:
                        data = response.read(chunk_size)
                        if not data:
                            break
                        generator.update(data)

                        with file_lock:
                            f.seek(offset)
                            f.write(data)
                            f.flush()
                            offset += len(data)

                    if offset != byte_range[1] + 1:
                        log.error('Range %d-%d(%d) incomplete download.' %
                                  (byte_range[0], byte_range[1], offset))
                        continue
                    tree_etag = generator.generate().digest()
                    if self._is_tree_hash_align() and \
                            tree_etag != response['x-cas-sha256-tree-hash']:
                        log.error('Range %d-%d invalid checksum %s, '
                                  'which %s expected.' %
                                  (byte_range[0], byte_range[1], tree_etag,
                                   response['x-cas-sha256-tree-hash']))
                        continue
                    else:
                        self.parts[byte_range] = tree_etag
                        with log_lock:
                            self._save(log_file)
                        log.info('Range %d-%d download success.' % byte_range)
                        return
                except (CASServerError, IOError) as e:
                    log.error('Range %d-%d download failed. Reason: %s' %
                              (byte_range[0], byte_range[1], e))
                    continue
                except Exception as e:
                    log.error('Range %d-%d download failed. The reason: %s' %
                              (byte_range[0], byte_range[1], e))
                    continue

            log.info('Range %d-%d download failed.' % byte_range)

        f = open_file(file_path=file_path, mode='wb+')
        with f:
            log.info('Start download.')
            pool = ThreadPool(
                            processes=min(self.NumberThread, len(self.parts)))
            pool.map(download_part,
                     [byte_range for byte_range, tag in self.parts.items()
                      if tag is None])

            size = self.size_completed
            size_total = range_size(self._parse_job_range())
            if size != size_total:
                raise DownloadArchiveError(
                    'Incomplete download: %d / %d' %
                    (size, size_total))
            if self._is_tree_hash_align() == True:
                tree_etag_list = [binascii.unhexlify(tree_etag) for _,tree_etag in self.parts.items()]
                tree_etag_actual = compute_combine_tree_etag_from_list(tree_etag_list)
                if tree_etag_actual != self.retrieval_etag:
                    raise HashDoesNotMatchError(
                            'tree-etag not match: %s / %s (actual)' %
                            (tree_etag_actual, self.retrieval_etag))

            if os.path.exists(log_file):
                os.remove(log_file)

        log.info('Download finish.')

    def _save(self, file_path):
        f = open_file(file_path=file_path, mode='wb+')
        with f:
            f.write(yaml.dump(self.parts, default_flow_style=True))

    def _load(self, file_path):
        f = open_file(file_path=file_path)
        with f:
            self.parts = yaml.load(f)

    @property
    def size_completed(self):
        size_list = [range_size(byte_range)
                     for byte_range,tree_etag in self.parts.items()
                     if tree_etag is not None]
        return sum(size_list)
