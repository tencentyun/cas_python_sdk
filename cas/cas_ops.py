# -*- coding=UTF-8 -*-
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
import datetime
import json
import os
import sys
import time
import hashlib
import binascii
from collections import namedtuple
import ConfigParser

from cas_api import CASAPI
from cas_util import UTC
from cas.ease.utils import *

CAS_PREFIX = 'cas://'
DEFAULT_HOST = 'cas.ap-chengdu.myqcloud.com'
DEFAULT_PORT = 80
DEFAULT_CONFIGFILE = os.path.expanduser('~') + '/.cascmd_credentials'
CONFIGSECTION = 'CASCredentials'

def check_response(res):
    try:
        if res.status / 100 != 2:
            errmsg = ''
            errmsg += 'Error Headers:\n'
            errmsg += str(res.getheaders())
            errmsg += '\nError Body:\n'
            errmsg += res.read(1024)
            errmsg += '\nError Status:\n'
            errmsg += str(res.status)
            errmsg += '\nFailed!\n'
            sys.stderr.write(errmsg)
            sys.exit(1)
    except AttributeError, e:
        sys.stderr.write('Error: check response status failed! msg: %s\n' % e)
        sys.exit(1)

def parse_vault_name(path):
    if not path.lower().startswith(CAS_PREFIX):
        sys.stderr.write('cas vault path must start with %s\n' % CAS_PREFIX)
        sys.exit(1)
    path_fields = path[len(CAS_PREFIX):].split('/')
    name = path_fields[0]
    return name

class CASCMD(object):

    def __init__(self, auth_info):
        self.api = CASAPI(auth_info.endpoint, auth_info.appid,
                auth_info.secretid, auth_info.secretkey)

    def byte_humanize(self, byte):
        if byte == None: return ''
        unitlist = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        unit = unitlist[0]
        for i in range(len(unitlist)):
            unit = unitlist[i]
            if int(byte) // 1024 == 0:
                break
            byte = float(byte) / 1024
        return '%.2f %s' % (byte, unit)

    def parse_size(self, size):
        try:
            if size == None: return size
            if isinstance(size, (int, long)): return size
            size = size.rstrip('B')
            if size.isdigit(): return int(size)
            byte, unit = int(size[:-1]), size[-1].upper()
            unitlist = ['K', 'M', 'G', 'T', 'P']
            if unit not in unitlist:
                byte = int(size)
            else:
                idx = unitlist.index(unit)
                for _ in range(idx + 1): byte *= 1024
            return byte
        except Exception, e:
            sys.stderr.write('[Error]: Inapropriate size %s provided\n' % size)
            sys.stderr.write('Only numbers, optionally ended with K, M, G, ' \
                             'T, P are accepted, units are case insensitive\n')
            sys.exit(1)

    def kv_print(self, idict, title=None):
        keys = title or idict.keys()
        maxlen = max([len(t) for t in keys])
        fmt = u'{:>{lname}}: {}'
        for k in keys:
            print fmt.format(k, idict[k], lname=maxlen)

    def cmd_ls(self, args):
        return self.cmd_list_vault(args)

    def cmd_cv(self, args):
        return self.cmd_create_vault(args)

    def cmd_rm(self, args):
        if not args.archive_id:
            return self.cmd_delete_vault(args)
        return self.cmd_delete_archive(args)

    def cmd_upload_archive(self, args):
        return self.cmd_upload(args)

    def cmd_upload(self, args):
        if not args.local_file or not os.path.isfile(args.local_file):
            sys.stderr.write("Error: file '%s' not existed\n" % args.local_file)
            sys.exit(1)
        size = os.path.getsize(args.local_file)
        desc = args.desc or args.local_file[:128]
        # 默认推荐单文件上传的阈值
        DEFAULT_NORMAL_UPLOAD_THRESHOLD = 100 * 1024 * 1024
        # 推荐的分块大小
        RECOMMOND_MIN_PART_SIZE = 16 * 1024 * 1024
        # 最大分块个数
        MAX_PART_NUM = 10000
        if (not args.part_size and size >= DEFAULT_NORMAL_UPLOAD_THRESHOLD) or \
                (args.part_size and size > RECOMMOND_MIN_PART_SIZE):
            resume = 0
            if not args.upload_id:
                partsize = self.parse_size(args.part_size)
                if partsize:
                    if partsize % (DEFAULT_NORMAL_UPLOAD_THRESHOLD) != 0:
                        sys.stderr.write('Error: partsize must be divided by 16MB!\n')
                        sys.exit(1)
                    if partsize * MAX_PART_NUM < size:
                        print 'specified partsize too small, will be adjusted'
                    if partsize > size:
                        print 'specified partsize too large, will be adjusted'
                        while partsize > size:
                            partsize /= 2
                else:
                    print 'File larger than 100MB, multipart upload will be used'
                    partsize = 16 * 1024 * 1024
                while partsize * MAX_PART_NUM < size:
                    partsize *= 2
                nparts = size // partsize
                if size % partsize: nparts += 1
                print 'Use %s parts with partsize %s to upload' % \
                        (nparts, self.byte_humanize(partsize))

                Create = namedtuple('Namespace', ['vault', 'part_size', 'desc'])
                cargs = Create(args.vault, partsize, desc)
                upload_id = self.cmd_init_multiupload(cargs)
            else:
                upload_id = args.upload_id
                vault_name = parse_vault_name(args.vault)
                res = self.api.list_multipart(vault_name, upload_id, None, None)
                check_response(res)
                rjson = json.loads(res.read(), 'UTF8')
                plist = rjson['Parts']
                partsize = rjson['PartSizeInBytes']
                nparts = size // partsize
                if size % partsize: 
                    nparts += 1
                # 这里有个问题, 如果已上传的不是连续的, 那么就会漏掉中间的分块
                if plist:
                    resumebytes = max([int(x['RangeInBytes'].split('-')[1]) \
                                       for x in plist]) + 1
                    if resumebytes == size:
                        resume = nparts
                    else:
                        resume = resumebytes // partsize
                print 'Resume last upload with partsize %s' % \
                        self.byte_humanize(partsize)
            Part = namedtuple('Namespace', ['vault', 'upload_id', 'local_file',
                'start', 'end', 'etag', 'tree_etag'])
            start = 0
            etaglist = []
            for i in range(nparts):
                end = min(size, start+partsize)-1
                etag, tree_etag = compute_hash_from_file(args.local_file, start,
                                                         end-start+1)
                pargs = Part(args.vault, upload_id, args.local_file, start,
                             end, etag, tree_etag)
                start += partsize
                etaglist.append(binascii.unhexlify(tree_etag))
                if i < resume: continue
                print 'Uploading part %s...' % (i+1)
                self.cmd_upload_part(pargs)
            Complete = namedtuple('Namespace', ['vault', 'upload_id',
                                                'size', 'tree_etag'])
            etree = compute_combine_tree_etag_from_list(etaglist)
            cargs = Complete(args.vault, upload_id, size, etree)
            self.cmd_complete_multiupload(cargs)
            return
        if size <= RECOMMOND_MIN_PART_SIZE and args.part_size:
            print 'File smaller than 16MB, part-size will be ignored.'
        Post = namedtuple('Namespace', ['vault', 'local_file', 'etag',
                                        'tree_etag', 'desc'])
        etag, tree_etag = compute_hash_from_file(args.local_file)
        pargs = Post(args.vault, args.local_file, etag, tree_etag, desc)
        return self.cmd_postarchive(pargs)

    def cmd_fetch(self, args):
        return self.cmd_fetch_joboutput(args)

    def cmd_create_vault(self, args):
        vault_name = parse_vault_name(args.vault)
        res = self.api.create_vault(vault_name)
        check_response(res)
        vault_location = res.getheader('Location')
        print 'Vault Location: %s' % vault_location

    def cmd_delete_vault(self, args):
        vault_name = parse_vault_name(args.vault)
        res = self.api.delete_vault(vault_name)
        check_response(res)
        print 'Delete success'

    def cmd_list_vault(self, args):
        marker = args.marker
        limit = args.limit
        res = self.api.list_vault(marker, limit)
        check_response(res)
        rjson = json.loads(res.read(), 'UTF8')
        marker = rjson['Marker']
        vault_list = rjson['VaultList']
        print 'Marker: %s' % marker
        print 'Vault count: %s' % len(vault_list)
        print ''
        if vault_list:
            max_name = max(max([len(v['VaultName']) for v in vault_list]),
                           len('VaultName')) + 1
            fmt = u'{:{lname}} {:<26} {:<16} {:<12} {:<26}'
            title = ('VaultName', 'CreationDate', 'NumberOfArchives',
                     'TotalSize', 'LastInventoryDate')
            print fmt.format(*title, lname=max_name)
            print '-' * (84+max_name)
            for vault in vault_list:
                print fmt.format(
                     vault['VaultName'],
                     vault['CreationDate'],
                     vault['NumberOfArchives'],
                     self.byte_humanize(vault['SizeInBytes']),
                     vault['LastInventoryDate'],
                     lname=max_name)

    def cmd_desc_vault(self, args):
        vault_name = parse_vault_name(args.vault)
        res = self.api.desc_vault(vault_name)
        check_response(res)
        rjson = json.loads(res.read(), 'UTF8')
        title = ('VaultName', 'VaultQCS', 'CreationDate', 'NumberOfArchives',
                 'SizeInBytes', 'LastInventoryDate')
        self.kv_print(rjson, title)

    def cmd_postarchive(self, args):
        vault_name = parse_vault_name(args.vault)
        filepath = args.local_file
        desc = args.desc or args.local_file[:128]
        if not os.path.isfile(filepath):
            sys.stderr.write("Error: file '%s' not existed!\n" % filepath)
            sys.exit(1)

        file_size = os.path.getsize(filepath)
        etag, tree_etag = compute_hash_from_file(args.local_file)
        with open(filepath, 'rb') as fp:
            res = self.api.post_archive_from_reader(
                vault_name, fp, file_size, etag, tree_etag, desc)
            check_response(res)
        archive_id = res.getheader('x-cas-archive-id')
        print 'Archive ID: %s' % archive_id

    def cmd_delete_archive(self, args):
        vault_name = parse_vault_name(args.vault)
        res = self.api.delete_archive(vault_name, args.archive_id)
        check_response(res)
        print 'Delete success'

    def cmd_file_tree_etag(self, args):
        if not os.path.isfile(args.local_file):
            sys.stderr.write("Error: file '%s' not existed!\n" % args.local_file)
            sys.exit(1)
        etag, tree_etag = compute_hash_from_file(args.local_file)
        print "etag     :", etag
        print "tree_etag:", tree_etag

    def cmd_part_tree_etag(self, args):
        if not os.path.isfile(args.local_file):
            sys.stderr.write("Error: file '%s' not existed!\n" % args.local_file)
            sys.exit(1)
        start = self.parse_size(args.start)
        end = self.parse_size(args.end)
        if end % (1024*1024) == 0: end -= 1
        size = end - start + 1
        etag, tree_etag = compute_hash_from_file(args.local_file, start, size)
        print "etag     :", etag
        print "tree_etag:", tree_etag

    def cmd_init_multiupload(self, args):
        vault_name = parse_vault_name(args.vault)
        part_size = self.parse_size(args.part_size)
        desc = args.desc
        res = self.api.create_multipart_upload(vault_name, part_size, desc)
        check_response(res)
        upload_id = res.getheader('x-cas-multipart-upload-id')
        print 'MultiPartUpload ID: %s' % upload_id
        return upload_id

    def cmd_list_multiupload(self, args):
        vault_name = parse_vault_name(args.vault)
        marker = args.marker
        limit = args.limit
        res = self.api.list_multipart_upload(vault_name, marker, limit)
        check_response(res)
        rjson = json.loads(res.read(), 'UTF8')
        marker = rjson['Marker']
        upload_list = rjson['UploadsList']
        print 'Marker: %s' % marker
        print 'Multipart upload count: %s' % len(upload_list)
        if upload_list:
            print
            maxdesc = max(max([len(m['ArchiveDescription']) for m in upload_list]),
                          len('ArchiveDescription')) + 1
            fmt = u'{:<66} {:<26} {:<10} {:<{ldesc}}'
            title = ('MultipartUploadId', 'CreationDate', 'PartSize',
                     'ArchiveDescription')
            print fmt.format(*title, ldesc=maxdesc)
            print '-' * (105+maxdesc)
            for upload in upload_list:
                print fmt.format(
                     upload['MultipartUploadId'],
                     upload['CreationDate'],
                     self.byte_humanize(upload['PartSizeInBytes']),
                     upload['ArchiveDescription'],
                     ldesc=maxdesc)
            print

    def cmd_complete_multiupload(self, args):
        vault_name = parse_vault_name(args.vault)
        upload_id = args.upload_id
        file_size = self.parse_size(args.size)
        tree_etag = args.tree_etag if args.tree_etag else ''

        res = self.api.complete_multipart_upload(
            vault_name, upload_id, file_size,  tree_etag)
        check_response(res)
        archive_id = res.getheader('x-cas-archive-id')
        print 'Archive ID: %s' % archive_id

    def cmd_abort_multiupload(self, args):
        vault_name = parse_vault_name(args.vault)
        res = self.api.abort_multipart_upload(vault_name, args.upload_id)
        check_response(res)
        print 'Delete success'

    def cmd_upload_part(self, args):
        vault_name = parse_vault_name(args.vault)
        upload_id = args.upload_id
        filepath = args.local_file
        start = self.parse_size(args.start)

        if not filepath or not os.path.isfile(filepath):
            sys.stderr.write("Error: file '%s' not existed!\n" % filepath)
            sys.exit(1)

        totalsize = os.path.getsize(filepath)
        end = args.end
        size = long(end) - long(start) + 1
        etag = args.etag.upper() if args.etag else None
        tree_etag = args.tree_etag.upper() if args.tree_etag else None
        if not (etag and tree_etag):
            tmpsum, tmptree = compute_hash_from_file(args.local_file, start, size)
            etag = etag or tmpsum
            tree_etag = tree_etag or tmptree
        with open(filepath, 'rb') as file_reader:
            prange = '%s-%s' % (start, end)
            file_reader.seek(long(start))
            res = self.api.post_multipart_from_reader(
                vault_name, upload_id, file_reader, size,
                prange, etag, tree_etag)
            check_response(res)
        print 'Upload success'

    def cmd_list_part(self, args):
        vault_name = parse_vault_name(args.vault)
        upload_id = args.upload_id
        marker = args.marker
        limit = args.limit
        res = self.api.list_multipart(vault_name, upload_id, marker, limit)
        check_response(res)
        rjson = json.loads(res.read(), 'UTF8')
        rjson['CreationDate'] = rjson['CreationDate']
        title = ('MultipartUploadId', 'CreationDate', 'PartSizeInBytes',
                 'ArchiveDescription', 'Marker', 'VaultQCS')
        self.kv_print(rjson, title)
        part_list = rjson['Parts']
        print '-' * 88
        print 'Part count: %s' % len(part_list)
        if len(part_list) > 0:
            print 'Part Ranges: '
            maxlen = max([len(p['RangeInBytes']) for p in part_list])
            fmt = u'{:>{lrange}}: {}'
            for part in part_list:
                print fmt.format(part['RangeInBytes'], part['SHA256TreeHash'],
                                 lrange=maxlen)

    def cmd_create_job(self, args):
        vault_name = parse_vault_name(args.vault)
        archive_id = args.archive_id
        desc = args.desc
        start = self.parse_size(args.start)
        size = self.parse_size(args.size)
        tier = args.tier

        jtype = 'archive-retrieval' if args.archive_id else 'inventory-retrieval'
        if jtype == 'inventory-retrieval':
            if start or size:
                print 'Tip: Inventory-retrieval does NOT support range, ignored'
                start = size = None

        byte_range = None
        if (start != None and size != None):
            byte_range = '%s-%s' % (start, start+size-1)
        elif (start != None and size == None):
            byte_range = '%s-' % (start)
        elif (start == None and size != None):
            byte_range = '0-%s' % (size-1)
        if byte_range:
            print 'Archive retrieval range: %s' % byte_range

        res = self.api.create_job(
            vault_name, jtype, archive_id, desc, byte_range, tier)
        check_response(res)
        job_id = res.getheader('x-cas-job-id')
        print '%s job created, job ID: %s' % (jtype, job_id)
        print 'Use\n\n    cascmd.py fetch %s %s <local_file>\n\nto check job ' \
              'progress and download the data when job finished' % \
              (args.vault, job_id)
        print 'NOTICE: Jobs usually take about 4 HOURS to complete.'
        return job_id

    def cmd_desc_job(self, args):
        vault_name = parse_vault_name(args.vault)
        job_id = args.jobid
        res = self.api.get_jobdesc(vault_name, job_id)
        check_response(res)
        rjson = json.loads(res.read(), 'UTF8')

        title = ('JobId', 'Action', 'StatusCode', 'StatusMessage',
                'ArchiveId', 'ArchiveSizeInBytes',
                'SHA256TreeHash', 'ArchiveSHA256TreeHash',
                'RetrievalByteRange', 'Completed', 'CompletionDate',
                'CreationDate', 'InventorySizeInBytes', 'JobDescription',
                'Tier', 'VaultQCS')
        self.kv_print(rjson, title)

    def cmd_fetch_joboutput(self, args):
        vault_name = parse_vault_name(args.vault)
        job_id = args.jobid
        dst = args.local_file
        start = self.parse_size(args.start)
        size = self.parse_size(args.size)

        res = self.api.get_jobdesc(vault_name, args.jobid)
        check_response(res)
        job = json.loads(res.read(), 'UTF8')
        status = job['StatusCode'].lower()
        jtype = 'inventory-retrieval' if job['Action'] == 'InventoryRetrieval' \
                else 'archive-retrieval'
        if status == 'inprogress':
            print '%s job still in progress. Repeat this ' \
                  'command later' % jtype
            sys.exit(0)
        elif status == 'failed':
            print '%s job failed.' % jtype
            sys.exit(0)
        brange = None
        if jtype == 'archive-retrieval':
            brange = [int(x) for x in job['RetrievalByteRange'].split('-')]
        else:
            brange = [0, int(job['InventorySizeInBytes']) - 1]

        output_range = None
        if (start != None and size == None):
            size = brange[1] - brange[0] - start + 1
        elif (start == None and size != None):
            start = 0
        if (start != None and size != None):
            output_range = 'bytes=%s-%s' % (start, start+size-1)

        if not args.force and os.path.exists(dst):
            ans = raw_input('Output file %s existed. Do you wish to ' \
                    'overwrite it? (y/n): ' % dst)
            if ans.strip().lower() != 'y':
                print 'Answer is no. Quit now.'
                sys.exit(0)

        res = self.api.fetch_job_output(vault_name, job_id, output_range)
        check_response(res)

        with open(dst, 'wb') as f:
            total_read = 0
            while True:
                data = res.read(1024 * 1024)
                if len(data) > 0:
                    f.write(data)
                    total_read += len(data)
                    if jtype == 'inventory-retrieval':
                        continue
                    totalsize = size or brange[1] - brange[0] + 1
                    percent = total_read * 100 // totalsize
                    nbar = percent // 2
                    sys.stdout.write('\r')
                    msgbar = '[%s] %s%%' % ('='*nbar+'>'+' '*(50-nbar), percent)
                    sys.stdout.write(msgbar)
                    sys.stdout.flush()
                else:
                    break
        sys.stdout.write('\n')
        print 'Download job output success'

    def cmd_list_job(self, args):
        vault_name = parse_vault_name(args.vault)
        marker = args.marker
        limit = args.limit
        res = self.api.list_job(vault_name, marker, limit)
        check_response(res)
        rjson = json.loads(res.read(), 'UTF8')
        marker = rjson['Marker']
        job_list = rjson['JobList']
        print 'Marker: %s' % marker
        print 'Job count: %s' % len(job_list)
        if not job_list:
            return
        print
        title = ('JobId', 'Action', 'StatusCode', 'StatusMessage',
                'ArchiveId', 'ArchiveSizeInBytes',
                'SHA256TreeHash', 'ArchiveSHA256TreeHash',
                'RetrievalByteRange', 'Completed', 'CompletionDate',
                'CreationDate', 'InventorySizeInBytes', 'JobDescription',
                'VaultQCS')
        for job in job_list:
            print '================ JobId: %s ================' % job['JobId']
            self.kv_print(job, title)
            print

