# -*- coding: utf-8 -*-

from cas.cas_api import CASAPI

from exceptions import CASServerError
from response import CASResponse
from utils import *


class APIProxy(object):

    def __init__(self, api):
        self.api = api

    def __getattr__(self, name):
        methods = [
            'create_vault', 'delete_vault', 'list_vault', 'get_vault_desc',
            'post_archive', 'post_archive_from_reader', 'delete_archive', 'head_archive',

            'create_multipart_upload', 'list_multipart_upload',
            'complete_multipart_upload', 'delete_multipart_upload',

            'post_multipart', 'post_multipart_from_reader', 'list_multipart',

            'create_job', 'create_oss_transfer_job', 'get_jobdesc', 'fetch_job_output', 'list_job']

        transform = {'describe_vault': 'get_vault_desc',
                     'describe_job': 'get_jobdesc',
                     'describe_multipart': 'list_multipart',
                     'initiate_multipart_upload': 'create_multipart_upload',
                     'cancel_multipart_upload': 'delete_multipart_upload'}

        if name in transform:
            name = transform[name]

        if name not in methods:
            return object.__getattribute__(self, name)

        def wrapped(*args, **kwargs):
            try:
                func = CASAPI.__getattribute__(self.api, name)
                res = func(*args, **kwargs)
            except Exception as e:
                raise IOError(str(e))

            if res.status / 100 == 2:
                return CASResponse(res)
            else:
                raise CASServerError(res)
        return wrapped

    def list_vaults(self):
        result = self.list_vault()
        marker = result['Marker']
        while marker:
            res = self.list_vault(marker=marker)
            result['VaultList'].extend(res['VaultList'])
            marker = res['Marker']
        result['Marker'] = None
        return result

    def list_multipart_uploads(self, vault_name):
        result = self.list_multipart_upload(vault_name)
        marker = result['Marker']
        while marker:
            res = self.list_multipart_upload(vault_name, marker=marker)
            result['UploadsList'].extend(res['UploadsList'])
            marker = res['Marker']
        result['Marker'] = None
        return result

    def list_parts(self, vault_name, upload_id):
        result = self.list_multipart(vault_name, upload_id)
        marker = result['Marker']
        while marker:
            res = self.list_multipart(vault_name, upload_id, marker=marker)
            result['Parts'].extend(res['Parts'])
            marker = res['Marker']
        result['Marker'] = None
        return result

    def list_jobs(self, vault_name):
        result = self.list_job(vault_name)
        marker = result['Marker']
        while marker:
            res = self.list_job(vault_name, marker=marker)
            result['JobList'].extend(res['JobList'])
            marker = res['Marker']
        result['Marker'] = None
        return result

    def upload_archive(self, vault_name, content, etag, tree_etag,
                    size=None, desc=None):
        if is_file_like(content):
            return self.post_archive_from_reader(
                vault_name, content, min(size, content_length(content)),
                etag, tree_etag, desc=desc)
        else:
            return self.post_archive(vault_name, content,
                                     etag, tree_etag, desc=desc)

    def upload_part(self, vault_name, upload_id, content, byte_range,
                    etag, tree_etag):
        if is_file_like(content):
            size_total = content_length(content)
            if range_size(byte_range) > size_total:
                raise ValueError('Byte range exceeded: %d-%d' % byte_range)
            return self.post_multipart_from_reader(
                vault_name, upload_id, content, range_size(byte_range),
                '%d-%d' % byte_range, etag, tree_etag)
        else:
            return self.post_multipart(
                vault_name, upload_id, content,
                '%d-%d' % byte_range, etag, tree_etag)

    def get_job_output(self, vault_name, job_id, byte_range=None):
        return self.fetch_job_output(vault_name, job_id,
                                     'bytes=%d-%d' % byte_range)
