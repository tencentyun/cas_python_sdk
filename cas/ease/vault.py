# -*- coding: utf-8 -*-

import mmap
import os

from api import APIProxy
from uploader import Uploader
from utils import *


class Vault(object):
    _MEGABYTE = 1024 * 1024

    NormalUploadThreshold = 32 * _MEGABYTE

    ResponseDataParser = (('CreationDate', 'creation_date', None),
                          ('LastInventoryDate', 'last_inventory_date', None),
                          ('NumberOfArchives', 'number_of_archives', 0),
                          ('SizeInBytes', 'size', 0),
                          ('VaultQCS', 'qcs', None),
                          ('VaultName', 'name', None))

    def __init__(self, api, response):
        self.api = api
        for response_name, attr_name, default in self.ResponseDataParser:
            value = response.get(response_name)
            setattr(self, attr_name, value or default)

    def __repr__(self):
        return 'Vault: %s' % self.name

    @classmethod
    def create_vault(cls, api, name):
        api = APIProxy(api)
        response = api.create_vault(name)
        # print '=== debug: create vault response: ', response
        response = api.describe_vault(name)
        # print '=== debug: describe vault response: ', response
        return Vault(api, response)

    @classmethod
    def get_vault_by_name(cls, api, vault_name):
        vaults = cls.list_all_vaults(api)
        for vault in vaults:
            if vault_name == vault.name:
                return vault
        raise ValueError('Vault not exists: %s' % vault_name)

    @classmethod
    def delete_vault_by_name(cls, api, vault_name):
        vaults = cls.list_all_vaults(api)
        for vault in vaults:
            if vault_name == vault.name:
                return vault.delete()
        raise ValueError('Vault not exists: %s' % vault_name)

    @classmethod
    def list_all_vaults(cls, api):
        api = APIProxy(api)
        result = api.list_vaults()
        return [Vault(api, data) for data in result['VaultList']]

    def list_all_multipart_uploads(self):
        result = self.api.list_multipart_uploads(self.name)
        return [Uploader(self, data) for data in result['UploadsList']]

    def list_all_jobs(self):
        result = self.api.list_jobs(self.name)
        return [Job(self, data) for data in result['JobList']]

    def delete(self):
        return self.api.delete_vault(self.name)

    def upload_archive(self, file_path, desc=None):
        length = os.path.getsize(file_path)
        if length > self.NormalUploadThreshold:
            return self.initiate_uploader(file_path, desc=desc).start()
        elif length > 0:
            return self._upload_archive_normal(file_path, desc=desc)
        else:
            raise ValueError('CAS does not support zero byte archive.')

    def _upload_archive_normal(self, file_path, desc):
        f = open_file(file_path=file_path)
        with f:
            file_size = content_length(f)
            etag = compute_etag_from_file(file_path)
            tree_etag = compute_tree_etag_from_file(file_path)
            mmapped_file = mmap.mmap(f.fileno(), length=file_size, offset=0,
                                     access=mmap.ACCESS_READ)
            try:
                response = self.api.upload_archive(self.name, mmapped_file,
                                                   etag, tree_etag,
                                                   size=file_size, desc=desc)
            finally:
                mmapped_file.close()
        # print '=== debug: upload archive normal response: ', response
        return response['x-cas-archive-id']

    def initiate_uploader(self, file_path, desc=None):
        f = open_file(file_path=file_path)
        with f:
            size_total = content_length(f)
        part_size = Uploader.calc_part_size(size_total)

        response = self.api.initiate_multipart_upload(
            self.name, part_size, desc=desc)
        upload_id = response['x-cas-multipart-upload-id']

        response = self.api.describe_multipart(self.name, upload_id)
        return Uploader(self, response, file_path=file_path)

    def recover_uploader(self, upload_id):
        response = self.api.describe_multipart(self.name, upload_id)
        return Uploader(self, response)

    def delete_archive(self, archive_id):
        return self.api.delete_archive(self.name, archive_id)

    # todo, download job
