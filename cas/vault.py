# -*- coding: utf-8 -*-
import sys

from cas.utils.file_utils import *
from cas.archive import Archive
from cas.conf.common_conf import DEFAULT_NORMAL_UPLOAD_THRESHOLD, CAS_PREFIX
from job import Job
from multipart_upload import MultipartUpload


class Vault(object):
    NormalUploadThreshold = DEFAULT_NORMAL_UPLOAD_THRESHOLD

    ResponseDataParser = (('CreationDate', 'creation_date', None),
                          ('LastInventoryDate', 'last_inventory_date', None),
                          ('NumberOfArchives', 'number_of_archives', 0),
                          ('SizeInBytes', 'size', 0),
                          ('VaultQCS', 'qcs', None),
                          ('VaultName', 'name', None))

    def __init__(self, cas_api, vault_props):
        """
        vault 的构造器
        :param cas_api: CasAPI对象，包含客户端的访问信息以及SDK的基本调用接口
        :param vault_props: vault的属性信息
        """
        self.api = cas_api
        for response_name, attr_name, default in self.ResponseDataParser:
            value = vault_props.get(response_name)
            setattr(self, attr_name, value or default)

    def __repr__(self):
        return 'Vault: %s' % self.name

    @classmethod
    def create(cls, cas_api, name):
        api = cas_api
        response = api.create_vault(name)
        response = api.describe_vault(name)
        return Vault(api, response)

    @classmethod
    def get_vault_by_name(cls, cas_api, vault_name):
        vaults = cls.list_all_vaults(cas_api)
        for vault in vaults:
            if vault_name == vault.name:
                return vault
        raise ValueError('Vault not exists: %s' % vault_name)

    @classmethod
    def delete_vault_by_name(cls, cas_api, vault_name):
        vaults = cls.list_all_vaults(cas_api)
        for vault in vaults:
            if vault_name == vault.name:
                return vault.delete()
        raise ValueError('Vault not exists: %s' % vault_name)

    @classmethod
    def list_all_vaults(cls, cas_api):
        api = cas_api
        result = api.list_all_vaults()
        return [Vault(api, data) for data in result['VaultList']]

    def list_all_multipart_uploads(self):
        result = self.api.list_all_multipart_uploads(self.name)
        return [MultipartUpload(self, data) for data in result['UploadsList']]

    def get_archive(self, archive_id):
        return Archive(self, archive_id)

    def retrieve_archive(self, archive_id, desc=None, byte_range=None, tier=None):
        byte_range_str = None
        if byte_range is not None:
            byte_range_str = '%d-%d' % byte_range
        response = self.api.initiate_job(self.name, 'archive-retrieval',
                                         archive_id=archive_id, desc=desc,
                                         byte_range=byte_range_str, tier=tier)
        return self.get_job(response['x-cas-job-id'])

    def initiate_retrieve_inventory(self, desc=None):
        response = self.api.initiate_job(self.name, 'inventory-retrieval', desc=desc)
        return self.get_job(response['x-cas-job-id'])

    def upload_archive(self, file_path, desc=None):
        length = os.path.getsize(file_path)
        if length > self.NormalUploadThreshold:
            uploader = self.initiate_multipart_upload(file_path, desc=desc)
            print "====== start the multipart upload: ", uploader
            archive_id = uploader.start()
            return archive_id
        elif length > 0:
            with open_file(file_path=file_path) as content:
                cas_response = self.api.upload_archive(self.name, content,
                                                       etag=compute_etag_from_file(file_path),
                                                       tree_tag=compute_tree_etag_from_file(file_path),
                                                       size=content_length(content), desc=desc)
                return cas_response['x-cas-archive-id']
        else:
            raise ValueError('CAS does not support zero byte archive.')

    def delete_archive(self, archive_id):
        return self.api.delete_archive(self.name, archive_id)

    def initiate_multipart_upload(self, file_path, desc=None):
        f = open_file(file_path=file_path)
        with f:
            size_total = content_length(f)

        part_size = MultipartUpload.calc_part_size(size_total)
        cas_response = self.api.initiate_multipart_upload(self.name, part_size, desc=desc)
        upload_id = cas_response['x-cas-multipart-upload-id']
        cas_response = self.api.describe_multipart(self.name, upload_id)
        return MultipartUpload(self, cas_response, file_path=file_path)

    def get_multipart_uploader(self, upload_id):
        cas_response = self.api.describe_multipart(self.name, upload_id)
        return MultipartUpload(self, cas_response)

    def delete(self):
        return self.api.delete_vault(self.name)

    def get_job(self, job_id):
        cas_response = self.api.describe_job(self.name, job_id)
        return Job(self, cas_response)


def parse_vault_name(path):
    if not path.lower().startswith(CAS_PREFIX):
        sys.stderr.write('cas vault path must start with %s\n' % CAS_PREFIX)
        sys.exit(1)
    path_fields = path[len(CAS_PREFIX):].split('/')
    name = path_fields[0]
    return name
