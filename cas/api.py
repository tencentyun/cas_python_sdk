# -*- coding=UTF-8 -*-

from cas.client import CASClient
from cas.response import CASResponse
from cas.exceptions.cas_server_error import CASServerError


class CasAPI(object):
    """
    高层次抽象的API，所有直接暴露接口的返回结果均为CASResponse类型
    """
    def __init__(self, client):
        self.client = client

    def __getattr__(self, item):
        proxy_method = ('post_multipart', 'post_multipart_from_reader',
                        'post_archive', 'post_archive_from_reader')

        mapping_method = {'describe_multipart': 'list_parts'}

        if item in mapping_method:
            item = mapping_method.get(item)

        if item not in proxy_method:
            return object.__getattribute__(self, item)

        def func(*args, **kwargs):
            http_response = CASClient.__getattribute__(self.client, item)(*args, **kwargs)
            return CasAPI._create_response(http_response)
        return func

    @classmethod
    def _create_response(cls, response):
        if response.status / 100 == 2:
            return CASResponse(response)
        else:
            raise CASServerError(response)

    def create_vault(self, vault_name):
        response = self.client.create_vault(vault_name)
        return CasAPI._create_response(response)

    def describe_vault(self, vault_name):
        response = self.client.describe_vault(vault_name)
        return CasAPI._create_response(response)

    def delete_vault(self, vault_name):
        response = self.client.delete_vault(vault_name)
        return CasAPI._create_response(response)

    def list_vaults(self, marker=None, limit=None):
        response = self.client.list_vaults(marker=marker, limit=limit)
        return CasAPI._create_response(response)

    def list_all_vaults(self):
        response = self.list_vaults()
        current_marker = response['Marker']
        while current_marker:
            tmp_response = self.list_vaults(marker=current_marker)
            response['VaultList'].extend(tmp_response['VaultList'])
            current_marker = tmp_response['Marker']
        response['Marker'] = None
        return response

    def upload_archive(self, vault_name, content, etag, tree_tag, size=None, desc=None):
        response = self.client.upload_archive(vault_name, content, etag, tree_tag, size, desc)
        return CasAPI._create_response(response)

    def delete_archive(self, vault_name, archive_id):
        response = self.client.delete_archive(vault_name,archive_id)
        return CasAPI._create_response(response)

    def initiate_multipart_upload(self, vault_name, part_size, desc=None):
        response = self.client.initiate_multipart_upload(vault_name,part_size,desc)
        return CasAPI._create_response(response)

    def list_multipart_uploads(self,vault_name, marker=None, limit=None):
        response = self.client.list_multipart_uploads(vault_name, marker=marker, limit=limit)
        return CasAPI._create_response(response)

    def list_all_multipart_uploads(self, vault_name):
        response = self.list_multipart_uploads(vault_name)
        marker = response['Marker']
        while marker:
            tmp_response = self.list_multipart_upload(vault_name,marker=marker)
            response['UploadsList'].extend(tmp_response['UploadsList'])
            marker = tmp_response['Marker']
        response['Marker'] = None                   # 标识list已完整
        return response

    def complete_multipart_upload(self, vault_name, upload_id, file_size, tree_etag):
        response = self.client.complete_multipart_upload(vault_name, upload_id, file_size, tree_etag)
        return CasAPI._create_response(response)

    def abort_multipart_upload(self, vault_name, upload_id):
        response = self.client.abort_multipart_upload(vault_name,upload_id)
        return CasAPI._create_response(response)

    def list_parts(self, vault_name, upload_id, marker=None, limit=None):
        response = self.client.list_parts(vault_name, upload_id, marker, limit)
        return CasAPI._create_response(response)

    def list_all_parts(self, vault_name, upload_id):
        response = self.client.list_all_parts(vault_name, upload_id)
        marker = response['Marker']
        while marker:
            tmp_response = self.list_parts(vault_name, upload_id, marker=marker)
            response['Parts'].extend(tmp_response['Parts'])
            marker = tmp_response['Marker']
        response['Marker'] = None
        return response

    def initiate_job(self, vault_name, job_type, archive_id=None, desc=None, byte_range=None, tier=None):
        """
        :param vault_name: 任务所属的vault
        :param job_type: 任务类型：archive-retrieval , inventory_retrieval
        :param archive_id: 当任务类型为archive-retrieval, 此项为要检索的档案ID
        :param desc: 任务的描述
        :param byte_range:  档案检索操作的字节范围，未指定时，默认检索整个档案。 如果指定了字节范围，则字节范围必须以兆字节对齐
        :param tier: 档案检索的类型，可指定枚举字符串：'Expedited', 'Standard', 'Bulk'    默认为：'Standard'。 注意：对inventory_retrieval任务指定该项无效
        :return:
        """
        response = self.client.initiate_job(vault_name, job_type, archive_id, desc, byte_range, tier)
        return CasAPI._create_response(response)

    def describe_job(self, vault_name, job_id):
        response = self.client.describe_job(vault_name, job_id)
        return CasAPI._create_response(response)

    def get_job_output(self, vault_name, job_id, byte_range=None):
        response = self.client.get_job_output(vault_name, job_id, "bytes=%d-%d" % byte_range)
        return CasAPI._create_response(response)

    def list_jobs(self, vault_name, completed=None, marker=None, limit=None, status_code=None):
        response = self.client.list_jobs(vault_name, completed, marker, limit, status_code)
        return CasAPI._create_response(response)

    def list_all_jobs(self, vault_name):
        response = self.list_jobs(vault_name)
        marker = response['Marker']
        while marker:
            tmp_response = self.list_jobs(vault_name, marker=marker)
            response['JobList'].extend(tmp_response['JobList'])
            marker = tmp_response['Marker']
        response['Marker'] = None                           # 标识已经全部列出
        return response

    def get_vault_access_policy(self, vault_name):
        response = self.client.get_vault_access_policy(vault_name)
        return CasAPI._create_response(response)

    def set_vault_access_policy(self, vault_name, policy):
        """
        不推荐调用API进行策略操作，建议在控制台页面上进行访问策略设置
        :param vault_name:  要设置policy的vault
        :param policy: 要设置的策略
        :return: CASResponse
        """
        response = self.client.set_vault_access_policy(vault_name, policy)
        return CasAPI._create_response(response)

    def delete_vault_access_policy(self, vault_name):
        """
        不推荐调用API进行策略操作，建议策略的设置直接在控制台页面上完成
        :param vault_name:
        :return:
        """
        response = self.client.delete_vault_access_policy(vault_name)
        return CasAPI._create_response(response)
