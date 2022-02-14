# -*- coding=UTF-8 -*-

import httplib
import json
import socket
import string
import sys
import time
import logging

from cas.conf import client_conf
from cas.utils import http_utils
from cas.utils import file_utils

log = logging.getLogger(__name__)


class CASClient(object):
    """
    Http API的封装，同时维持客户端的所有访问信息
    基础接口均返回HTTPResponse
    """

    _DefaultSendBufferSize = client_conf.DefaultSendBufferSize
    _DefaultGetBufferSize = client_conf.DefaultGetBufferSize
    _DefaultAuthTimeout = client_conf.DefaultAuthTimeout
    _provider = client_conf.provider

    def __init__(self, endpoint, appid, ak, sk, port=80, is_security=False):
        self.host = endpoint
        self.appid = appid
        self.ak = ak
        self.sk = sk
        self.port = port
        self.is_security = is_security

    def __get_connection(self):
        if self.is_security or self.port == 443:
            self.is_security = True
            return httplib.HTTPSConnection(self.host, self.port, timeout=100)
        else:
            return httplib.HTTPConnection(self.host, self.port, timeout=100)

    def __http_request(self, method, url, headers=None, body='', params=None):
        headers = headers or dict()
        headers['Host'] = self.host
        if headers.get('Authorization') is None:
            headers['Authorization'] = self.__create_auth(method, url, headers, params)

        headers['Date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        headers['User-Agent'] = 'CAS Python SDK'

        if params is not None:
            url = http_utils.append_param(url, params)
        conn = self.__get_connection()
        try:
            conn.request(method, url, body, headers)
            return conn.getresponse()
        except socket.timeout, e:
            raise Exception('Connect or send timeout! ' + e.message)

    def __http_reader_huge_cache_request(self, method, url, headers, content):
        try:
            # only host need to sign !!!
            headers['Host'] = self.host
            if headers.get('Authorization') is None:
                headers['Authorization'] = self.__create_auth(method, url, headers)

            headers['Date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            headers['User-Agent'] = 'CAS Python SDK'

            conn = self.__get_connection()
            conn.putrequest(method, url)
            for k in headers.keys():
                conn.putheader(str(k), str(headers[k]))
            conn.endheaders()

            send_buffer_size = 1024 * 1024
            sended_size = 0
            content_length = len(content)
            left_length = content_length
            while True:
                if sended_size == content_length:
                    break
                elif sended_size > content_length:
                    raise Exception(
                        'Sended data more than content_length set.')

                left_length = content_length - sended_size
                if left_length > send_buffer_size:
                    buf = content[sended_size:(send_buffer_size+sended_size)]
                else:
                    buf = content[sended_size:content_length]
                buf_len = len(buf)
                if buf_len > 0:
                    conn.send(buf)
                    sended_size = sended_size + buf_len
                else:
                    break

            if sended_size < content_length:
                raise Exception('Sended data less than content_length set.')

            return conn.getresponse()

        except socket.timeout, e:
            raise Exception('Connect or send timeout! ' + e.message)
        except socket.error, e:
            error_info = str(e)
            bpipe_error = '[Errno 32] Broken pipe'
            if string.find(error_info, bpipe_error) >= 0:
                return conn.getresponse()
            else:
                raise Exception('Request error! ' + str(e))

    def __http_reader_request(self, method, url, headers, reader, content_length):
        try:
            # only host need to sign !
            headers['Host'] = self.host
            if headers.get('Authorization') is None:
                headers['Authorization'] = self.__create_auth(method, url, headers)

            headers['Date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            headers['User-Agent'] = 'CAS Python SDK'
            conn = self.__get_connection()
            conn.putrequest(method, url)
            for k in headers.keys():
                conn.putheader(str(k), str(headers[k]))
            conn.endheaders()

            send_buffer_size = 1024 * 1024
            sended_size = 0
            left_length = content_length
            while True:
                if sended_size == content_length:
                    break
                elif sended_size > content_length:
                    raise Exception(
                        'Sended data more than content_length set.')

                left_length = content_length - sended_size
                if left_length > send_buffer_size:
                    buf = reader.read(send_buffer_size)
                else:
                    buf = reader.read(left_length)
                buf_len = len(buf)
                if buf_len > 0:
                    conn.send(buf)
                    sended_size = sended_size + buf_len
                else:
                    break

            if sended_size < content_length:
                raise Exception('Sended data less than content_length set.')

            return conn.getresponse()

        except socket.timeout, e:
            raise Exception('Connect or send timeout! ' + e.message)
        except socket.error, e:
            error_info = str(e)
            bpipe_error = '[Errno 32] Broken pipe'
            if string.find(error_info, bpipe_error) >= 0:
                return conn.getresponse()
            else:
                raise Exception('Request error! ' + str(e))

    def __create_auth(self, method, url, headers=None, params=None, expire=_DefaultAuthTimeout):
        auth_value = http_utils.create_auth(self.ak, self.sk, self.host, method, url, headers, params, expire)
        return auth_value

    def create_vault(self, vault_name):
        url = '/%s/vaults/%s' % (self.appid, vault_name)
        method = 'PUT'
        res = self.__http_request(method, url)
        return res

    def describe_vault(self, vault_name):
        url = '/%s/vaults/%s' % (self.appid, vault_name)
        method = 'GET'
        return self.__http_request(method, url)

    def delete_vault(self, vault_name):
        url = '/%s/vaults/%s' % (self.appid, vault_name)
        method = 'DELETE'
        return self.__http_request(method, url)

    def list_vaults(self, marker=None, limit=None):
        """
        列出当前用户的所有文件库
        :param marker: 按照字典序，从marker位置列出Vault的QCS。 未指定则从头开始列出
        :param limit: 指定要返回的文件的最大数目。 这个值为正整数，取值范围：1-1000，默认为 1000
        :return:  HTTPResponse
        """
        url = '/%s/vaults' % self.appid
        method = 'GET'
        params = dict()
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        return self.__http_request(method, url, params=params)

    def post_archive(self, vault_name, content, etag, tree_etag, desc=None):
        """

        :param vault_name:
        :param content:
        :param etag:
        :param tree_etag:
        :param desc:
        :return:
        """
        url = '/%s/vaults/%s/archives' % (self.appid, vault_name)
        method = 'POST'
        headers = dict()
        headers['x-cas-content-sha256'] = etag
        headers['x-cas-sha256-tree-hash'] = tree_etag
        if desc is not None:
            headers['x-cas-archive-description'] = desc
        headers['Content-Length'] = len(content)

        if len(content) < 512*1024*1024:
            return self.__http_request(method, url, headers, content)
        else:
            return self.__http_reader_huge_cache_request(method, url, headers, content)

    def post_archive_from_reader(self, vault_name, reader, content_length, etag, tree_etag, desc=None):
        """
        从IO reader中上传archive
        :param vault_name:
        :param reader:
        :param content_length:
        :param etag:
        :param tree_etag:
        :param desc:
        :return:
        """
        url = '/%s/vaults/%s/archives' % (self.appid, vault_name)
        method = 'POST'
        headers = dict()
        headers['x-cas-content-sha256'] = etag
        headers['x-cas-sha256-tree-hash'] = tree_etag
        if desc is not None:
            headers['x-cas-archive-description'] = desc
        headers['Content-Length'] = content_length

        return self.__http_reader_request(method, url, headers, reader, content_length)

    def delete_archive(self, vault_name, archive_id):
        url = '/%s/vaults/%s/archives/%s' % (self.appid, vault_name, archive_id)
        method = 'DELETE'
        headers = dict()
        res = self.__http_request(method, url, headers)
        return res

    def initiate_multipart_upload(self, vault_name, part_size, desc=None):
        """
            @param part_size:required, int, size of per part should be large than 64M(67108864) and mod 1M equals 0
        """
        url = '/%s/vaults/%s/multipart-uploads' % (self.appid, vault_name)
        headers = dict()
        headers['x-cas-part-size'] = str(part_size)
        if desc is not None:
            headers['x-cas-archive-description'] = desc
        method = 'POST'
        res = self.__http_request(method, url, headers)
        return res

    def list_multipart_uploads(self, vault_name, marker=None, limit=None):
        """
            list all multipart uploads
        """
        url = '/%s/vaults/%s/multipart-uploads' % (self.appid, vault_name)
        method = 'GET'
        params = {}
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        headers = dict()
        body = ''
        res = self.__http_request(method, url, headers, body, params)
        return res

    def complete_multipart_upload(self, vault_name, upload_id, file_size, tree_etag):
        """
            complete multipart upload
        """
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'POST'
        headers = dict()
        headers['x-cas-archive-size'] = str(file_size)
        headers['x-cas-sha256-tree-hash'] = tree_etag
        res = self.__http_request(method, url, headers)
        return res

    def abort_multipart_upload(self, vault_name, upload_id):
        """
            delete multipart upload
        """
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'DELETE'
        headers = dict()
        res = self.__http_request(method, url, headers)
        return res

    def post_multipart(self, vault_name, upload_id, content, prange, etag, tree_etag):
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'PUT'
        headers = dict()
        headers['Host'] = self.host
        headers['Content-Length'] = len(content)
        headers['x-cas-content-sha256'] = etag
        headers['x-cas-sha256-tree-hash'] = tree_etag

        headers['Authorization'] = self.__create_auth(method, url, headers)
        # Content-Range:bytes 0-4194303/*
        # place range here !
        headers['Content-Range'] = 'bytes ' + prange + '/*'

        log.debug('debug: post part from content, url: %s, range, %s\n' % (url, prange))

        if len(content) < 512*1024*1024:
            return self.__http_request(method, url, headers, content)
        else:
            return self.__http_reader_huge_cache_request(method, url, headers, content)

    def post_multipart_from_reader(self, vault_name, upload_id, reader, content_length, prange, etag, tree_etag):
        """
            post multipart from reader to cas

            @param content_length: size of post part , should be equal with the multipart upload setting,the last part can be less
            @param prange: required, string, upload data range eg: 0-67108863
        """
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'PUT'
        headers = dict()
        headers['Host'] = self.host
        headers['Content-Length'] = content_length
        headers['x-cas-content-sha256'] = etag
        headers['x-cas-sha256-tree-hash'] = tree_etag

        headers['Authorization'] = self.__create_auth(method, url, headers)
        # Content-Range:bytes 0-4194303/*
        # place range here !
        headers['Content-Range'] = 'bytes ' + prange + '/*'

        log.debug('debug: post part from reader, url: %s, range, %s\n' % (url, prange))

        return self.__http_reader_request(method, url, headers, reader, content_length)

    def list_parts(self, vault_name, upload_id, marker=None, limit=None):
        """
            list all multiparts belong to one upload
        """
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'GET'
        params = dict()
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        return self.__http_request(method, url, params=params)

    def initiate_job(self, vault_name, job_type, archive_id=None, desc=None, byte_range=None, tier=None, marker=None, limit=None, start_date=None, end_date=None, bucket_endpoint=None, object_name=None):
        """
            create job
            @param vault_name:
            @param job_type: required, string, can only be archive-retrieval or inventory-retrieval or push-to-cos
            @param archive_id: not required, string, when job_type is archive-retrieval , archive_id must be set
            @param desc: retrieval job's description
            @param byte_range: the range of bytes to retrieve
            @param tier:  retrieval type : 'Expedited' , 'Standard', 'Bulk'
            @param marker:  start position marker, effective when job_type is inventory-retrieval
            @param limit:  count of archives, effective when job_type is inventory-retrieval
            @param start_date:  start date of archive uploaded, effective when job_type is inventory-retrieval
            @param end_date:  end date of archive uploaded, effective when job_type is inventory-retrieval
            @param bucket_endpoint:  the bucket endpoint, effective when job_type is push-to-cos
            @param object_name:  the object id, effective when job_type is push-to-cos
        """
        url = '/%s/vaults/%s/jobs' % (self.appid, vault_name)
        method = 'POST'
        body = dict()
        body['Type'] = job_type
        if job_type == 'archive-retrieval':
            body['ArchiveId'] = archive_id
            if byte_range is not None:
                body['RetrievalByteRange'] = byte_range
            if tier is not None:
                body['Tier'] = tier
        elif job_type == 'push-to-cos':
            body['ArchiveId'] = archive_id
            body['Bucket'] = bucket_endpoint
            body['Object'] = object_name
            if byte_range is not None:
                body['RetrievalByteRange'] = byte_range
            if tier is not None:
                body['Tier'] = tier
        elif job_type == 'inventory-retrieval':
            body['InventoryRetrievalParameters'] = dict()
            if marker is not None:
                body['InventoryRetrievalParameters']['Marker'] = marker
            if limit is not None:
                body['InventoryRetrievalParameters']['Limit'] = str(limit)
            if start_date is not None:
                body['InventoryRetrievalParameters']['StartDate'] = start_date
            if end_date is not None:
                body['InventoryRetrievalParameters']['EndDate'] = end_date
        if desc is not None:
            body['Description'] = desc
        body = json.dumps(body)
        return self.__http_request(method, url, body=body)

    def describe_job(self, vault_name, job_id):
        url = '/%s/vaults/%s/jobs/%s' % (self.appid, vault_name, job_id)
        method = 'GET'
        return self.__http_request(method, url)

    def get_job_output(self, vault_name, job_id, orange=None):
        """
           get job output
           @param orange: not required , string, fetch byte range like bytes=0-1048575
        """
        url = '/%s/vaults/%s/jobs/%s/output' % (self.appid, vault_name, job_id)
        method = 'GET'
        headers = dict()
        if orange is not None:
            headers['Range'] = orange
        return self.__http_request(method, url, headers)

    def list_jobs(self, vault_name, completed=None, marker=None, limit=None, status_code=None):
        """
        :param vault_name: 对应的vault
        :param completed: 要返回的任务的状态，枚举值：true, false
        :param marker: 字典序，从Marker起读取对应条目
        :param limit: 要返回任务的最大数目，默认限制为 1000
        :param status_code: 要返回的任务状态的类型。枚举值：InProgress, Succeeded, Failed
        :return: HTTPResponse
        """
        url = '/%s/vaults/%s/jobs' % (self.appid, vault_name)
        method = 'GET'
        params = dict()
        if completed is not None:
            params['completed'] = completed
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        if status_code is not None:
            params['statuscode'] = status_code

        return self.__http_request(method, url, params=params)

    def upload_archive(self, vault_name, content, etag, tree_tag, size=None, desc=None):
        if not file_utils.is_file_like(content):
            return self.post_archive(vault_name, content, etag, tree_tag, desc=desc)
        else:
            return self.post_archive_from_reader(vault_name, content, min(size, file_utils.content_length(content)),
                                                 etag, tree_tag, desc)

    def get_vault_access_policy(self, vault_name):
        url = '/%s/vaults/%s/access-policy' % (self.appid, vault_name)
        method = "GET"
        return self.__http_request(method, url)

    def set_vault_access_policy(self,vault_name,policy):
        url = '/%s/vaults/%s/access-policy' % (self.appid, vault_name)
        method = "PUT"
        body = dict()
        body['policy'] = policy
        body = json.dumps(body)
        return self.__http_request(method, url, body=body)

    def delete_vault_access_policy(self,vault_name):
        url = '/%s/vaults/%s/access-policy' % (self.appid, vault_name)
        method = 'DELETE'
        return self.__http_request(method, url)
