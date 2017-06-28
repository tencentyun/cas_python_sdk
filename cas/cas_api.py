# -*- coding=UTF-8 -*-

import json
import httplib
import socket
import string
import sys
import time

from cas.cas_util import create_auth, append_param

class CASAPI(object):
    DefaultSendBufferSize = 8192
    DefaultGetBufferSize = 1024 * 1024 * 10
    DefaultAuthTimeout = 1200  # 1200 seconds
    provider = 'CAS'

    def __init__(self, host, appid, ak, sk, port=80, is_security=False):
        self.host = host
        self.appid = appid
        self.ak = ak
        self.sk = sk
        self.port = port
        self.is_security = is_security

    def __get_connection(self):
        if self.is_security or self.port == 443:
            self.is_security = True
            return httplib.HTTPSConnection(host=self.host, port=self.port, timeout=100)
        else:
            return httplib.HTTPConnection(host=self.host, port=self.port, timeout=100)

    def __http_request(self, method, url, headers=None, body='', params=None):
        headers = headers or dict()
        # only host need to sign !
        headers['Host'] = self.host
        if headers.get('Authorization') is None:
            headers['Authorization'] = self.__create_auth(method, url, headers, params)

        headers['Date'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        headers['User-Agent'] = 'CAS Python SDK'

        if params is not None:
            url = append_param(url, params)
        # print '=== debug: send headers: ', headers
        # print '=== debug: send url: ', url
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
            # print '=== debug: send headers: ', headers
            # print '=== debug: send url: ', url
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

    def __create_auth(self, method, url, headers=None, params=None, expire=DefaultAuthTimeout):
        auth_value = create_auth(self.ak, self.sk, self.host, method, url, headers, params, expire)
        return auth_value

    def create_vault(self, vault_name):
        url = '/%s/vaults/%s' % (self.appid, vault_name)
        method = 'PUT'
        res = self.__http_request(method, url)
        return res

    def get_vault_desc(self, vault_name):
        url = '/%s/vaults/%s' % (self.appid, vault_name)
        method = 'GET'
        return self.__http_request(method, url)

    def delete_vault(self, vault_name):
        url = '/%s/vaults/%s' % (self.appid, vault_name)
        method = 'DELETE'
        return self.__http_request(method, url)

    def list_vault(self, marker=None, limit=None):
        '''
            @param marker: not required , string, marker of start
            @param limit:  not required , int, retrive size
        '''
        url = '/%s/vaults' % self.appid
        method = 'GET'
        params = dict()
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        return self.__http_request(method, url, params=params)

    def post_archive(self, vault_name, content, etag, tree_etag, desc=None):
        '''
            Post content to cas as archive
            @param content: required , string , content of archive
        '''
        url = '/%s/vaults/%s/archives' % (self.appid, vault_name)
        method = 'POST'
        headers = dict()
        headers['x-cas-content-sha256'] = etag
        headers['x-cas-sha256-tree-hash'] = tree_etag
        if desc is not None:
            headers['x-cas-archive-description'] = desc
        headers['Content-Length'] = len(content)

        if(len(content) < 512*1024*1024):
            return self.__http_request(method, url, headers, content)
        else:
            return self.__http_reader_huge_cache_request(method, url, headers, content)

    def post_archive_from_reader(self, vault_name, reader, content_length, etag, tree_etag, desc=None):
        '''
            Post archive from reader to cas
            @param content_length: required , int , byte length of archive
        '''
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
        headers = {}
        res = self.__http_request(method, url, headers)
        return res

    def create_multipart_upload(self, vault_name, partsize, desc=None):
        '''
            @param partsize:required, int, size of per part should be large than 64M(67108864) and mod 1M equals 0
        '''
        url = '/%s/vaults/%s/multipart-uploads' % (self.appid, vault_name)
        headers = {}
        headers['x-cas-part-size'] = str(partsize)
        if desc != None:
            headers['x-cas-archive-description'] = desc
        method = 'POST'
        res = self.__http_request(method, url, headers)
        return res

    def list_multipart_upload(self, vault_name, marker=None, limit=None):
        '''
            list all multipart uploads
        '''
        url = '/%s/vaults/%s/multipart-uploads' % (self.appid, vault_name)
        method = 'GET'
        params = {}
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        headers = {}
        body = ''
        res = self.__http_request(method, url, headers, body, params)
        return res

    def complete_multipart_upload(self, vault_name, upload_id, filesize, tree_etag):
        '''
            complete multipart upload
        '''
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'POST'
        headers = {}
        headers['x-cas-archive-size'] = str(filesize)
        headers['x-cas-sha256-tree-hash'] = tree_etag
        res = self.__http_request(method, url, headers)
        return res

    def delete_multipart_upload(self, vault_name, upload_id):
        '''
            delete multipart upload
        '''
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'DELETE'
        headers = {}
        res = self.__http_request(method, url, headers)
        return res

    def post_multipart(self, vault_name, upload_id, content, prange, etag, tree_etag):
        '''
            post content to cas as multipart

            @param content:required, string, upload data content
            @param prange: required, string, upload data range eg: 0-67108863
        '''
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'PUT'
        headers = {}
        headers['Host'] = self.host
        headers['Content-Length'] = len(content)
        headers['x-cas-content-sha256'] = etag
        headers['x-cas-sha256-tree-hash'] = tree_etag

        headers['Authorization'] = self.__create_auth(method, url, headers)
        # Content-Range:bytes 0-4194303/*
        # place range here !
        headers['Content-Range'] = 'bytes ' + prange + '/*'

        sys.stdout.write('====== debug: post part from content, url: %s, range, %s\n' % (url, prange))

        if(len(content) < 512*1024*1024):
            return self.__http_request(method, url, headers, content)
        else:
            return self.__http_reader_huge_cache_request(method, url, headers, content)

    def post_multipart_from_reader(self, vault_name, upload_id, reader, content_length, prange, etag, tree_etag):
        '''
            post multipart from reader to cas

            @param content_length: size of post part , should be equal with the multipart upload setting,the last part can be less
            @param prange: required, string, upload data range eg: 0-67108863
        '''
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'PUT'
        headers = {}
        headers['Host'] = self.host
        headers['Content-Length'] = content_length
        headers['x-cas-content-sha256'] = etag
        headers['x-cas-sha256-tree-hash'] = tree_etag

        headers['Authorization'] = self.__create_auth(method, url, headers)
        # Content-Range:bytes 0-4194303/*
        # place range here !
        headers['Content-Range'] = 'bytes ' + prange + '/*'
        #headers['Content-Range'] = prange

        sys.stdout.write('====== debug: post part from reader, url: %s, range, %s\n' % (url, prange))

        return self.__http_reader_request(method, url, headers, reader, content_length)

    def list_multipart(self, vault_name, upload_id, marker=None, limit=None):
        '''
            list all multiparts belong to one upload
        '''
        url = '/%s/vaults/%s/multipart-uploads/%s' % (self.appid, vault_name, upload_id)
        method = 'GET'
        params = dict()
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        return self.__http_request(method, url, params=params)

    # todo
    def create_job(self, vault_id, job_type, archive_id=None, desc=None, byte_range=None):
        '''
            create job
            @param job_type: required, string, can only be archive-retrieval or inventory-retrieval
            @param archive_id: not required, string, when job_type is archive-retrieval , archive_id must be set
        '''
        url = '/vaults/%s/jobs' % (vault_id)
        method = 'POST'
        body = dict()
        body['Type'] = job_type
        if job_type == 'archive-retrieval':
            body['ArchiveId'] = archive_id
            if byte_range is not None:
                body['RetrievalByteRange'] = byte_range
        if desc is not None:
            body['Description'] = desc
        body = json.dumps(body)
        return self.__http_request(method, url, body=body)

    def get_jobdesc(self, vault_id, job_id):
        url = '/vaults/%s/jobs/%s' % (vault_id, job_id)
        method = 'GET'
        return self.__http_request(method, url)

    def fetch_job_output(self, vault_id, job_id, orange=None):
        '''
           fetch job output
           @param orange: not required , string, fetch byte range like bytes=0-1048575
        '''
        url = '/vaults/%s/jobs/%s/output' % (vault_id, job_id)
        method = 'GET'
        headers = dict()
        if orange is not None:
            headers['Range'] = orange
        return self.__http_request(method, url, headers)

    def list_job(self, vault_id, marker=None, limit=None):
        '''
            fetch all jobs
        '''
        url = '/vaults/%s/jobs' % (vault_id)
        method = 'GET'
        params = dict()
        if marker is not None:
            params['marker'] = marker
        if limit is not None:
            params['limit'] = limit
        return self.__http_request(method, url, params=params)
