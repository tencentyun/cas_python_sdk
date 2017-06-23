# -*- coding: utf-8 -*-

import json
import sys
from httplib import HTTPException


class CASServerError(Exception):

    def __init__(self, response):
        raw_headers = response.getheaders()
        headers = dict()
        for k, v in raw_headers:
            headers[k.lower()] = v

        self.request_id = headers.get('x-cas-requestid')
        self.status = response.status
        sys.stdout.write('====== debug: error: receive headers: %s\n' % headers)

        content = ''
        try:
            content = response.read()
            body = json.loads(content)
            sys.stdout.write('====== debug: error: receive body: %s\n' % body)
            self.code = body.get('code')
            self.type = body.get('type')
            self.message = body.get('message')
            msg = 'Expected 2xx, got '
            msg += '(%d, code=%s, message=%s, type=%s, request_id=%s)' % \
                   (self.status, self.code,
                    self.message, self.type, self.request_id)
        except (HTTPException, ValueError):
            msg = 'Expected 2xx, got (%d, content=%s, request_id=%s)' % \
                  (self.status, content, self.request_id)

        super(CASServerError, self).__init__(msg)


class CASClientError(Exception):
    pass


class UploadArchiveError(CASClientError):
    pass


class DownloadArchiveError(CASClientError):
    pass


class HashDoesNotMatchError(CASClientError):
    pass
