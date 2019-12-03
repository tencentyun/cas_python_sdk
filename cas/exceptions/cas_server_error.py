import json
import sys
import logging
from httplib import HTTPException

log = logging.getLogger(__name__)


class CASServerError(Exception):

    def __init__(self, response):
        raw_headers = response.getheaders()
        headers = dict()
        for k, v in raw_headers:
            headers[k.lower()] = v

        self.request_id = headers.get('x-cas-requestid')
        self.status = response.status
        log.debug('debug: error: receive status: %s\n' % response.status)
        log.debug('debug: error: receive headers: %s\n' % headers)

        content = ''
        try:
            content = response.read()
            body = json.loads(content)
            log.debug('debug: error: receive body: %s\n' % body)
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
