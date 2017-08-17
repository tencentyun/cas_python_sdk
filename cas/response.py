# -*- coding: utf-8 -*-

import io
import json
import yaml


class CASResponse(dict):
    def __init__(self, http_response):
        super(dict, self).__init__()
        raw_headers = http_response.getheaders()
        headers = dict()
        for k, v in raw_headers:
            headers[k.lower()] = v

        self.status = http_response.status
        self.request_id = headers['x-cas-requestid']
        self.response = http_response

        for k, v in headers.items():
            self[k] = v

        content_type = headers.get('content-type')
        if content_type == 'application/octet-stream':
            self.reader = http_response
            return

        try:
            body = http_response.read()
            self.reader = io.BytesIO(body)
            if 'content-range' in self:
                return
            if content_type == 'application/json':
                self.update(json.loads(body))
            else:
                content = yaml.load(body)
                if content is not None:
                    self.update(content)
        except Exception as e:
            raise IOError(e.message)

    def read(self, size):
        return self.reader.read(size)
