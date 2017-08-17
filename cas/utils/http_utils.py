# -*- coding=UTF-8 -*-

import hashlib
import sys
import time
import hmac
import urllib
from datetime import tzinfo, timedelta

from cas.conf.common_conf import SELF_DEFINE_HEADER_PREFIX


class UTC(tzinfo):
    """UTC"""
    def __init__(self, offset=0):
        self._offset = offset

    def utcoffset(self, dt):
        return timedelta(hours=self._offset)

    def tzname(self, dt):
        return "UTC +%s" % self._offset

    def dst(self, dt):
        return timedelta(hours=self._offset)


def safe_get_element(name, container):
    for k, v in container.items():
        if k.strip().lower() == name.strip().lower():
            return v
    return ""


def format_params(params=None):
    if not params:
        return ''
    tmp_params = dict()
    for k, v in params.items():
        tmp_k = k.lower().strip()
        tmp_params[tmp_k] = str(v).lower()
    res = ''
    separator = '&'
    check_params = tmp_params.keys()
    check_params.sort()

    if check_params is not None:
        for p in check_params:
            res += urllib.quote(p)
            # print p, tmp_params[p]
            v = tmp_params[p]
            if len(v) != 0:
                res += '='
                res += urllib.quote_plus(v, '~').replace('+', '%20')
            res += separator
        res = res.rstrip(separator)
    return res


def format_headers(headers=None):
    if not headers:
        return ''
    tmp_headers = {}
    for k, v in headers.items():
        tmp_k = k.lower().strip()
        tmp_headers[tmp_k] = str(v)
    res = ''
    separator = '&'
    check_headers = tmp_headers.keys()
    check_headers.sort()
    if check_headers is not None:
        for p in check_headers:
            res += p
            v = tmp_headers[p]
            if len(v) != 0:
                res += '='
                res += urllib.quote_plus(v, '~').replace('+', '%20')
            res += separator
        res = res.rstrip(separator)
    return res


def create_auth(ak, sk, host, method, url, headers, params, expire):
    """
    :param ak: access key
    :param sk: secret key
    :param host:
    :param method:
    :param url:
    :param headers:
    :param params:
    :param expire:
    :return:
    """

    if isinstance(sk, unicode):
        sk = sk.encode('utf8')
    now = int(time.time())
    time_range = '%d;%d' % (now, now + expire)
    sign_key = hmac.new(sk, time_range, hashlib.sha1).hexdigest()

    format_string = method.lower() + '\n'
    format_string += url + '\n'
    format_string += format_params(params) + '\n'
    format_string += format_headers(headers) + '\n'

    # print '=== debug: headers: ', headers
    # print '=== debug: format headers: ', format_headers(headers)
    # print '=== debug: format string:\n', format_string

    string_to_sign = 'sha1\n'
    string_to_sign += time_range + '\n'
    string_to_sign += hashlib.sha1(format_string).hexdigest() + '\n'

    # print '=== debug: string to sign: ', string_to_sign

    sign = hmac.new(sign_key, string_to_sign, hashlib.sha1).hexdigest()

    auth_content = 'q-sign-algorithm=sha1&'
    auth_content += 'q-ak=%s&' % ak.encode('utf8')
    auth_content += 'q-sign-time=%s&' % time_range
    auth_content += 'q-key-time=%s&' % time_range

    h_list = []
    if headers:
        h_list = [k.lower() for k in headers.keys()]
        h_list.sort()
    s_header = ';'.join(h_list)
    auth_content += 'q-header-list=%s&' % s_header

    p_list = []
    if params:
        p_list = [k.lower() for k in params.keys()]
        p_list.sort()
    s_param = ';'.join(p_list)
    auth_content += 'q-url-param-list=%s&' % s_param
    auth_content += 'q-signature=%s' % sign
    return auth_content


def append_param(url, params):
    """
        convert the parameters to query string of URI.
    """
    l = []
    for k, v in params.items():
        k = k.replace('_', '-')
        if k == 'maxkeys':
            k = 'max-keys'
        if isinstance(v, unicode):
            v = v.encode('utf-8')
        if v is not None and v != '':
            l.append('%s=%s' % (urllib.quote(k), urllib.quote(str(v))))
        elif k == 'acl':
            l.append('%s' % (urllib.quote(k)))
        elif v is None or v == '':
            l.append('%s' % (urllib.quote(k)))
    if len(l):
        url = url + '?' + '&'.join(l)
    return url


def check_response(http_response):
    try:
        if http_response.status / 100 != 2:
            errmsg = ''
            errmsg += 'Error Headers:\n'
            errmsg += str(http_response.getheaders())
            errmsg += '\nError Body:\n'
            errmsg += http_response.read(1024)
            errmsg += '\nError Status:\n'
            errmsg += str(http_response.status)
            errmsg += '\nFailed!\n'
            sys.stderr.write(errmsg)
            sys.exit(1)
    except AttributeError, e:
        sys.stderr.write('Error: check response status failed! msg: %s\n' % e)
        sys.exit(1)
