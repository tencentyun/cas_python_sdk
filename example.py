# -*- coding=UTF-8 -*-

import logging
import logging.handlers

from cas.cas_api import CASAPI
from cas.ease.vault import Vault
from cas.ease.uploader import *

LOG_FILE = 'test.log'
LOG_FILE="test.log"
handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 5)
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)

# log is in uploader
log.addHandler(handler)
log.setLevel(logging.DEBUG)

# create casapi
# need host, appid, accesskey, secretkey
api = CASAPI('cas.ap-chengdu.myqcloud.com',   # host must be this region
              # to be replaced, use your own appid, accesskey, secretkey
             '1253870963', 'AKIDzqVxDHfaP6bx7Aog8SingOuLmAB3qQwO', 'PdfWBfze4YxYFoCOsGSSa2PCuTq1AB5n')
print api
print api.host, api.port, api.appid, api.ak, api.sk
print '================================================='

# create vault
vault = Vault.create_vault(api, 'father_day')
print '====== create vault, response: name: %s, qcs: %s' % (vault.name, vault.qcs)

# get vault by name, if vault already exists
# vault = Vault.get_vault_by_name(api, 'father_day')
# print '====== get vault, response: name: %s, qcs: %s' % (vault.name, vault.qcs)

# upload normal file
print '================================================='
archive_id = vault.upload_archive('../aerospike.tgz')
print '====== normal upload, response archive id: ', archive_id
# delete archive
res = vault.delete_archive(archive_id)
print "====== delete just uploaded archive, response: ", res

# multipart upload
print '================================================='
print "====== initial one multipart upload"
uploader = vault.initiate_uploader('../pycharm.dmg')
uploader_id = uploader.id
print "====== uploader_id", uploader_id

print "====== start the multipart upload"
archive_id = uploader.start()
print "====== multipart upload, response archive_id: ", archive_id

# delete archive
res = vault.delete_archive(archive_id)
print "====== delete just uploaded archive response: ", res
