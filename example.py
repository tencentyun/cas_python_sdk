#!/usr/bin/env python2.7
# -*- coding=UTF-8 -*-

from logging import getLogger, basicConfig, DEBUG
from logging.config import dictConfig

from cas.cas_api import CASAPI
from cas.ease.vault import Vault
from cas.ease.uploader import *
from cas.ease.job import Job

log_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
        'error': {
            'format': '%(asctime)s\t%(message)s'
        }
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'error_file': {
            'level': 'INFO',
            'formatter': 'error',
            'class': 'logging.FileHandler',
            'filename': 'fail_files.txt',
            'mode': 'a'
        }
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        },
        'migrate_tool.fail_file': {
            'handlers': ['error_file'],
            'level': 'WARN',
            'propagate': False
        },
        'requests.packages': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': True
        }
    }
}

dictConfig(log_config)

# create casapi
# need host, appid, accesskey, secretkey
api = CASAPI('cas.ap-chengdu.myqcloud.com',   # host must be this region
              # to be replaced, use your own appid, accesskey, secretkey
             '1251668577', 'AKIDrbAYjEBqqdEconpFi8NPFsOjrnX4LYUE',
             'gCYjhT4ThiXAbp4aw65sTs56vY2Kcooc')
print api
print api.host, api.port, api.appid, api.ak, api.sk
print '================================================='

# create vault
# vault = Vault.create_vault(api, 'sdktest222')
# print '====== create vault, response: name: %s, qcs: %s' % (vault.name, vault.qcs)

# get vault by name, if vault already exists
vault = Vault.get_vault_by_name(api, 'sdktest222')
print '====== get vault, response: name: %s, qcs: %s' % (vault.name, vault.qcs)

# upload normal file
#print '================================================='
#archive_id = vault.upload_archive('../len8M.txt')
#print '====== normal upload, response archive id: ', archive_id
#
## delete archive
#res = vault.delete_archive(archive_id)
#print "====== delete just uploaded archive, response: ", res
#
# auto multipart upload for file larger than 100M
#print '================================================='
#archive_id = vault.upload_archive('../len110M.txt')
#print "====== multipart upload, response archive id: ", archive_id
# create retrieval archive job
print '================================================='
archive_id='rEXGTl2xkYWrUDFxpWYpg56__PVD-ne6YEPftWm4ZotDfP6fQmBf8-ZdX_5Bhutl0Sm7CUfILOD6lRL2-mu-fUIIdfuhCBOXjjs1EkYra9t5xoMMqxUvX_3jEOnB7Udf'
# tier可用的值是Expedited, Standard, Bulk
#archive_job = vault.retrieve_archive(archive_id, desc='python sdk test', tier='Expedited')
#print "====== retrieve_archive, response job id: ", archive_job.id
# desc archive job job
archive_job = vault.get_job('PxKAOsM028F6wEw3p9YoI4DyBHDdwxuk9vWg9aQDUTZArm1gGlvURorXjA7w6TaA')
print '======= retrieve_archive job status_code: %s, status_msg: %s, tier: %s' % \
        (archive_job.status_code, archive_job.status_message, archive_job.tier)

print '======= retrieve_archive job create_date: %s, complete_date: %s' % \
        (archive_job.creation_date, archive_job.completion_date)

#file_path='./mysdk_down.txt'
#archive_job.download_to_file(file_path)

file_path='./mysdk_down_part.txt'
archive_job.download_by_range((0, 110), file_path)


## delete archive
#res = vault.delete_archive(archive_id)
#print "====== delete just uploaded archive response: ", res


# create vault inventory job
#print '================================================='
#job_id = vault.retrieve_inventory('chengwu_inventory_test')
#print "====== retrieve_inventory, response job id: ", job_id

# desc inventory job job
#job = vault.get_job(job_id)
#print '======= retrieve_inventory job status_code: %s, status_msg: %s ' % (job.status_code, job.status_message)



