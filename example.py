#!/usr/bin/env python2.7
# -*- coding=UTF-8 -*-

import sys
import os

from logging.config import dictConfig

from cas.client import CASClient
from cas.vault import Vault

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

def Usage():
    print "Usage: python ./example <host> <appid> <AccessKey> <SecretKey> "

TEST_VAULT_NAME = "sdk_test"
TEST_NORMAL_FILE = "test-1M.dat"        # Used to test the upload operation for the normal file
TEST_LARGE_FILE = "test-101M.dat"      # Used to test  the multipart uploads for the large file

if __name__ == "__main__":
    if len(sys.argv) != 5:
        Usage()
        exit(0)

    host = sys.argv[1]
    appid = sys.argv[2]
    access_key = sys.argv[3]
    secret_key = sys.argv[4]

    os.system("dd if=/dev/zero of=" + TEST_NORMAL_FILE + " bs=1M count=1 ")
    os.system("dd if=/dev/zero of=" + TEST_LARGE_FILE + " bs=101M count=1 ")

    # Create a client for accessing CAS
    # 创建一个CAS客户端，用于访问CAS服务
    client = CASClient(host,appid,access_key,secret_key)

    # List all vaults
    # 获取当前的vault列表
    vault_list = Vault.list_all_vaults(client)
    print "====== vault list: \n", vault_list

    # Create vault
    # 创建vault
    vault = Vault.create_vault(client,TEST_VAULT_NAME)
    print "====== create vault, response:name :%s, qcs:%s\n" % (vault.name,vault.qcs)

    # Get a vault by its name, if vault already exists
    # 根据名称获取vault对象
    vault = Vault.get_vault_by_name(client,TEST_VAULT_NAME)
    print "====== get vault, response: name: %s, qcs: %s\n" % (vault.name, vault.qcs)

    # Upload a normal file.   size: 1MB
    # 上传小于100MB的普通文件
    archive_id_0 = vault.upload_archive(TEST_NORMAL_FILE)
    print "====== upload a normal archive,response archive id: \n" , archive_id_0

    # 获取Archive列表，以job形式运行，检索结果输出到inventory.out
#    inventory_job = vault.retrieve_inventory()
#    inventory_job.download_to_file("inventory.out")

    # 下载Archive，以job形式运行，检索到archive下载到指定路径的文件中
    # 可以在对tier参数指定检索类型： Expedited: 1--5分钟（最大支持256MB的文件）；Standard: 3--5小时； Bulk：5--12小时
#    archive_job = vault.retrieve_archive(archive_id_0,tier = "Expedited")
#    archive_job.download_to_file("FilePath")

    # 通过multipart任务上传大于100MB的文件
    uploader = vault.initiate_uploader(TEST_LARGE_FILE)
    archive_id_1 = uploader.start()

    # 如果上述multipart任务上传失败，则可以使用下列方法进行断点续传，
    # 其中recover_uploader方法的参数，是待续传的uploader对象的ID
#    uploader = vault.recover_uploader(uploader.id)
#    uploader.resume(TEST_LARGE_FILE)

    # Delete the specified archive
    resp = vault.delete_archive(archive_id_0)
    print "====== delete archive_id: %s, response:%s \n" % (archive_id_0,resp)
    resp = vault.delete_archive(archive_id_1)
    print "====== delete archive_id: %s, response:%s \n" % (archive_id_1,resp)

    # delete the vault
    resp = vault.delete()
    print "====== delete itself, response; \n" , (resp)