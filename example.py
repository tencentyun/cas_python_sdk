#!/usr/bin/env python2.7
# -*- coding=UTF-8 -*-

import sys
import os

from logging.config import dictConfig

from cas.client import CASClient
from cas.api import CasAPI
from cas.vault import Vault
from cas.conf.common_conf import MEGABYTE

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


def gen_random_file(file_name, size):
    _file = open(file_name, 'w')
    _file.seek(size - 3)
    _file.write('cas')
    _file.close()
    return _file


def Usage():
    print "Usage: python ./example <host> <appid> <SecretID> <SecretKey> "

TEST_VAULT_NAME = "sdk_test_iainyu"
TEST_NORMAL_FILE = "test-1M.dat"        # Used to test the upload operation for the normal file
TEST_LARGE_FILE = "test-101M.dat"      # Used to test  the multipart uploads for the large file

if __name__ == "__main__":
    if len(sys.argv) != 5:
        Usage()
        exit(0)

    host = sys.argv[1]
    appid = sys.argv[2]
    secret_id = sys.argv[3]
    secret_key = sys.argv[4]

    gen_random_file(TEST_NORMAL_FILE, 1 * MEGABYTE)
    gen_random_file(TEST_LARGE_FILE, 101 * MEGABYTE)

    # Create a client for accessing CAS
    # 创建一个CAS客户端，其中包含客户端的所有访问信息，提供Http接口的封装，基本接口均返回Http Response
    client = CASClient(host, appid, secret_id, secret_key)
    # 初始化 CasAPI用于调用SDK的API
    cas_api = CasAPI(client)

    # 获取当前用户的所有vault列表的两种方法：
    # 获取当前用户的所有vault列表，返回CASResponse结构
    vault_list = cas_api.list_all_vaults()
    print "====== vault_list: \n" % vault_list
    # 获取当前用户的所有vault列表，返回python列表结构，列表中的每个元素为vault对象
    vault_list = Vault.list_all_vaults(cas_api)
    print "====== vault list: \n" % vault_list

    # 创建vault对象的两种方法：
    # 通过CasAPI创建vault，在成功创建vault以后，通过获取新vault的属性信息来实例化本地vault对象
    # cas_response = cas_api.create_vault(TEST_VAULT_NAME)
    # vault_props = cas_api.describe_vault(TEST_VAULT_NAME)
    # vault = Vault(cas_api, vault_props)

    # 也可以通过Vault类的create方法来直接创建得到vault对象
    vault = Vault.create(cas_api, TEST_VAULT_NAME)
    print "====== create vault, response:name :%s, qcs:%s\n" % (vault.name, vault.qcs)

    # Get a vault by its name, if vault already exists
    # 根据名称获取vault对象
    vault = Vault.get_vault_by_name(cas_api, TEST_VAULT_NAME)
    print "====== get vault, response: name: %s, qcs: %s\n" % (vault.name, vault.qcs)

    # Upload a normal file.   size: 1MB
    # 上传小于100MB的普通文件
    archive_id_0 = vault.upload_archive(TEST_NORMAL_FILE)
    print "====== upload a normal archive,response archive id: \n", archive_id_0

    # 获取Archive列表，以job形式运行，检索结果输出到inventory.out
    # inventory_job = vault.initiate_retrieve_inventory()
    # inventory_job.download_to_file("inventory.out")

    # 下载Archive，以job形式运行，检索到archive下载到指定路径的文件中
    # 可以在对tier参数指定检索类型： Expedited: 1--5分钟（最大支持256MB的文件）；Standard: 3--5小时； Bulk：5--12小时
    archive_job = vault.retrieve_archive(archive_id_0, tier="Expedited")
    archive_job.download_to_file("test_archive.out")

    # 通过multipart任务分片上传大于100MB的文件
    uploader = vault.initiate_multipart_upload(TEST_LARGE_FILE)
    archive_id_1 = uploader.start()

    # 如果上述multipart任务上传失败，则可以使用下列方法进行断点续传，
    # 其中recover_uploader方法的参数，是待续传的uploader对象的ID
#    uploader = vault.recover_uploader(uploader.id)
#    uploader.resume(TEST_LARGE_FILE)

    # 删除指定的档案
    resp = vault.delete_archive(archive_id_0)
    print "====== delete archive_id: %s, response:%s \n" % (archive_id_0, resp)
    resp = vault.delete_archive(archive_id_1)
    print "====== delete archive_id: %s, response:%s \n" % (archive_id_1, resp)

    # 删除vault
    resp = vault.delete()
    # 也可以调用CasAPI基本接口进行删除
    # cas_api.delete_vault(vault.name)
    print "====== delete itself, response; \n", (resp)

    os.remove(TEST_NORMAL_FILE)
    os.remove(TEST_LARGE_FILE)