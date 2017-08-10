# -*- coding=UTF-8 -*-

import os

MEGABYTE = 1024 * 1024
GIGABYTE = 1024 * MEGABYTE

CAS_PREFIX = 'cas://'
DEFAULT_HOST = 'cas.ap-chengdu.myqcloud.com'
DEFAULT_PORT = 80
DEFAULT_CONFIG_FILE = os.path.expanduser('~') + '/.cascmd_credentials'
CONFIG_SECTION = 'CASCredentials'

SELF_DEFINE_HEADER_PREFIX = "x-cas-"

DEFAULT_NORMAL_UPLOAD_THRESHOLD = 100 * MEGABYTE    # 单文件的默认最大上传阈值
RECOMMEND_MIN_PART_SIZE = 16 * MEGABYTE             # 推荐的最小分片大小
MAX_PART_NUM = 10000                                # 最大的分片数目
MAX_FILE_SIZE = 4 * 10000 * GIGABYTE                # 最大支持40TB的文件存储

MultipartUpload_MinimumPartSize = 16 * MEGABYTE     # 分块上传的最小分片大小
Multipart_Upload_MaximumNumberOfParts = 10000       # 分块上传的最大分片数目

Job_Default_Download_PartSize = 32 * MEGABYTE       # Archive取回时的默认分块大小



