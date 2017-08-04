#!/usr/bin/env python2.7
# -*- coding=UTF-8 -*-
#
import ConfigParser
import os
import sys
import time
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import namedtuple

from cas.cas_cmd.cas_ops import CASCMD
from cas.conf.common_conf import CONFIG_SECTION
from cas.conf.common_conf import DEFAULT_CONFIG_FILE

HELP_INFO = \
    '''Usage: cascmd <action> [<args>]:

General Use:
    ls
    cv                     cas://vault
    rm                     cas://vault [archive_id]
    upload                 cas://vault local_file [-p PART_SIZE] [--upload_id upload_id] [--desc desc]
    create_job             cas://vault [archive_id] [--start start] [--size size] [--desc desc]
    fetch                  cas://vault jobid local_file [--start start] [--size size] [-f]

Vault Operations:
    create_vault           cas://vault
    delete_vault           cas://vault
    list_vault             [--marker marker] [--limit limit]
    desc_vault             cas://vault

Archive Operations:
    upload_id              cas://vault local_file [-p PART_SIZE] [--upload_id upload_id] [--desc desc]
    delete_archive         cas://vault archive_id

Etag Operations:
    file_tree_etag   local_file
    part_tree_etag   local_file start end

Multipart Archive Operations:
    init_multipart_upload       cas://vault part_size [--desc desc]
    list_multipart_upload       cas://vault [--marker marker] [--limit limit]
    complete_multipart_upload   cas://vault upload_id file_size [file_tree_etag]
    abort_multipart_upload      cas://vault upload_id
    upload_part            cas://vault upload_id local_file start end [etag] [part_tree_etag]
    list_part              cas://vault upload_id [--maker marker] [--limit limit]

Job Operations:
    create_job             cas://vault [archive_id] [--desc desc] [--start start] [--size size]
    desc_job               cas://vault job_id
    fetch_joboutput        cas://vault jobid local_file [--start start] [--size size] [-f]
    list_job                cas://vault [--marker marker] [--limit limit]

Other Operations:
    config                 --endpoint endpoint --appid appid --secretid secretid --secretkey secretkey
    help

'''

AuthInfo = namedtuple(
    'AuthInfo', ['endpoint', 'appid', 'secretid', 'secretkey'], verbose=False)


def build_auth_info(args):
    try:
        (endpoint, appid, secretid, secretkey) = (args.endpoint, args.appid, args.secretid, args.secretkey)
        if not (endpoint and appid and secretid and secretid):
            config = ConfigParser.ConfigParser()
            cfgfile = args.config_file or DEFAULT_CONFIG_FILE
            config.read(cfgfile)
            # user defined inputs have higher priority
            endpoint = endpoint or config.get(CONFIG_SECTION, 'endpoint')
            appid = appid or config.get(CONFIG_SECTION, 'appid')
            secretid = secretid or config.get(CONFIG_SECTION, 'secretid')
            secretkey = secretkey or config.get(CONFIG_SECTION, 'secretkey')
        return AuthInfo(endpoint, appid, secretid, secretkey)
    except:
        sys.stderr.write("Cannot get authinfo (endpoint, appid, secretid, secretkey). " \
                         "Setup use: cascmd.py config\n")
        sys.exit(1)


def save_config(args):
    config = ConfigParser.RawConfigParser()
    config.add_section(CONFIG_SECTION)
    config.set(CONFIG_SECTION, 'endpoint', args.endpoint)
    config.set(CONFIG_SECTION, 'appid', args.appid)
    config.set(CONFIG_SECTION, 'secretid', args.secretid)
    config.set(CONFIG_SECTION, 'secretkey', args.secretkey)
    # set config_file
    cfgfile = args.config_file or DEFAULT_CONFIG_FILE
    if os.path.isfile(cfgfile):
        ans = raw_input('Config file already existed. Do you wish to overwrite it?(y/n)')
        if ans.lower() != 'y':
            print 'Answer is No. Quit now'
            return
    with open(cfgfile, 'w+') as f:
        config.write(f)
    print 'Your configuration has been saved to %s' % cfgfile


def print_help(args):
    print HELP_INFO


def add_userinfo_config(parser):
    parser.add_argument('--endpoint', type=str, help='endpoint, e.g. cas.ap-chengdu.myqcloud.com')
    parser.add_argument('--appid', type=int, help='appid, please refer to https://console.qcloud.com/capi')
    parser.add_argument('--secretid', type=str, help='secretid, please refer to https://console.qcloud.com/capi')
    parser.add_argument('--secretkey', type=str, help='secretkey, please refer to https://console.qcloud.com/capi')
    parser.add_argument('--config-file', type=str, help='configuration file')

if __name__ == '__main__':
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    subcmd = parser.add_subparsers(dest='cmd', title='Supported actions', \
            metavar='cmd', description=\
            """
            Commands {ls, cv, rm, upload, create_job, fetch} provide easier
            ways to use CAS by combining commands below them. Generally they will
            suffice your daily use. For advanced operations, use commands like
            {createvault...}
            """)

    cmd = 'config'
    pcfg = subcmd.add_parser(cmd, help='config cascmd')
    pcfg.add_argument('--endpoint', type=str, help='cas endpoint', required=True)
    pcfg.add_argument('--appid', type=int, help='user appid', required=True)
    pcfg.add_argument('--secretid', type=str, help='user secretid', required=True)
    pcfg.add_argument('--secretkey', type=str, help='user secretkey', required=True)
    pcfg.add_argument('--config-file', type=str, help='file to save configuration')

    cmd = 'help'
    phelp = subcmd.add_parser(cmd, help='show a detailed help message and exit')

    cmd = 'ls'
    pls = subcmd.add_parser(cmd, help='list all vaults')
    pls.add_argument('--marker', type=str, help='list start position marker')
    pls.add_argument('--limit', type=int, help='number of vaults to be listed, max 1000')
    add_userinfo_config(pls)

    cmd = 'cv'
    pcv = subcmd.add_parser(cmd, help='create a vault')
    pcv.add_argument('vault', type=str, help='format cas://vault-name')
    add_userinfo_config(pcv)

    cmd = 'rm'
    prm = subcmd.add_parser(cmd, help='remove a vault or an archive')
    prm.add_argument('vault', type=str, help='format cas://vault-name')
    prm.add_argument('archive_id', nargs='?', type=str, help=
            'ID of archive to be deleted')
    add_userinfo_config(prm)

    cmd = 'upload'
    pupload = subcmd.add_parser(cmd, help='upload a local file')
    pupload.add_argument('vault', type=str, help='format cas://vault-name')
    pupload.add_argument('local_file', type=str, help='file to be uploaded')
    pupload.add_argument('--upload_id', type=str, help=\
            'MultiPartUpload ID upload returned to resume last upload')
    pupload.add_argument('--desc', type=str, help='description of the file')
    pupload.add_argument('-p', '--part-size', type=str, help=
            'multipart upload part size')
    add_userinfo_config(pupload)

    cmd = 'create_job'
    pcj = subcmd.add_parser(cmd, help=\
            'create an inventory/archive retrieval job')
    pcj.add_argument('vault', type=str, help='format cas://vault-name')
    pcj.add_argument('archive_id', nargs='?', help='ID of archive to be ' \
            'downloaded. If not provided, an inventory-retrieval job will be ' \
            'created')
    pcj.add_argument('--start', help=\
            'start position of archive to retrieve, default to be 0')
    pcj.add_argument('--size', help=\
            'size to retrieve, default to be (totalsize - start)' )
    pcj.add_argument('--desc', type=str, help='description of the job')
    pcj.add_argument('--tier', type=str, help='The retrieval option to use for '\
            'the archive retrieval. Standard is the default value used.')
    add_userinfo_config(pcj)

    cmd = 'fetch'
    pfj = subcmd.add_parser(cmd, help='fetch job output')
    pfj.add_argument('vault', type=str, help='format cas://vault-name')
    pfj.add_argument('jobid', type=str, help='jobId createjob returned')
    pfj.add_argument('local_file', type=str, help='local file output written to')
    pfj.add_argument('-f', '--force', action='store_true', help='force overwrite if file exists')
    pfj.add_argument('--start', type=str, help='start position to download output retrieved, default to be 0')
    pfj.add_argument('--size', type=str, help='size to download, default to be (totalsize - start)')
    add_userinfo_config(pfj)

    cmd = 'create_vault'
    pcvault = subcmd.add_parser(cmd, help='create a vault')
    pcvault.add_argument('vault', type=str, help='format cas://vault-name')
    add_userinfo_config(pcvault)

    cmd = 'delete_vault'
    pdvault = subcmd.add_parser(cmd, help='delete a vault')
    pdvault.add_argument('vault', type=str, help='format cas://vault-name')
    add_userinfo_config(pdvault)

    cmd = 'list_vault'
    plsv = subcmd.add_parser(cmd, help='list all vaults')
    plsv.add_argument('--marker', type=str, help='list start position marker')
    plsv.add_argument('--limit', type=int, help='number of vaults to be listed')
    add_userinfo_config(plsv)

    cmd = 'desc_vault'
    pgv = subcmd.add_parser(cmd, help='get detailed vault description')
    pgv.add_argument('vault', type=str, help='format cas://vault name')
    add_userinfo_config(pgv)

    cmd = 'upload_archive'
    pua = subcmd.add_parser(cmd, help='upload a local file')
    pua.add_argument('vault', type=str, help='format cas://vault-name')
    pua.add_argument('local_file', type=str, help='file to be uploaded')
    pua.add_argument('--upload_id', type=str, help='MultiPartUpload ID upload returned to resume last upload')
    pua.add_argument('--desc', type=str, help='description of the file')
    pua.add_argument('-p', '--part-size', type=str, help='multipart upload part size')
    add_userinfo_config(pua)

    cmd = 'delete_archive'
    pdar = subcmd.add_parser(cmd, help='delete an archive')
    pdar.add_argument('vault', type=str, help='format cas://vault-name')
    pdar.add_argument('archive_id', type=str, help='ID of archive to be deleted')
    add_userinfo_config(pdar)

    cmd = 'file_tree_etag'
    pfth = subcmd.add_parser(cmd, help='calculate tree sha256 hash of a file')
    pfth.add_argument('local_file', type=str, help='file to be calculated')
    add_userinfo_config(pfth)

    cmd = 'part_tree_etag'
    ppth = subcmd.add_parser(cmd, help= 'calculate tree sha256 hash of a multipart upload part')
    ppth.add_argument('local_file', type=str, help='file to be read from')
    ppth.add_argument('start', type=str, help='start position to read')
    ppth.add_argument('end', type=str, help='end position to read')
    add_userinfo_config(ppth)

    cmd = 'init_multipart_upload'
    pim = subcmd.add_parser(cmd, help='initiate a multipart upload')
    pim.add_argument('vault', type=str, help='format cas://vault-name')
    pim.add_argument('part_size', type=str, help='size of each multipart upload')
    pim.add_argument('--desc', type=str, help='description of the upload')
    add_userinfo_config(pim)

    cmd = 'abort_multipart_upload'
    pam = subcmd.add_parser(cmd, help='abort a multipart upload')
    pam.add_argument('vault', type=str, help='format cas://vault-name')
    pam.add_argument('upload_id', type=str, help=
            'ID of multipart upload to be aborted')
    add_userinfo_config(pam)

    cmd = 'list_multipart_upload'
    plm = subcmd.add_parser(cmd, help='list all multipart uploads in a vault')
    plm.add_argument('vault', type=str, help='format cas://vault-name')
    plm.add_argument('--marker', type=str, help='list start multiupload position marker')
    plm.add_argument('--limit', type=int, help='number to be listed, max 1000')
    add_userinfo_config(plm)

    cmd = 'upload_part'
    ppm = subcmd.add_parser(cmd, help='upload one part')
    ppm.add_argument('vault', type=str, help='vault to store the part')
    ppm.add_argument('upload_id', type=str, help='ID createmupload returned')
    ppm.add_argument('local_file', type=str, help='file to read from')
    ppm.add_argument('start', type=str, help=
            'read start position, start must be divided by partsize')
    ppm.add_argument('end', type=str, help=
            'read end position, end+1 must be the size of file or '
            'partsize larger than start')
    ppm.add_argument('etag', nargs='?', help='sha256 hash value')
    ppm.add_argument('tree_etag', nargs='?', help='tree sha256 hash value of part')
    add_userinfo_config(ppm)

    cmd = 'list_part'
    plp = subcmd.add_parser(cmd, help='list all parts uploaded in one upload')
    plp.add_argument('vault', type=str, help='format cas://vault-name')
    plp.add_argument('upload_id', type=str, help='ID of multipart upload')
    plp.add_argument('--marker', type=str, help='list start part position marker')
    plp.add_argument('--limit', type=int, help='number to be listed')
    add_userinfo_config(plp)

    cmd = 'complete_multipart_upload'
    pcm = subcmd.add_parser(cmd, help='complete the multipart upload')
    pcm.add_argument('vault', type=str, help='vault where the upload initiated')
    pcm.add_argument('upload_id', type=str, help='ID create multipartupload returned')
    pcm.add_argument('size', type=str, help='size of the file')
    pcm.add_argument('tree_etag', type=str, help='tree sha256 hash vaule of the file')
    add_userinfo_config(pcm)

    cmd = 'desc_job'
    pdj = subcmd.add_parser(cmd, help='get job status description')
    pdj.add_argument('vault', type=str, help='format cas://vault-name')
    pdj.add_argument('jobid', type=str, help='the id of createjob returned')
    add_userinfo_config(pdj)

    cmd = 'fetch_joboutput'
    pfjob = subcmd.add_parser(cmd, help='fetch job output')
    pfjob.add_argument('vault', type=str, help='format cas://vault-name')
    pfjob.add_argument('jobid', type=str, help='jobId createjob returned')
    pfjob.add_argument('local_file', type=str, help='local file output written to')
    pfjob.add_argument('-f', '--force', action='store_true', help=\
            'force overwrite if file exists')
    pfjob.add_argument('--start', type=str, help=\
            'start position to download output retrieved, default to be 0')
    pfjob.add_argument('--size', type=str, help=\
            'size to download, default to be (totalsize - start)')
    add_userinfo_config(pfjob)

    cmd = 'list_job'
    plj = subcmd.add_parser(cmd, help='list all jobs except expired')
    plj.add_argument('vault', type=str, help='format cas://vault-name')
    plj.add_argument('--marker', type=str, help=\
            'start list job position marker')
    plj.add_argument('--limit', type=int, help='number to be listed')
    add_userinfo_config(plj)

    args = parser.parse_args()

    if args.cmd == 'help':
        print_help(args)
        sys.exit(0)
    elif args.cmd == 'config':
        save_config(args)
        sys.exit(0)

    # build auth_info
    auth_info = build_auth_info(args)

    # error command
    method = CASCMD.__dict__.get('cmd_%s' % args.cmd)
    if method is None:
        sys.stderr.write('Unsupported command: %s\nUse help for more ' \
                         'information\n' % args.cmd)
        sys.exit(1)

    begin = time.time()
    cas_cmd = CASCMD(auth_info) 
    method(cas_cmd, args)
    end = time.time()
    sys.stderr.write('%.3f(s) elapsed\n' % (end - begin))

