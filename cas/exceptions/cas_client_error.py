# -*- coding=UTF-8 -*-


class CASClientError(Exception):
    pass


class UploadArchiveError(CASClientError):
    pass


class DownloadArchiveError(CASClientError):
    pass


class HashDoesNotMatchError(CASClientError):
    pass
