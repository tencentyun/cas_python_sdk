# CAS Python SDK

## Requirements

* Python 2.7(included) to 3.0(not included)
* yaml
* ordereddict

## Setup

### Setup through source, need sudo
```
$ python setup.py install
```

## Update Logs

* 1.0.0 support python sdk and command tools: cascmd
* 1.0.1 fix a bug on list_job cmd and compatiable job_id starting with '-'
* 1.0.3 command create_job support param --marker --limit; compatiable archive_id starting with '-'
* 1.0.4 compatiable archive_id starting with '--' also job_id, upload_id
* 1.0.5 support push-to-cos job
* 1.0.6 fix bug: close threadpool when finished
