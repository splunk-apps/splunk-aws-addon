"""
Custom REST Endpoint for enumerating AWS S3 Bucket.
"""

import os
import sys

import splunk
import splunk.admin

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER


import logging
from taaws.log import setup_logger
from taaws.aws_accesskeys import AwsAccessKeyManager
from taaws.s3util import connect_s3

logger = setup_logger(APPNAME + '-RestEndpoints', level=logging.DEBUG)


def log_enter_exit(func):
    def wrapper(*args, **kwargs):
        logger.debug("{} entered.".format(func.__name__))
        result = func(*args, **kwargs)
        logger.debug("{} exited.".format(func.__name__))
        return result
    return wrapper


class ConfigHandler(splunk.admin.MConfigHandler):

    @log_enter_exit
    def setup(self):
        self.supportedArgs.addOptArg('aws_account')

    @log_enter_exit
    def handleList(self, confInfo):

        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())

        aws_account = self.callerArgs['aws_account'][0] if self.callerArgs['aws_account'] is not None else ""
        if not aws_account:
            confInfo['S3BucketsResult'].append('buckets', [])
            return

        acct = km.get_accesskey(name=aws_account)
        if not acct:
            raise Exception(aws_account + " selected is incorrect, Setup App first.")

        connection = connect_s3(acct.key_id, acct.secret_key, self.getSessionKey())
        rs = connection.get_all_buckets()
        
        rlist = []

        for r in rs:
            rlist.append(r.name)

        confInfo['S3BucketsResult'].append('buckets', rlist)


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
