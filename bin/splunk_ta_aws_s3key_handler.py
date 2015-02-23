"""
Custom REST Endpoint for enumerating AWS S3 Bucket.
"""

import os
import sys

import splunk
import splunk.admin

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER

from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
sys.path.append(make_splunkhome_path(['etc', 'apps', APPNAME, 'lib']))

import logging
import taaws.s3util
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
        self.supportedArgs.addReqArg('bucket_name')
        self.supportedArgs.addReqArg('aws_account')

    @log_enter_exit
    def handleList(self, confInfo):

        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())
        aws_account = self.callerArgs['aws_account'][0] or "default"

        acct = km.get_accesskey(name=aws_account)

        if not acct:
            raise Exception("No AWS Account is configured. Setup App first.")

        connection = connect_s3(acct.key_id, acct.secret_key, self.getSessionKey())
        bucket = connection.get_bucket(self.callerArgs['bucket_name'][0])
        
        rlist = []
        for key in taaws.s3util.get_keys(bucket,recursion_depth=1):
            rlist.append(key.name)

        confInfo['S3KeyNamesResult'].append('key_names', rlist)


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
