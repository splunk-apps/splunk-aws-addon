"""
Custom REST Endpoint for enumerating AWS cloudwatch namepaces.
"""

import os
import sys

import splunk
import splunk.admin

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER


import logging
from taaws.log import setup_logger
logger = setup_logger(APPNAME + '-RestEndpoints', level=logging.DEBUG)

from taaws.aws_accesskeys import AwsAccessKeyManager
import taaws.s3util


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

        self.supportedArgs.addReqArg('aws_region')
        self.supportedArgs.addReqArg('aws_account')

    @log_enter_exit
    def handleList(self, confInfo):

        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())
        aws_account = self.callerArgs['aws_account'][0] or "default"

        acct = km.get_accesskey(name=aws_account)

        if not acct:
            raise Exception("No AWS Account is configured. Setup App first.")

        namespaces = taaws.s3util.list_cloudwatch_namespaces(self.callerArgs['aws_region'][0],acct.key_id,acct.secret_key, self.getSessionKey())
        confInfo['NameSpacesResult'].append('metric_namespace', namespaces)


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
