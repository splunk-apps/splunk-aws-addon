"""
Setup Handler
"""

import os
import sys

import splunk
import splunk.admin

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER

from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
sys.path.append(make_splunkhome_path(['etc', 'apps', APPNAME, 'lib']))

import logging
from taaws.log import setup_logger
logger = setup_logger(APPNAME + '-RestEndpoints-account-list', level=logging.DEBUG)

# from taaws.aws_accesskeys import AwsAccessKeyManager, AwsAccessKey


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
        self.supportedArgs.addOptArg('aws_account_service')

    @log_enter_exit
    def handleList(self, confInfo):
        from taaws.credentialsRaw import CredentialManager
        cred_manager = CredentialManager(self.getSessionKey())
        ret = cred_manager.get_all_user_credentials(APPNAME)
        logger.info("get acct : " + str(ret))

        rlist = []

        # we will fitler all user with prefix _AWS_DATA_
        filter_prefix = "_aws_"
        for c in ret:
            if not str(c["realm"]).lower().startswith(filter_prefix):
                rlist.append(c["realm"])

        confInfo['AwsAccountListResult'].append('accounts', rlist)

@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
