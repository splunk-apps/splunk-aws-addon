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
logger = setup_logger(APPNAME + '-RestEndpoints', level=logging.DEBUG)

from taaws.aws_accesskeys import AwsAccessKeyManager


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

        if self.requestedAction == splunk.admin.ACTION_EDIT:
            for arg in ['key_id', 'secret_key']:
                self.supportedArgs.addOptArg(arg)

    @log_enter_exit
    def handleList(self, confInfo):

        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())
        acct = km.get_accesskey()
        confInfo['default_aws_key'].append('key_id', acct and acct.key_id or '')
        confInfo['default_aws_key'].append('secret_key', acct and acct.secret_key or '')

    @log_enter_exit
    def handleEdit(self, confInfo):

        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())
        key_id = self.callerArgs.data['key_id'][0]
        key_id = key_id and key_id.strip() or ''
        secret_key = self.callerArgs.data['secret_key'][0]
        secret_key = secret_key and secret_key.strip() or ''
        if not key_id and not secret_key:
            km.delete_accesskey()
        else:
            km.set_accesskey(key_id=key_id, secret_key=secret_key)


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()