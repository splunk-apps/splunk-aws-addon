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

        if self.requestedAction == splunk.admin.ACTION_EDIT:
            for arg in ['delete', 'key_id', 'fname']:
                self.supportedArgs.addOptArg(arg)

    @log_enter_exit
    def handleList(self, confInfo):
        from taaws.credentialsRaw import CredentialManager
        cred_manager = CredentialManager(self.getSessionKey())
        ret = cred_manager.get_all_user_credentials(APPNAME)
        logger.info("get acct : " + str(ret))

        filter_prefix = "_aws_"

        for c in ret:
            if str(c["realm"]).lower().startswith(filter_prefix):
                continue
            confInfo[c["realm"]].append('fname', c["realm"])
            confInfo[c["realm"]].append('key_id', c["username"])
            confInfo[c["realm"]].append('delete', 0)

    @log_enter_exit
    def handleEdit(self, confInfo):
        # logger.info("get self info: " + str(self.__dict__))

        logger.info("get config info: " + str(self.callerArgs))

        # name = self.callerArgs.name
        name = self.callerArgs.data['fname'][0]
        key_id = self.callerArgs.data['key_id'][0]
        delete = self.callerArgs.data['delete'][0]

        logger.info("handle edit name {}, key id {}, delete: {}".format(name, key_id, delete))

        if delete != "1":
            return

        # delete key_id
        from taaws.aws_accesskeys import AwsAccessKeyManager
        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())
        km.delete_accesskey(name=name)

@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
