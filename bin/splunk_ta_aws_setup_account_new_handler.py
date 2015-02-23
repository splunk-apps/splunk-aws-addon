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
logger = setup_logger(APPNAME + '-RestEndpoints-account-new', level=logging.DEBUG)

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
            for arg in ['secret_key', 'key_id', 'fname']:
                self.supportedArgs.addOptArg(arg)

    @log_enter_exit
    def handleList(self, confInfo):
        # return
        confInfo['new_item'].append('fname', '')
        confInfo['new_item'].append('key_id', '')
        confInfo['new_item'].append('secret_key', '')

    @log_enter_exit
    def handleEdit(self, confInfo):
        logger.info("get config info: " + str(self.callerArgs))

        # name = self.callerArgs.name
        name = (self.callerArgs.data['fname'][0] or "").strip()
        key_id = (self.callerArgs.data['key_id'][0] or "").strip()
        secret_key = (self.callerArgs.data['secret_key'][0] or "").strip()

        logger.info("handle add name {}, key id {}, secret_key: {}".format(name, key_id, secret_key))

        name = name.strip()
        if name == "":
            logger.info("the name field is empty, ignore the request.")
            return

        if key_id == "":
            logger.error("raise exceptions, because key id is empty")
            raise Exception("Invalid Data for fields: Key Id")

        # filter name's invalid character
        name2 = "".join([c if c.isalnum() or c in "- _," else '' for c in name]).strip()

        if name != name2:
            logger.error("raise exceptions, because data is invalid: name=" + name)
            raise Exception("Invalid Data for fields: Account Name")

        # add or update
        from taaws.aws_accesskeys import AwsAccessKeyManager
        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())
        km.set_accesskey(key_id=key_id, secret_key=secret_key, name=name)

@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()