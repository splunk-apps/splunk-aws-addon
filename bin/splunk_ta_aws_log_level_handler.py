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
logger = setup_logger(APPNAME + '-RestEndpoints-log-level', level=logging.DEBUG)
from taaws.log_settings import get_level,set_level

def log_enter_exit(func):
    def wrapper(*args, **kwargs):
        logger.debug("{} entered.".format(func.__name__))
        result = func(*args, **kwargs)
        logger.debug("{} exited.".format(func.__name__))
        return result
    return wrapper

DATA_INPUTS=['aws_billing',
      'aws_cloudtrail',
      'aws_cloudwatch',
      'aws_config',
      'aws_s3',
      ]

class ConfigHandler(splunk.admin.MConfigHandler):



    @log_enter_exit
    def setup(self):

        if self.requestedAction == splunk.admin.ACTION_EDIT:
            for name in DATA_INPUTS:
                self.supportedArgs.addOptArg(name+'_log_level')

    @log_enter_exit
    def handleList(self, confInfo):

        for name in DATA_INPUTS:
            arg=name+'_log_level'
            level=get_level(name,self.getSessionKey(), appName=APPNAME)
            confInfo['log_level'].append(arg,level)

    @log_enter_exit
    def handleEdit(self, confInfo):
        try:
            for name in DATA_INPUTS:
                arg=name+'_log_level'
                level=self.callerArgs.data[arg][0]
                if not level in logging._levelNames:
                    level='INFO'
                set_level(name,self.getSessionKey(),level, appName=APPNAME)

        except Exception as e:
            logger.error("Failed to edit {}".format(str(e)))


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
