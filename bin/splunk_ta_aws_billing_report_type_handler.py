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
        pass

    @log_enter_exit
    def handleList(self, confInfo):
        confInfo['ReportTypesResultList'].append('monthly_report', ["None", "Monthly report",
                                                     "Monthly cost allocation report"])
        confInfo['ReportTypesResultList'].append('detail_report', ["None", "Detailed billing report",
                                                    "Detailed billing report with resources and tags"])

@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()