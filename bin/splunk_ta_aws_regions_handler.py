"""
Custom REST Endpoint for enumerating AWS regions.
"""

import os
import sys

import splunk
import splunk.admin

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER


import logging
from taaws.log import setup_logger
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

        self.supportedArgs.addReqArg('aws_service')

    @log_enter_exit
    def handleList(self, confInfo):

        if self.callerArgs.data['aws_service'][0] == 'cloudwatch':
            import boto.ec2.cloudwatch
            regions = boto.ec2.cloudwatch.regions()
        elif self.callerArgs.data['aws_service'][0] == 'cloudtrail':
            import boto.sqs
            regions = boto.sqs.regions()
        elif self.callerArgs.data['aws_service'][0] == 'awsconfig':
            import boto.sqs
            regions = boto.sqs.regions()
        else:
            raise ValueError("Unsupported aws_service specified.")

        rlist = []
        for r in regions:
            rlist.append(r.name)

        confInfo['RegionsResult'].append('regions', rlist)


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()