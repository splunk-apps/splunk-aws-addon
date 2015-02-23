"""
Custom REST Endpoint for enumerating AWS SQS queue names.
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
from taaws.s3util import connect_sqs

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

        import boto.sqs
        import boto.exception

        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, self.getSessionKey())
        aws_account = self.callerArgs['aws_account'][0] or "default"

        acct = km.get_accesskey(name=aws_account)

        if not acct:
            raise Exception("No AWS Account is configured. Setup App first.")

        sqs_conn = connect_sqs(self.callerArgs['aws_region'][0], acct.key_id, acct.secret_key, self.getSessionKey())

        if sqs_conn is None:
            raise Exception("Invalid SQS Queue Region: {}".format(self.callerArgs['aws_region'][0]))

        q_names = []
        try:
            q_list = sqs_conn.get_all_queues()
            if q_list:
                for q in q_list:
                    q_names.append(q.name)
        except boto.exception.SQSError as e:
                if e.status == 403 and e.reason == 'Forbidden':
                    if e.error_code == 'InvalidClientId':
                        raise Exception("Authentication Key ID is invalid. Check App Setup.")
                    elif e.error_code == 'SignatureDoesNotMatch':
                        raise Exception("Authentication Secret Key is invalid. Check App Setup.")
        except Exception as e:
                raise Exception("Failed AWS Authentication: {}: {} {} ({}): {}".format(
                    type(e).__name__, e.status, e.reason, e.error_code, e.error_message))

        confInfo['SqsQueueNamesResult'].append('sqs_queue_names', q_names)


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
