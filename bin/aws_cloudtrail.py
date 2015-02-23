"""
Modular Input for AWS CloudTrail
"""

import sys
import os
import time
import calendar
import gzip
import io
import re
import json

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER

from taaws.aws_accesskeys import AwsAccessKeyManager
from splunklib import modularinput as smi

import boto.sqs
import boto.sqs.jsonmessage
import boto.s3.connection
import boto.exception

import taaws.s3util

import logging
from taaws.log import setup_logger
logger = setup_logger(os.path.basename(__file__), level=logging.ERROR)
from taaws.log_settings import get_level
import traceback

class MyScript(smi.Script):

    def __init__(self):

        super(MyScript, self).__init__()
        self._canceled = False
        self._ew = None

        self.input_name = None
        self.input_items = None

        self.remove_files_when_done = False
        self.exclude_describe_events = True
        self.blacklist = None
        self.blacklist_pattern = None

    def get_scheme(self):
        """overloaded splunklib modularinput method"""

        scheme = smi.Scheme("AWS CloudTrail")
        scheme.description = ("Collect and index log files produced by AWS CloudTrail."
                              " CloudTrail logging must be enabled and published to SNS topics and an SQS queue.")
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False
        # defaults != documented scheme defaults, so I'm being explicit.
        scheme.add_argument(smi.Argument("name", title="Name",
                                         description="Choose an ID or nickname for this configuration",
                                         required_on_create=True))
        scheme.add_argument(smi.Argument("aws_account", title="AWS Account",
                                         description="AWS account",
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("aws_region", title="SQS Queue Region",
                                         description=("Name of the AWS region in which the"
                                                      " notification queue is located. Regions should be entered as"
                                                      " e.g., us-east-1, us-west-2, eu-west-1, ap-southeast-1, etc."),
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("sqs_queue", title="SQS Queue Name",
                                         description=("Name of queue to which notifications of new CloudTrail"
                                                      " logs are sent. CloudTrail logging should be configured to"
                                                      " publish to an SNS topic. The queue should be subscribed"
                                                      " to the topics that notify of the desired logs. (Note that"
                                                      " multiple topics from different regions can publish to a single"
                                                      " queue if desired.)"),
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("exclude_describe_events", title="Exclude \"Describe*\" events",
                                         description=("Do not index \"Describe\" events. These events typically"
                                                      " constitute a high volume of calls, and indicate"
                                                      " read-only requests for information."),
                                         data_type=smi.Argument.data_type_boolean, required_on_create=False))
        scheme.add_argument(smi.Argument("remove_files_when_done", title="Remove log files when done",
                                         description=("Delete log files from the S3 bucket once they have been"
                                                      "read and sent to the Splunk index"),
                                         data_type=smi.Argument.data_type_boolean,
                                         required_on_create=False))
        scheme.add_argument(smi.Argument("blacklist", title="Blacklist for Describe events",
                                         description=("PCRE regex for specifying event names to be excluded. Leave blank"
                                                      " to use the default set of read-only event names"),
                                         required_on_create=False))
        scheme.add_argument(smi.Argument("excluded_events_index", title="Excluded events index",
                                         description=("Optional index in which to write the excluded events. Leave blank"
                                                      " to use the default of simply deleting events. Specified indexes"
                                                      " must be created in Splunk for this to be effective."),
                                         required_on_create=False))
        return scheme

    @staticmethod
    def get_access_key_pwd_real(session_key="", aws_account_name="default"):
        if not AwsAccessKeyManager:
            raise Exception("Access Key Manager needed is not imported sucessfully")
        logger.log(logging.DEBUG, "creating AwsAccessKeyManager...")
        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, session_key)
        logger.log(logging.DEBUG, "AwsAccessKeyManager created.")

        logger.info("get account name: " + aws_account_name)

        logger.log(logging.DEBUG, "getting access key...")
        acct = km.get_accesskey(name=aws_account_name)
        logger.log(logging.DEBUG, "access key ")
        if not acct:
            # No recovering from this...
            logger.log(logging.FATAL, "No AWS Account is configured.")
            raise Exception("No AWS Account is configured.")

        return acct.key_id, acct.secret_key

    def validate_input(self, definition):
        """overloaded splunklib modularinput method"""

        self.input_items = definition.parameters
        try:
            self.configure_blacklist()
        except Exception as e:
            raise Exception("Exclude Describe Events Blacklist Configuration: {}: {}".format(type(e).__name__, e))

        session_key = definition.metadata['session_key']
        aws_account_name = self.input_items.get("aws_account") or "default"
        (key_id, secret_key) = self.get_access_key_pwd_real(session_key=session_key, aws_account_name=aws_account_name)

        sqs_conn = taaws.s3util.connect_sqs(self.input_items['aws_region'], key_id, secret_key, session_key)

        if sqs_conn is None:
            raise Exception("Invalid SQS Queue Region: {}".format(self.input_items['aws_region']))

        s3_conn = taaws.s3util.connect_s3(key_id, secret_key, session_key)

        logger.log(logging.DEBUG, "Connection to s3 & sqs success!")

        sqs_queue = sqs_conn.get_queue(self.input_items['sqs_queue'])
        if sqs_queue is None:
            try:
                # verify it isn't an auth issue
                sqs_queues = sqs_conn.get_all_queues()
            except boto.exception.SQSError as e:
                if e.status == 403 and e.reason == 'Forbidden':
                    if e.error_code == 'InvalidClientId':
                        raise Exception("Authentication Key ID is invalid. Check App Setup.")
                    elif e.error_code == 'SignatureDoesNotMatch':
                        raise Exception("Authentication Secret Key is invalid. Check App Setup.")
            except Exception as e:
                    raise Exception("Failed AWS Validation: {}: {} {} ({}): {}".format(
                        type(e).__name__, e.status, e.reason, e.error_code, e.error_message))

            else:
                raise Exception("Invalid SQS Queue Name: {}".format(self.input_items['sqs_queue']))

    def _exit_handler(self, signum, frame=None):
        self._canceled = True
        logger.log(logging.INFO, "cancellation received.")

        if os.name == 'nt':
            return True

    def configure_blacklist(self, input_items=None):
        """Configure blacklist for events to ignore."""
        if input_items is None:
            input_items = self.input_items
        logger.log(logging.DEBUG, "getting blacklist config..")
        self.exclude_describe_events = (input_items.get('exclude_describe_events') or 'true').lower() in (
            '1', 'true', 'yes', 'y', 'on')
        self.blacklist = (input_items.get('blacklist') or "^(?:Describe|List|Get)").strip()
        self.blacklist = self.blacklist if (self.blacklist and self.exclude_describe_events) else None
        self.blacklist_pattern = re.compile(self.blacklist) if self.blacklist is not None else None
        logger.log(logging.DEBUG, "blacklist config done.")

    def stream_events(self, inputs, ew):
        """overloaded splunklib modularinput method"""
        logger.setLevel(get_level(os.path.basename(__file__)[:-3],self.service.token, appName=APPNAME))
        logger.log(logging.INFO, "STARTED: {}".format(len(sys.argv) > 1 and sys.argv[1] or ''))
        logger.log(logging.DEBUG, "Start streaming.")
        self._ew = ew

        logger.log(logging.DEBUG, "setting exit handler...")
        if os.name == 'nt':
            import win32api
            win32api.SetConsoleCtrlHandler(self._exit_handler, True)
        else:
            import signal
            signal.signal(signal.SIGTERM, self._exit_handler)
            signal.signal(signal.SIGINT, self._exit_handler)
        logger.log(logging.DEBUG, "exit handler set.")

        # because we only support one stanza...
        self.input_name, self.input_items = inputs.inputs.popitem()

        self.remove_files_when_done = (self.input_items.get('remove_files_when_done')or 'false').lower() in (
            '1', 'true', 'yes', 'y', 'on')
        self.configure_blacklist()

        logger.log(logging.DEBUG, "blacklist regex for eventNames is {}".format(self.blacklist))

        session_key = self.service.token
        aws_account_name = self.input_items.get("aws_account") or "default"
        logger.log(logging.DEBUG, "getting access key with account name %s",aws_account_name)
        (key_id, secret_key) = self.get_access_key_pwd_real(session_key=session_key, aws_account_name=aws_account_name)
        logger.log(logging.DEBUG, "access key got.")
        logger.log(logging.DEBUG, "taaws.s3util.connect_sqs")
        aws_region=self.input_items['aws_region']
        sqs_conn = taaws.s3util.connect_sqs(aws_region, key_id, secret_key, session_key)
        logger.log(logging.DEBUG, "taaws.s3util.connect_sqs done")
        if sqs_conn is None:
            # No recovering from this...
            logger.log(logging.FATAL, "Invalid SQS Queue Region: {}".format(self.input_items['aws_region']))
            raise Exception("Invalid SQS Queue Region: {}".format(self.input_items['aws_region']))

        try:
            logger.log(logging.DEBUG, "taaws.s3util.connect_s3")
            s3_conn = taaws.s3util.connect_s3_to_region(key_id, secret_key, session_key,aws_region)
            logger.log(logging.DEBUG, "taaws.s3util.connect_s3 done")
            logger.log(logging.DEBUG, "Connect to S3 & Sqs sucessfully")

            while not self._canceled:

                if self._canceled:
                    break
                logger.log(logging.DEBUG, "sqs_conn.get_queue")
                logger.log(logging.DEBUG, "sqs queue: %s",self.input_items['sqs_queue'])
                sqs_queue = sqs_conn.get_queue(self.input_items['sqs_queue'])
                logger.log(logging.DEBUG, "sqs_conn.get_queue done")
                if sqs_queue is None:
                    try:
                        # verify it isn't an auth issue
                        logger.log(logging.DEBUG, "sqs_conn.get_all_queues")
                        sqs_queues = sqs_conn.get_all_queues()
                        logger.log(logging.DEBUG, "sqs_conn.get_all_queues done")
                    except boto.exception.SQSError as e:
                        logger.log(logging.FATAL, "sqs_conn.get_all_queues(): {} {}: {} - {}".format(
                            e.status, e.reason, e.error_code, e.error_message))
                        raise
                    else:
                        logger.log(logging.FATAL, "sqs_conn.get_queue(): Invalid SQS Queue Name: {}".format(
                            self.input_items['sqs_queue']))
                        raise


                logger.log(logging.DEBUG, "sqs_queue.set_message_class")
                sqs_queue.set_message_class(boto.sqs.message.RawMessage)
                logger.log(logging.DEBUG, "sqs_queue.set_message_class done")

                # num_messages=10 was chosen based on aws pricing faq.
                # see request batch pricing: http://aws.amazon.com/sqs/pricing/
                logger.log(logging.DEBUG, "sqs_queue.get_messages")
                notifications = sqs_queue.get_messages(num_messages=10, visibility_timeout=20, wait_time_seconds=20)
                logger.log(logging.DEBUG, "sqs_queue.get_messages done")

                if not notifications or self._canceled:
                    logger.log(logging.DEBUG, "No message in SQS or cancellation received; will exit soon")
                    break


                start_time = time.time()
                completed, keys_to_delete, failed = self.process_notifications(s3_conn, notifications)

                # TODO: better handling for possible exception types from aws calls.
                # TODO: examine actual errors?
                # TODO: ensure proper (better?) recovery for transient errors on retry?
                if self.remove_files_when_done:  # this is a redundant check...
                    key_delete_errors = 0
                    for bucket_name, keys in keys_to_delete.iteritems():
                        logger.log(logging.DEBUG, "s3_conn.get_bucket %s",bucket_name)
                        s3_bucket = s3_conn.get_bucket(bucket_name, validate=False)
                        logger.log(logging.DEBUG, "s3_conn.get_bucket done")
                        logger.log(logging.DEBUG, "s3_bucket.delete_keys")
                        mdr = s3_bucket.delete_keys(keys, quiet=True)
                        logger.log(logging.DEBUG, "s3_bucket.delete_keys finished")
                        if mdr.errors:
                            key_delete_errors += len(mdr.errors)

                notification_delete_errors = 0
                if completed:
                    logger.log(logging.DEBUG, "sqs_queue.delete_message_batch(completed)")
                    br = sqs_queue.delete_message_batch(completed)
                    logger.log(logging.DEBUG, "sqs_queue.delete_message_batch done")
                    if br.errors:
                        notification_delete_errors = len(br.errors)

                ##  ADDON-2359 : fix the issue that invalid message will block the processing
                if failed:
                    logger.log(logging.DEBUG, "sqs_queue.delete_message_batch(failed)")
                    br = sqs_queue.delete_message_batch(failed)
                    logger.log(logging.DEBUG, "sqs_queue.delete_message_batch done")
                    if br.errors:
                        notification_delete_errors = len(br.errors)
                    failed_messages = ','.join([ m.get_body() for m in failed])
                    logger.log(logging.WARN, "Invalid notifications have been removed from SQS : {}".format(failed_messages))

                if self.remove_files_when_done:
                    logger.log(logging.INFO, ("{} completed, {} failed while processing a notification batch of {}"
                                              " [{} errors deleting {} notifications]"
                                              " [{} errors deleting {} keys in {} buckets]"
                                              "  Elapsed: {:.3f}s").format(
                           len(completed), len(failed), len(notifications),
                           notification_delete_errors, len(completed),
                           key_delete_errors, sum(len(v) for v in keys_to_delete.values()), len(keys_to_delete),
                           time.time() - start_time))
                else:
                    logger.log(logging.INFO, ("{} completed, {} failed while processing a notification batch of {}"
                                              " [{} errors deleting {} notifications]"
                                              "  Elapsed: {:.3f}s").format(
                           len(completed), len(failed), len(notifications),
                           notification_delete_errors, len(completed),
                           time.time() - start_time))

        except Exception as e:
            logger.log(logging.FATAL, "Outer catchall - Traceback:\n"+traceback.format_exc())
            raise

    def process_notifications(self, s3_conn, notifications):
        """Extract events from CloudTrail S3 logs referenced in SNS notifications."""

        completed = []
        failed = []
        keys_to_delete = {}
        for notification in notifications:
            if self._canceled:
                break
            try:
                envelope = json.loads(notification.get_body())
            except Exception as e:
                failed.append(notification)
                logger.log(logging.ERROR, "problems decoding notification JSON string: {} {}".format(
                    type(e).__name__, e))
                continue

            if not isinstance(envelope,dict):
                failed.append(notification)
                logger.log(logging.ERROR, "This doesn't look like a valid CloudTrail message. Please check SQS settings.")
                continue

            if all(key in envelope for key in ("Type", "MessageId", "TopicArn", "Message")) and isinstance(envelope['Message'],basestring):
                logger.log(logging.DEBUG, "This is considered a CloudTrail notification.")
                try:
                    envelope = json.loads(envelope['Message'])
                    if not isinstance(envelope,dict):
                        failed.append(notification)
                        logger.log(logging.ERROR, "This doesn't look like a valid CloudTrail message. Please check SQS settings.")
                        continue
                except Exception as e:
                    failed.append(notification)
                    logger.log(logging.ERROR, "problems decoding message JSON string: {} {}".format(
                        type(e).__name__, e))
                    continue

            if all(key in envelope for key in ("s3Bucket", "s3ObjectKey")):
                logger.log(logging.DEBUG, "This is considered a CloudTrail message. 'Raw Message Delivery' may be 'True'.")
                message=envelope
            else:
                failed.append(notification)
                logger.log(logging.ERROR, "This doesn't look like a CloudTrail notification or message. Please check SQS settings.")
                continue



            try:
                # defer validation to minimize queries.
                bucket_name = message['s3Bucket']
                logger.log(logging.DEBUG, "s3_conn.get_bucket %s",bucket_name)
                s3_bucket = s3_conn.get_bucket(bucket_name, validate=False)
                logger.log(logging.DEBUG, "s3_conn.get_bucket done")

                for key in message['s3ObjectKey']:
                    logger.log(logging.DEBUG, "s3_conn.get_key %s",key)
                    s3_file = s3_bucket.get_key(key)
                    logger.log(logging.DEBUG, "s3_conn.get_key done")
                    if s3_file is None:
                        file_json = {}
                    else:
                        logger.log(logging.DEBUG, "load gzip file")
                        with io.BytesIO(s3_file.read()) as bio:
                            with gzip.GzipFile(fileobj=bio) as gz:
                                file_json = json.loads(gz.read())
                        logger.log(logging.DEBUG, "load gzip file done")

            except boto.exception.S3ResponseError as e:

                # TODO: if e.error_code == 'NoSuchBucket' --- should we delete from queue also?
                # Or is this something that should be left for SQS Redrive?

                # We remove files from s3 before deleting the notifications, so it is possible in a fail
                # case to create this scenario.
                loglevel = logging.ERROR
                if e.status == 404 and e.reason == 'Not Found' and e.error_code in ('NoSuchKey',):
                    completed.append(notification)
                    loglevel = logging.WARN
                else:
                    failed.append(notification)

                edetail = e.body
                if e.body:
                    try:
                        elem = ET.fromstring(e.body)
                        edetail = elem.findtext('Key') or elem.findtext('BucketName') or ''
                    except Exception:
                        logger.log(logging.WARN,"Failed to parse the content from S3ResponseError : {}".format(e.body))
                        
                logger.log(loglevel, "{}: {} {}: {} - {}: {}".format(
                    type(e).__name__, e.status, e.reason, e.error_code, e.error_message, edetail))
                continue

            except ValueError as e:
                failed.append(notification)
                logger.log(logging.ERROR, "problems reading json from s3:{}/{}: {} {}".format(
                    message['s3Bucket'], key, type(e).__name__, e))
                continue

            except IOError as e:
                failed.append(notification)
                logger.log(logging.ERROR, "problems unzipping from s3:{}/{}: {} {}".format(
                    message['s3Bucket'], key, type(e).__name__, e))
                continue

            try:
                records = file_json.get('Records', [])
                logger.log(logging.INFO, "processing {} records in s3:{}/{}".format(
                    len(records), bucket_name, key))
            except KeyError as e:
                failed.append(notification)
                logger.log(logging.ERROR, "json not in expected format from s3:{}/{}: {} {}".format(
                    bucket_name, key, type(e).__name__, e))
                continue

            stats = {'written': 0, 'redirected': 0, 'discarded': 0}

            try:
                for idx, record in enumerate(records):
                    if self.blacklist_pattern is None or re.search(self.blacklist_pattern, record["eventName"]) is None:
                        logger.log(logging.DEBUG, "writing event {} with timestamp {}".format(
                            record['eventName'], record['eventTime']))

                        event = smi.Event(data=json.dumps(record),
                                          time=int(calendar.timegm(time.strptime(record['eventTime'].replace(
                                              "Z", "GMT"), "%Y-%m-%dT%H:%M:%S%Z"))),
                                          source="s3://{}/{}".format(bucket_name, key))
                        self._ew.write_event(event)
                        stats['written'] += 1
                    elif self.input_items.get('excluded_events_index'):
                        event = smi.Event(data=json.dumps(record),
                                          time=int(calendar.timegm(time.strptime(record['eventTime'].replace(
                                              "Z", "GMT"), "%Y-%m-%dT%H:%M:%S%Z"))),
                                          index=self.input_items['excluded_events_index'],
                                          source="s3://{}/{}".format(bucket_name, key))
                        self._ew.write_event(event)
                        stats['redirected'] += 1
                    else:
                        logger.log(logging.DEBUG, "blacklisted event"
                                                  " {} with timestamp {} being discarded".format(
                            record['eventName'], record['eventTime']))
                        stats['discarded'] += 1

                logger.log(logging.INFO, ("fetched {} records, wrote {}, discarded {}, redirected {}"
                                          " from s3:{}/{}").format(len(records), stats['written'], stats['discarded'],
                                          stats['redirected'], bucket_name, key))
                completed.append(notification)
            except IOError as e:
                if not self._canceled:
                    failed.append(notification)

                if stats['written'] or stats['redirected']:
                    logger.log(logging.ERROR,
                               "likely future duplicates:"
                               " {}while processing record {} of {} for s3:{}/{}: {} {}".format(
                                    "cancellation request received " if self._canceled else '', idx+1, len(records),
                                    bucket_name, key, type(e).__name__, e))
                break
            else:
                if self.remove_files_when_done:
                    if bucket_name not in keys_to_delete:
                        keys_to_delete[bucket_name] = []
                    keys_to_delete[bucket_name].append(key)

        return completed, keys_to_delete, failed


if __name__ == "__main__":

    exitcode = MyScript().run(sys.argv)
    sys.exit(exitcode)
