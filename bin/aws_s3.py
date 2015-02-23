"""
Modular Input for AWS S3
"""

import re
import sys
import os
import time
import hashlib
import datetime
import json
import errno
import shutil
import codecs
import socket

import taaws.s3util
import taaws.s3readers

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER
from taaws.aws_accesskeys import AwsAccessKeyManager

from splunklib import modularinput as smi
import splunklib.client
import splunklib.data


import boto.ec2
import boto.ec2.cloudwatch
import boto.exception
import boto.utils

import logging
from taaws.log import setup_logger
logger = setup_logger(os.path.basename(__file__), level=logging.ERROR)
from taaws.log_settings import get_level


#boto.set_file_logger('boto', r'C:\boto.log', level=logging.DEBUG)
def detect_unicode_by_bom(data):
    if data[:2]=='\xFE\xFF':
        return 'UTF-16BE'
    if data[:2]=='\xFF\xFE':
        return 'UTF-16LE'
    if data[:4]=='\x00\x00\xFE\xFF':
        return 'UTF-32BE'
    if data[:4]=='\xFF\xFE\x00\x00':
        return 'UTF-32LE'
    return 'UTF-8'

class Checkpointer(object):
    """Tracks state of current S3 items being processed."""

    class MaxTrackedItemsReached(Exception):
        """Exception raised by Checkpointer.get_item() when key names being tracked will exceed max_items."""
        pass

    class CheckpointItem(object):
        """Helper proxy for an item's state info array."""

        def __init__(self, checkpointer, key_name, state):
            """

            @param checkpointer: Checkpointer instance
            @param key_name: S3 key name of item
            @param state: state info array
            @return:
            """
            self._checkpointer = checkpointer
            self._key_name = key_name
            # state = [etag, last_modified, position, eof_reached, attempts]
            self._state = state

        @property
        def key_name(self):
            return self._key_name

        @property
        def etag(self):
            return self._state[0]

        @property
        def last_modified(self):
            return self._state[1]

        @property
        def position(self):
            return self._state[2]

        @position.setter
        def position(self, value):
            self._state[2] = value
            self._checkpointer._data_modified()

        @property
        def eof_reached(self):
            return self._state[3]

        @eof_reached.setter
        def eof_reached(self, value):
            self._state[3] = value
            self._checkpointer._data_modified()

        @property
        def attempt_count(self):
            return self._state[4]

        @attempt_count.setter
        def attempt_count(self, value):
            self._state[4] = value
            self._checkpointer._data_modified()

        @property
        def created(self):
            return self._state[5]

    @property
    def filepath(self):
        return self._filepath

    @property
    def last_completed_scan_datetime(self):

        return self._data['last_completed_scan_datetime']

    @last_completed_scan_datetime.setter
    def last_completed_scan_datetime(self, value):
        """Setter for utc datetime of when the last full scan of S3 keys was started. Used as the minimum datetime
        for filtering new items for processing.

        @param value: utc datetime
        @return:
        """
        logger.log(logging.DEBUG,'boto.utils.get_ts')
        self._data['last_completed_scan_datetime'] = boto.utils.get_ts(value.utctimetuple())
        logger.log(logging.DEBUG,'boto.utils.get_ts done')
        self._data_modified()

    def get_earliest_datetime(self):
        """Get the earliest datetime for filtering S3 keys.

        @return:
        """
        item = self.get_oldest_item()
        if item:
            return item.last_modified
        else:
            return self.last_completed_scan_datetime

    def _data_modified(self):
        """Mark checkpoint data as modified.

        @return:
        """
        if not self._data_modified_time:
            self._data_modified_time = time.time()

    def __init__(self, checkpoint_dir, stanza_name, bucket_name, server_uri,
                 last_completed_scan_datetime_init=None, max_items=None):
        """

        @param checkpoint_dir: base directory for checkpoint data
        @param stanza_name:
        @param bucket_name: S3 bucket name
        @param last_completed_scan_datetime_init: function to generate the initial earliest utc datetime for scanning.
        @param max_items: max items that can be processed (tracked) at one time.
        @return:
        """

        logger.log(logging.DEBUG,'Create Checkpointer')
        self._data = None
        self._data_modified_time = False

        self._max_items = max_items

        start_offset = 0
        if stanza_name.startswith('aws-s3://'):
            start_offset = len('aws-s3://')

        safe_filename_prefix = "".join([c if c.isalnum() else '_' for c in stanza_name[start_offset:start_offset + 20]])
        stanza_hexdigest = hashlib.md5("{}_{}".format(stanza_name, bucket_name)).hexdigest()
        self._filepath = os.path.join(checkpoint_dir, "cp_{}_{}.json".format(safe_filename_prefix, stanza_hexdigest))

        self._load(stanza_name, bucket_name, server_uri, last_completed_scan_datetime_init)

        logger.log(logging.INFO, "Checkpointer initialized {} {} {} ".format(checkpoint_dir,stanza_name,bucket_name))


    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        self.save(force=True)

    def __len__(self):
        return len(self._data['items'])

    def _load(self, stanza_name, bucket_name, server_uri, last_completed_scan_datetime_init=None):
        """Loads saved state information from checkpoint_dir

        @param stanza_name:
        @param bucket_name:
        @param last_completed_scan_datetime_init:
        @return:
        """
        logger.log(logging.DEBUG,'checkpointer load')
        logger.log(logging.DEBUG,'checkpointer: stanza_name = %s',stanza_name)
        try:
            # gzip makes it a lot slower.
            # with open(self.filepath, mode='rb') as f:
            #     with gzip.GzipFile(fileobj=f) as gz:
            #         self._data = json.load(gz)
            logger.log(logging.DEBUG,'open file: %s',self.filepath)
            with open(self.filepath, mode='r') as f:
                self._data = json.load(f)

        except IOError as e:
            logger.log(logging.DEBUG,'create empty checkpointer')
            if e.errno == errno.ENOENT:
                if not last_completed_scan_datetime_init:
                    last_completed_scan_datetime = datetime.datetime.utcnow()
                else:
                    last_completed_scan_datetime = last_completed_scan_datetime_init(server_uri)
                logger.log(logging.DEBUG,'boto.utils.get_ts')
                last_completed_scan_datetime = boto.utils.get_ts(last_completed_scan_datetime.utctimetuple())
                logger.log(logging.DEBUG,'boto.utils.get_ts done')

                self._data = {
                    'stanza_name': stanza_name,
                    'bucket_name': bucket_name,
                    'last_completed_scan_datetime': last_completed_scan_datetime,
                    'items': {}
                }
                self._data_modified()
            else:
                logger.log(logging.ERROR, "%s: %s", type(e).__name__, e)
                raise
        logger.log(logging.DEBUG,'checkpointer load done')

    def save(self, force=False):
        """Saves current checkpointer state to checkpoint_dir

        @param force:
        @return:
        """
        logger.log(logging.DEBUG,'checkpointer save')
        if not self._data_modified_time or not force and time.time() - self._data_modified_time < 15:
            logger.log(logging.DEBUG,'checkpointer save canceled: not modified or too close to last save')
            return

        stime = time.time()

        tfp = "{}.tmp".format(self.filepath)

        try:
            # gzip makes it slower.
            # with open(tfp, 'wb') as f:
            #     with gzip.GzipFile(filename=self.filepath, fileobj=f) as gz:
            #         json.dump(self._data, gz, indent=2)
            logger.log(logging.DEBUG,'dump to temp file: %s',tfp)
            with open(tfp, 'w') as f:
                json.dump(self._data, f, indent=2)
            logger.log(logging.DEBUG,'dump to temp file done')
        except Exception as e:
            logger.log(logging.ERROR, "%s: %s", type(e).__name__, e)
            raise
        else:
            try:
                logger.log(logging.DEBUG,'move temp file')
                shutil.move(tfp, self.filepath)
                logger.log(logging.DEBUG,'move temp file done')
            except Exception as e:
                logger.log(logging.ERROR, "%s: %s", type(e).__name__, e)
                raise
            else:
                self._data_modified_time = False
        finally:
            pass

        logger.log(logging.INFO, "Checkpointer saved %d items in %f seconds to: %s", len(self._data['items']),
                   time.time() - stime, self.filepath)

    def delete_item(self, key):
        """Delete S3 key from tracked items.

        @param key: S3 key
        @return:
        """

        logger.log(logging.DEBUG,'delete checkpointer item')
        try:
            logger.log(logging.DEBUG,'item keyname: %s',key.name)
            del self._data['items'][key.name]
            self._data_modified()
            logger.log(logging.DEBUG,'delete checkpointer item done')
        except NameError:
            logger.log(logging.WARNING,'delete checkpointer item failed - item does not exist')

    def get_item(self, key):
        """Get CheckpointItem for S3 key, or None if not valid.

        @param key: S3 key
        @return: CheckpointItem or None
        """

        logger.log(logging.DEBUG,'get checkpointer item')

        def _default_state():
            return [key.etag, key.last_modified, 0, False, 0, boto.utils.get_ts()]

        try:
            logger.log(logging.DEBUG,'item keyname: %s',key.name)
            state = self._data['items'][key.name]

            if key.etag != state[0]:
                # checkpointed item has changed...
                logger.log(logging.DEBUG,'checkpointed item has changed')
                state = _default_state()
                self._data['items'][key.name] = state
        except KeyError:

            if key.last_modified < self.last_completed_scan_datetime:
                return None

            if self._max_items and len(self._data['items']) >= self._max_items:
                raise self.MaxTrackedItemsReached(self._max_items)
            state = _default_state()
            self._data['items'][key.name] = state

        logger.log(logging.DEBUG,'get checkpointer item done')
        return self.CheckpointItem(self, key.name, state)

    def get_oldest_item(self):
        """Get oldest item in checkpoint data being tracked.

        @return: CheckpointItem or None
        """
        if not len(self._data['items']):
            return None

        oi = sorted(self._data['items'].iteritems(), key=lambda x: x[1][1])[0]
        return self.CheckpointItem(self, oi[0], oi[1])

    def prune(self, max_retries):
        """Removes completed items with a last_modified date older than last scan time or
        incomplete items with an attempt_count >= max_retries or cached item created is older than
        last_completed_scan_datetime - 10 days.

        @param max_retries: maximum allowed retries for failures before forcing a prune.
        @return: count pruned items.
        """

        logger.log(logging.DEBUG,'prune checkpointer items')
        pruned = 0
        pruned_incomplete = 0

        logger.log(logging.DEBUG,'boto.utils.parse_ts')
        max_age = boto.utils.parse_ts(self.last_completed_scan_datetime) - datetime.timedelta(days=10)
        logger.log(logging.DEBUG,'boto.utils.parse_ts done')
        logger.log(logging.DEBUG,'boto.utils.get_ts')
        max_age = boto.utils.get_ts(max_age.utctimetuple())
        logger.log(logging.DEBUG,'boto.utils.get_ts done')

        for key_name, state in self._data['items'].items():
            item = self.CheckpointItem(self, key_name, state)
            if item.eof_reached and item.last_modified < self.last_completed_scan_datetime:
                logger.log(logging.DEBUG,'prune: key_name = %s',key_name)
                del self._data['items'][key_name]
                pruned += 1
            # retries = attempt_count - 1
            elif item.attempt_count > max_retries or item.created < max_age:
                logger.log(logging.DEBUG,'prune_incomplete: key_name = %s',key_name)
                del self._data['items'][key_name]
                pruned_incomplete += 1

                if item.attempt_count > max_retries:
                    reason = "max_retries"
                else:
                    reason = "aged_out"
                logger.log(logging.WARNING, "Checkpointer pruned incomplete item: reason: %s key_name: %r,"
                                            "etag: %r, last_modified: %r, position: %r",
                           reason, item.key_name,
                           item.etag, item.last_modified, item.position)

        if pruned or pruned_incomplete:
            self._data_modified()
        logger.log(logging.DEBUG,'prune checkpointer items done')
        return pruned + pruned_incomplete


class MyScript(smi.Script):

    def __init__(self):
        super(MyScript, self).__init__()
        self._canceled = False
        self._ew = None

        self.input_name = None
        self.input_items = None

    def get_scheme(self):
        """overloaded splunklib modularinput method"""

        scheme = smi.Scheme("AWS S3")
        scheme.description = ("Collect and index log files stored in AWS S3.")
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False

        scheme.add_argument(smi.Argument("name", title="Name",
                                         description="Choose an ID or nickname for this configuration",
                                         required_on_create=True))
        scheme.add_argument(smi.Argument("aws_account", title="AWS Account",
                                         description="AWS account",
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("bucket_name", title="Bucket Name",
                                         description="Bucket name",
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("key_name", title="Key Name",
                                         description="Key name",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("recursion_depth", title="Recursion Depth",
                                         description="Only applies if key_name is a folder prefix (e.g., \"path/\")",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("initial_scan_datetime", title="Initital Scan DateTime",
                                         description="Initial scan datetime.",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("max_items", title="Max Items",
                                         description="Max trackable items.",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("max_retries", title="Max Item Retries",
                                         description="Max number of retry attempts to stream incomplete items.",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("whitelist", title="Whitelist Regex",
                                         description="Whitelist",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("blacklist", title="Blacklist Regex",
                                         description="Blacklist",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("host_name", title="S3 host name",
                                         description="S3 host name",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("is_secure", title="whether to use secure connection",
                                         description="whether to use secure connection",
                                         required_on_create=False, required_on_edit=False))

        return scheme

    @staticmethod
    def get_access_key_pwd_real(session_key="", aws_account_name="default"):
        logger.log(logging.DEBUG,'get_access_key_pwd_real')
        if not AwsAccessKeyManager:
            raise Exception("Access Key Manager needed is not imported sucessfully")
        logger.log(logging.DEBUG,'create AwsAccessKeyManager')
        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, session_key)
        logger.log(logging.DEBUG,'create AwsAccessKeyManager done')

        logger.info("get account name: " + aws_account_name)

        logger.log(logging.DEBUG,'get_accesskey')
        acct = km.get_accesskey(name=aws_account_name)
        logger.log(logging.DEBUG,'get_accesskey done')
        if not acct:
            # No recovering from this...
            logger.log(logging.FATAL, "No AWS Account is configured.")
            raise Exception("No AWS Account is configured.")
        logger.log(logging.DEBUG,'get_access_key_pwd_real done')
        return acct.key_id, acct.secret_key

    def validate_input(self, definition):
        """overloaded splunklib modularinput method"""

        self.input_items = definition.parameters

        session_key = definition.metadata['session_key']
        aws_account_name = self.input_items.get("aws_account") or "default"
        (key_id, secret_key) = self.get_access_key_pwd_real(session_key=session_key, aws_account_name=aws_account_name)

        try:
            whitelist = self.input_items.get('whitelist') or None
            if whitelist:
                whitelist = re.compile(whitelist)
        except Exception as e:
            raise Exception("whitelist: regex compile error: {}".format( e))
        try:
            blacklist = self.input_items.get('blacklist') or None
            if blacklist:
                blacklist = re.compile(blacklist)
        except Exception as e:
            raise Exception("blacklist: regex compile error: {}".format(e))

        try:
            recursion_depth = int(self.input_items.get('recursion_depth') or 0)
        except Exception as e:
            raise type(e)("recursion_deption: {}".format(e))

        try:
            self._get_initial_scan_datetime(uri=definition.metadata['server_uri'], token=definition.metadata['session_key'])
        except Exception as e:
            raise Exception("initial_scan_datetime: {}".format(e))

        s3_conn = taaws.s3util.connect_s3(key_id, secret_key, session_key)

        try:
            s3_bucket = s3_conn.get_bucket(self.input_items['bucket_name'], validate=True)
        except boto.exception.S3ResponseError as e:
                raise Exception("Failed AWS Validation: {}: {} {} ({}): {}".format(
                    type(e).__name__, e.status, e.reason, e.error_code, e.error_message))

        try:
            prefix = self.input_items.get('key_name', '')
            logger.log(logging.DEBUG, "Prefix is {}".format(prefix))
            if prefix:
                s3_key = s3_bucket.get_key(prefix)
                if s3_key is None:
                    pass ## There is an issue that for first level of key, it returns none!
                    ## TODO check the root cause, might be a boto bug
                    #raise NameError('key_name not found.')
        except boto.exception.S3ResponseError as e:
                raise Exception("Failed fetching key_name: {}: {} {} ({}): {}".format(
                    type(e).__name__, e.status, e.reason, e.error_code, e.error_message))

    def _exit_handler(self, signum, frame=None):
        self._canceled = True
        logger.log(logging.INFO, "cancellation received.")

        if os.name == 'nt':
            return True

    def _get_initial_scan_datetime(self, uri, token=None):
        """Helper function for using Splunk relative time by calling the Splunk timeparser REST endpoint
        to generate the initial datetime.

        @param token:
        @return:
        """

        logger.log(logging.DEBUG,'_get_initial_scan_datetime')
        relative_time = self.input_items.get('initial_scan_datetime')
        logger.log(logging.DEBUG,'_get_initial_scan_datetime : relative_time = %s',str(relative_time))
        # default is 7 days before now
        if not relative_time or relative_time == 'default':
            return datetime.datetime.utcnow()+datetime.timedelta(days=-7)

        if not token:
            token = self.service.token
        scheme, host, port = tuple(uri.replace("/", '').split(':'))

        logger.log(logging.DEBUG,'_get_initial_scan_datetime : create Sercive')
        service = splunklib.client.Service(token=token, scheme=scheme, host=host, port=port)
        logger.log(logging.DEBUG,'_get_initial_scan_datetime : create Endpoint')
        endpoint = splunklib.client.Endpoint(service, 'search/timeparser/')
        logger.log(logging.DEBUG,'_get_initial_scan_datetime : endpoint.get')
        endpoint.get(time=relative_time)
        r = endpoint.get(time=relative_time, output_time_format='%s')
        logger.log(logging.DEBUG,'_get_initial_scan_datetime : splunklib.data.load')
        r = splunklib.data.load(r.body.read()).response[relative_time]
        logger.log(logging.DEBUG,'_get_initial_scan_datetime done')
        return datetime.datetime.utcfromtimestamp(float(r))

    def _do_delay(self, seconds):
        for i in xrange(0, seconds):
            time.sleep(1)
            if self._canceled:
                break

    def _stream_s3_item(self, key, checkpoint_item):
        """Handles streaming single S3 item to Splunk

        @param key: S3 key
        @param checkpoint_item:
        @return:
        """
        class State(object):
            writing = False

        def _start(reader_type):
            pass

        def _stop(reader_type, position):
            _write_done_event(checkpoint=True)

        def _write_done_event(checkpoint=False):
            if State.writing:
                logger.log(logging.DEBUG,"create Event")
                event = smi.Event(source=reader.source_name, data='', unbroken=True, done=True)
                logger.log(logging.DEBUG,"write_done_event")
                self._ew.write_event(event)
                logger.log(logging.DEBUG,"write_done_event done")
                State.writing = False
            if checkpoint:
                checkpoint_item.position = reader.position

        try:
            retry_num = 0
            current_reader_pos = 0
            # If user set an encoding, use the user setting, or default to 'auto'. Used in incremental decoder.
            character_set=self.input_items.get('character_set') or 'auto'
            logger.log(logging.DEBUG,"character_set = %s",character_set)
            try:
                inc_decoder = codecs.getincrementaldecoder(character_set)(errors='replace')
            except LookupError:
                # If use 'auto' or encoding is invalid, auto decode utf8 with/without BOM, utf16/32 with BOM
                logger.log(logging.DEBUG,"character_set is auto or does not exist, try auto UTF detect")
                reader = taaws.s3readers.get_stream_reader(key, source_start_func=_start, source_stop_func=_stop)
                data = reader.read(1024)
                inc_decoder = codecs.getincrementaldecoder(detect_unicode_by_bom(data))(errors='replace')
                key.close(fast=True)

            while True:
                try:
                    if self._canceled:
                        break
                    logger.log(logging.DEBUG,"taaws.s3readers.get_stream_reader")
                    reader = taaws.s3readers.get_stream_reader(key, source_start_func=_start, source_stop_func=_stop)
                    logger.log(logging.DEBUG,"taaws.s3readers.get_stream_reader done")

                    if current_reader_pos:
                        logger.log(logging.DEBUG,"reader.seek_position : %s",str(current_reader_pos))
                        reader.seek_position(current_reader_pos)
                    elif checkpoint_item.position:
                        logger.log(logging.DEBUG,"reader.seek_position : %s",str(checkpoint_item.position))
                        reader.seek_position(checkpoint_item.position)
                    logger.log(logging.DEBUG,"reader.seek_position done")
                    while True:
                        if self._canceled:
                            break

                        data = reader.read(8192)
                        logger.log(logging.DEBUG,"reader.read(8192) done")

                        # If decoder is not initialized (meaning that this is the first loop with file header),
                        # initialize it with data
                        if inc_decoder is None:
                            inc_decoder = codecs.getincrementaldecoder(detect_unicode_by_bom(data))(errors='replace')

                        if not data:
                            checkpoint_item.eof_reached = True
                            break

                        logger.log(logging.DEBUG,"create Event")
                        event = smi.Event(source=reader.source_name, data=inc_decoder.decode(data), unbroken=True,
                                          done=False)
                        logger.log(logging.DEBUG, "write event data length={} ".format(len(data)))
                        self._ew.write_event(event)
                        logger.log(logging.DEBUG,"write event data done")
                        current_reader_pos = reader.position
                        State.writing = True

                    ## Adding by Gang to fix the dead loop bug
                    if checkpoint_item.eof_reached:
                        break

                except IOError as e:
                    # unchecked, it could also be stdout
                    if self._canceled:
                        break

                    retry_num += 1
                    if retry_num > 3:
                        raise
                    key.close(fast=True)
                    logger.log(logging.WARN, "Will Retry Resume: bucket: %r key: %r etag: %s retry_resume_num: %d "
                               " checkpoint_pos: %d, current_reader_pos: %d Exception: %s: %s",
                               key.bucket.name, key.name, key.etag, retry_num,
                               checkpoint_item.position, current_reader_pos, type(e).__name__, e)

                    self._do_delay(2**(retry_num-1))

        except Exception as e:
            raise
        finally:
            _write_done_event(checkpoint=False)

            if not self._canceled:
                checkpoint_item.attempt_count += 1

    def stream_events(self, inputs, ew):
        """overloaded splunklib modularinput method"""
        logger.setLevel(get_level(os.path.basename(__file__)[:-3],self.service.token, appName=APPNAME))
        logger.log(logging.INFO, "STARTED: {}".format(len(sys.argv) > 1 and sys.argv[1] or ''))

        self._ew = ew

        if os.name == 'nt':
            import win32api
            win32api.SetConsoleCtrlHandler(self._exit_handler, True)
        else:
            import signal
            signal.signal(signal.SIGTERM, self._exit_handler)
            signal.signal(signal.SIGINT, self._exit_handler)

        # because we only support one stanza...
        self.input_name, self.input_items = inputs.inputs.popitem()
        logger.log(logging.DEBUG, "Login Token is {}".format(self.service.token))

        session_key = self.service.token
        aws_account_name = self.input_items.get("aws_account") or "default"

        logger.log(logging.DEBUG, "get_access_key_pwd_real")
        (key_id, secret_key) = self.get_access_key_pwd_real(session_key=session_key, aws_account_name=aws_account_name)
        logger.log(logging.DEBUG, "get_access_key_pwd_real done")
        using_secure_connection = (self.input_items.get("is_secure") or 'true').lower() in (
            '1', 'true', 'yes', 'y', 'on')
        s3_host_name = self.input_items.get("host_name") or "s3.amazonaws.com"

        try:
            logger.log(logging.DEBUG, "taaws.s3util.connect_s3")
            s3_conn = taaws.s3util.connect_s3(key_id, secret_key, session_key, host=s3_host_name, is_secure=using_secure_connection)
            logger.log(logging.DEBUG, "taaws.s3util.connect_s3 done")
            try:
                logger.log(logging.DEBUG, "s3_conn.get_bucket")
                s3_bucket = s3_conn.get_bucket(self.input_items['bucket_name'])
                logger.log(logging.DEBUG, "s3_conn.get_bucket done")
            except socket.gaierror as e:
                logger.error('Host "%s" is invalid.' % s3_host_name)
                raise
            logger.log(logging.DEBUG, "connection success!")

            scan_start_datetime = datetime.datetime.utcnow()
            with Checkpointer(inputs.metadata['checkpoint_dir'],
                              self.input_name, self.input_items['bucket_name'], inputs.metadata['server_uri'],
                              last_completed_scan_datetime_init=self._get_initial_scan_datetime,
                              max_items=int(self.input_items.get('max_items') or 0)) as checkpointer:

                orig_size = len(checkpointer)
                max_retries = int(self.input_items.get('max_retries', 10))
                pruned = checkpointer.prune(max_retries)
                logger.log(logging.INFO, "Checkpointer pruned %d of %d items leaving %d items.",
                           pruned, orig_size, len(checkpointer))

                logger.log(logging.DEBUG, "Last scan time is {}".format(str(checkpointer.get_earliest_datetime())))

                logger.log(logging.DEBUG, "taaws.s3util.get_keys")
                for key in taaws.s3util.get_keys(s3_bucket,
                                                 prefix=self.input_items.get('key_name', ''),
                                                 recursion_depth=int(self.input_items.get('recursion_depth') or 0),
                                                 blacklist=self.input_items.get('blacklist') or None,
                                                 whitelist=self.input_items.get('whitelist') or None,
                                                 last_modified_after=checkpointer.last_completed_scan_datetime,
                                                 object_keys=True, prefix_keys=False):
                    logger.log(logging.DEBUG, "Handling one key %s ", key.name)
                    if self._canceled:
                        break

                    try:
                        cp_item = checkpointer.get_item(key)
                        if not cp_item:
                            continue

                        start_position = cp_item.position
                        logger.log(logging.INFO, "Starting: bucket: %r key: %r etag: %s retry_count: %d"
                                                 " orig_size: %d stream_position: %d"
                                                 " storage_class: %s last_modified: %s"
                                                 " datetime_delta: %s",
                                   s3_bucket.name, key.name, key.etag, cp_item.attempt_count,
                                   key.size, start_position,
                                   key.storage_class, key.last_modified,
                                   datetime.datetime.utcnow() - boto.utils.parse_ts(key.last_modified))

                        self._stream_s3_item(key, cp_item)
                        logger.log(logging.DEBUG, "Save item")
                        checkpointer.save(force=False)
                        logger.log(logging.DEBUG, "Save completed")

                    except Checkpointer.MaxTrackedItemsReached as e:
                        logger.log(logging.ERROR, "%s: %s", type(e).__name__, e)
                        break
                    except Exception as e:
                        if isinstance(e, boto.exception.S3ResponseError):
                            msg = "{} {} - {} - {}".format(e.status, e.reason, e.error_code, e.error_message)
                        else:
                            msg = e
                        logger.log(logging.ERROR, "Incomplete: bucket: %r key: %r etag: %s attempt_number: %d"
                                                  " orig_size: %d bytes_streamed: %d total_bytes_streamed: %d"
                                                  " Exception: %s: %s",
                                   s3_bucket.name, key.name, key.etag, cp_item.attempt_count,
                                   key.size, cp_item.position - start_position, cp_item.position,
                                   type(e).__name__, msg)
                        # re-raise everything else...
                        if not isinstance(e, boto.exception.S3ResponseError):
                            raise
                    else:
                        if cp_item.eof_reached:
                            level = logging.INFO
                            status = "Completed"
                        else:
                            level = logging.WARNING
                            status = "Incomplete"
                        logger.log(level, "%s: bucket: %r key: %r etag: %s attempt_number: %d"
                                          " orig_size: %d bytes_streamed: %d total_bytes_streamed: %d",
                                   status, s3_bucket.name, key.name, key.etag, cp_item.attempt_count,
                                   key.size, cp_item.position - start_position, cp_item.position)

                else:
                    logger.log(logging.INFO, "Completed full scan: %s",
                               datetime.datetime.utcnow() - scan_start_datetime)
                    checkpointer.last_completed_scan_datetime = scan_start_datetime

        except boto.exception.S3ResponseError as e:
                msg = "{} {} - {} - {}".format(e.status, e.reason, e.error_code, e.error_message)
                logger.log(logging.FATAL, "Outer catchall: %s: %s", type(e).__name__, msg)
                raise
        except Exception as e:
            logger.log(logging.FATAL, "Outer catchall: %s: %s", type(e).__name__, e)
            raise


if __name__ == "__main__":

    exitcode = MyScript().run(sys.argv)
    sys.exit(exitcode)
