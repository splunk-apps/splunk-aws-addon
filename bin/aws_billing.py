"""
Modular Input for AWS Billing to grab AWS monthly CSV report and index into Splunk
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
import taaws.s3util
import taaws.s3readers
import csv
import logging
from taaws.log import setup_logger

from splunklib import modularinput as smi
import splunklib.client
import splunklib.data
import boto.exception
import boto.utils
from taaws.aws_accesskeys import AwsAccessKeyManager
import taaws.s3util
import zipfile
import traceback
import tempfile
from taaws.s3util import connect_s3

from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER

internal_is_secure = True
internal_blacklist = ".+"

logger = setup_logger(os.path.basename(__file__), level=logging.ERROR)
from taaws.log_settings import get_level

InvoiceIDColumnName = "InvoiceID"
DetailBillingReportFileMonthReg = "aws-billing-detailed-line-items-.*(\d{4}-\d{2})\.csv\.zip$"
MonthlyBillingReportFileMonthReg = "(\d{4}-\d{2})\.csv$"


def read_in_chunks(file_object, chunk_size=4096):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 4k."""
    logger.debug('read_in_chunks')
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data
    logger.debug('read_in_chunks done')

class CheckPointer(object):
    """Tracks state of current billing items being processed."""

    class MaxTrackedItemsReached(Exception):
        """Exception raised by Checkpointer.get_item() when key names being tracked will exceed max_items."""
        pass

    class CheckpointItem(object):
        """Helper proxy for an item's state info array."""

        def __init__(self, check_pointer, key_name, state):
            """

            @param check_pointer: CheckPointer instance
            @param key_name: S3 key name of item
            @param state: state info array
            @return:
            """
            self._check_pointer = check_pointer
            self._key_name = key_name
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
            self._check_pointer.data_modified()

        @property
        def eof_reached(self):
            return self._state[3]

        @eof_reached.setter
        def eof_reached(self, value):
            self._state[3] = value
            self._check_pointer.data_modified()

        @property
        def attempt_count(self):
            return self._state[4]

        @attempt_count.setter
        def attempt_count(self, value):
            self._state[4] = value
            self._check_pointer.data_modified()

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
        logger.debug('boto.utils.get_ts')
        self._data['last_completed_scan_datetime'] = boto.utils.get_ts(value.utctimetuple())
        logger.debug('boto.utils.get_ts done')
        self.data_modified()

    def get_earliest_datetime(self):
        """Get the earliest datetime for filtering S3 keys.

        @return:
        """
        item = self.get_oldest_item()
        if item:
            return item.last_modified
        else:
            return self.last_completed_scan_datetime

    def data_modified(self):
        """Mark checkpoint data as modified.

        @return:
        """
        if not self._data_modified_time:
            self._data_modified_time = time.time()

    def __init__(self, checkpoint_dir, stanza_name, bucket_name,
                 last_completed_scan_datetime_init=None, max_items=None):
        """

        @param checkpoint_dir: base directory for checkpoint data
        @param stanza_name:
        @param bucket_name: S3 bucket name
        @param last_completed_scan_datetime_init: function to generate the initial earliest utc datetime for scanning.
        @param max_items: max items that can be processed (tracked) at one time.
        @return:
        """

        logger.debug('create checkpointer')
        self._data = None
        self._data_modified_time = False

        self._max_items = max_items

        start_offset = 0

        if stanza_name.startswith('aws-billing://'):
            start_offset = len('aws-billing://')

        safe_filename_prefix = "".join([c if c.isalnum() else '_' for c in stanza_name[start_offset:start_offset + 20]])
        stanza_hexdigest = hashlib.md5("{}_{}".format(stanza_name, bucket_name)).hexdigest()
        self._filepath = os.path.join(checkpoint_dir, "cp_{}_{}.json".format(safe_filename_prefix, stanza_hexdigest))

        self._load(stanza_name, bucket_name, last_completed_scan_datetime_init)
        logger.debug('create checkpointer done')

    def __enter__(self):

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info("exiting:" + str(exc_type) + "," + str(exc_val) + "," + str(exc_tb))
        self.save(force=True)

    def __len__(self):
        return len(self._data['items'])

    def _load(self, stanza_name, bucket_name, last_completed_scan_datetime_init=None):
        """Loads saved state information from checkpoint_dir

        @param stanza_name:
        @param bucket_name:
        @param last_completed_scan_datetime_init:
        @return:
        """
        logger.debug('checkpointer load')
        logger.debug('checkpointer: stanza_name = %s',stanza_name)
        try:
            # gzip makes it a lot slower.
            # with open(self.filepath, mode='rb') as f:
            #     with gzip.GzipFile(fileobj=f) as gz:
            #         self._data = json.load(gz)
            logger.debug('open file: %s',self.filepath)
            with open(self.filepath, mode='r') as f:
                self._data = json.load(f)
        except IOError as e:
            logger.debug('create empty checkpointer')
            if e.errno == errno.ENOENT:
                if not last_completed_scan_datetime_init:
                    last_completed_scan_datetime = datetime.datetime.utcnow()
                else:
                    last_completed_scan_datetime = last_completed_scan_datetime_init()
                logger.debug('boto.utils.get_ts')
                last_completed_scan_datetime = boto.utils.get_ts(last_completed_scan_datetime.utctimetuple())
                logger.debug('boto.utils.get_ts done')

                self._data = {
                    'stanza_name': stanza_name,
                    'bucket_name': bucket_name,
                    'last_completed_scan_datetime': last_completed_scan_datetime,
                    'items': {}
                }
                self.data_modified()
            else:
                logger.log(logging.ERROR, "%s: %s", type(e).__name__, e)
                raise
        logger.debug('checkpointer load done')

    def save(self, force=False):
        """Saves current checkpointer state to checkpoint_dir

        @param force:
        @return:
        """
        logger.debug('checkpointer save')
        if not self._data_modified_time or not force and time.time() - self._data_modified_time < 15:
            logger.debug('checkpointer save canceled: not modified or too close to last save')
            return

        stime = time.time()

        tfp = "{}.tmp".format(self.filepath)

        try:
            # gzip makes it slower.
            # with open(tfp, 'wb') as f:
            #     with gzip.GzipFile(filename=self.filepath, fileobj=f) as gz:
            #         json.dump(self._data, gz, indent=2)
            logger.debug('dump to temp file: %s',tfp)
            with open(tfp, 'w') as f:
                json.dump(self._data, f, indent=2)
            logger.debug('dump to temp file done')
        except Exception as e:
            logger.log(logging.ERROR, "%s: %s", type(e).__name__, e)
            raise
        else:
            try:
                logger.debug('move temp file')
                shutil.move(tfp, self.filepath)
                logger.debug('move temp file done')
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
        logger.debug('delete checkpointer item')
        try:
            logger.debug('item keyname: %s',key.name)
            del self._data['items'][key.name]
            self.data_modified()
            logger.debug('delete checkpointer item done')
        except NameError:
            logger.log(logging.WARNING,'delete checkpointer item failed - item does not exist')

    def get_item(self, key):
        """Get CheckpointItem for S3 key, or None if not valid.

        @param key: S3 key
        @return: CheckpointItem or None
        """
        logger.debug('get checkpointer item')
        def _default_state(position=None):
            if position is None:
                position = dict()
            return [key.etag, key.last_modified, position, False, 0, boto.utils.get_ts()]

        try:
            logger.debug('item keyname: %s',key.name)
            state = self._data['items'][key.name]

            # file is changed. reset finish tag
            if key.etag != state[0]:
                logger.debug('checkpointed item has changed')
                # checkpointed item has changed, reset finish tag
                # if it's a monthly report, we re-use state or else we ignore it
                if key.name.endswith(".csv"):
                    state = _default_state(state[2])
                else:
                    state = _default_state(None)
                self._data['items'][key.name] = state
        except KeyError:

            # if it's a not scanned before, scan it regardless of the modified time
            # if key.last_modified < self.last_completed_scan_datetime:
            #     return None

            if self._max_items and len(self._data['items']) >= self._max_items:
                raise self.MaxTrackedItemsReached(self._max_items)
            state = _default_state()
            self._data['items'][key.name] = state

        # check if already scanned or not
        item = self.CheckpointItem(self, key.name, state)
        logger.debug('get checkpointer item done')
        if item.eof_reached:
            return None

        return item

    def get_oldest_item(self):
        """Get oldest item in checkpoint data being tracked.

        @return: CheckpointItem or None
        """
        if not len(self._data['items']):
            return None

        oi = sorted(self._data['items'].iteritems(), key=lambda x: x[1][1])[0]
        return self.CheckpointItem(self, oi[0], oi[1])


class MyScript(smi.Script):

    def __init__(self):
        super(MyScript, self).__init__()
        self._canceled = False
        self._ew = None

        self.input_name = None
        self.input_items = None
        self.definition = None

    def get_scheme(self):
        """overloaded splunklib modularinput method"""

        scheme = smi.Scheme("AWS Billing")
        scheme.description = "Collect and index billing report of AWS in CSV format located in AWS S3 bucket."
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = False

        scheme.add_argument(smi.Argument("name", title="Name",
                                         description="Choose an ID or nickname for this configuration",
                                         required_on_create=True))
        scheme.add_argument(smi.Argument("bucket_name", title="Bucket Name",
                                         description="Bucket name",
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("aws_account", title="AWS Account",
                                         description="AWS account",
                                         required_on_create=True, required_on_edit=True))

        scheme.add_argument(smi.Argument("monthly_report_type",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("detail_report_type",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("report_file_match_reg", title="Report File Match Regex",
                                         description="Using regex instead to match configure AWS report file. "
                                                     "Note: If it's not blank value, the selected report types "
                                                     "will be ignored.",
                                         required_on_create=False, required_on_edit=False))

        scheme.add_argument(smi.Argument("recursion_depth", title="Recursion Depth",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("initial_scan_datetime", title="Initial Scan Date timestamp",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("time_format_list", title="Time format list",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("monthly_timestamp_select_column_list", title="Column list used for monthly "
                                                                                       "report event's timestamp",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("detail_timestamp_select_column_list", title="Column list used for detail "
                                                                                      "report event's timestamp",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("max_file_size_csv_in_bytes", title="Max file size in bytes for CSV file",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("max_file_size_csv_zip_in_bytes",
                                         title="Max file size in bytes for CSV Zip file",
                                         required_on_create=False, required_on_edit=False))

        scheme.add_argument(smi.Argument("header_look_up_max_lines",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("header_magic_regex",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("monthly_real_timestamp_extraction",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("monthly_real_timestamp_format_reg_list",
                                         required_on_create=False, required_on_edit=False))

        return scheme

    def _get_initial_scan_datetime(self, token=None):
        """Helper function for using Splunk relative time by calling the Splunk timeparser REST endpoint
        to generate the initial datetime.

        @param token:
        @return:
        """
        logger.debug('_get_initial_scan_datetime')
        relative_time = self.input_items.get('initial_scan_datetime') or None
        logger.debug('_get_initial_scan_datetime : relative_time = %s',str(relative_time))
        if not relative_time or relative_time == 'now':
            return datetime.datetime.utcnow()

        if not token:
            token = self.service.token
        logger.debug('_get_initial_scan_datetime : create Sercive')
        service = splunklib.client.Service(token=token)
        logger.debug('_get_initial_scan_datetime : create Endpoint')
        endpoint = splunklib.client.Endpoint(service, 'search/timeparser/')
        logger.debug('_get_initial_scan_datetime : endpoint.get')

        endpoint.get(time=relative_time)
        r = endpoint.get(time=relative_time, output_time_format='%s')
        logger.debug('_get_initial_scan_datetime : splunklib.data.load')
        r = splunklib.data.load(r.body.read()).response[relative_time]
        logger.debug('_get_initial_scan_datetime done')
        return datetime.datetime.utcfromtimestamp(float(r))

    def validate_input(self, definition):
        """overloaded splunklib modularinput method"""

        self.input_items = definition.parameters
        self.definition = definition

        logger.info("valid date input: input items: " + str(self.input_items))

        try:
            # check report files & regex
            report_file_match_reg = (self.input_items.get('report_file_match_reg') or "").strip()
            if report_file_match_reg != "":
                try:
                    re.compile(report_file_match_reg)
                except Exception:
                    raise Exception("Report File Match Regex configured is not a valid regular expression.")
            else:
                # verify
                monthly_report_type = self.input_items.get('monthly_report_type') or "None"
                if monthly_report_type not in ["None", "Monthly report", "Monthly cost allocation report"]:
                    raise Exception("The monthly report type is not valid. Please use one of option: "
                                    "'None', 'Monthly report', 'Monthly cost allocation report'")
                detail_report_type = self.input_items.get('detail_report_type') or "None"
                if detail_report_type not in ["None",
                                              "Detailed billing report",
                                              "Detailed billing report with resources and tags"]:
                    raise Exception("The detail report type is not valid. Please use one of option: "
                                    "'None', 'Detailed billing report', "
                                    "'Detailed billing report with resources and tags'")

                if monthly_report_type == "None" and detail_report_type == "None":
                    raise Exception("At least select one report type or configure customized report match regex.")

            session_key = definition.metadata['session_key']
            # check init scan date time
            try:
                self._get_initial_scan_datetime(token=session_key)
            except Exception as e:
                raise Exception("initial_scan_datetime: {}".format(e))

            # check bucket
            (key_id, secret_key) = self.get_access_key_pwd()
            s3_conn = connect_s3(key_id,
                                 secret_key,
                                 session_key,
                                 is_secure=internal_is_secure)

            try:
                s3_conn.get_bucket(self.input_items['bucket_name'], validate=True)
            except boto.exception.S3ResponseError as e:
                    raise Exception("Failed AWS Validation: {}: {} {} ({}): {}".format(
                        type(e).__name__, e.status, e.reason, e.error_code, e.error_message))
        except Exception as e:
            logger.error("get exception while validating input: " + e.message)
            logger.error(traceback.format_exc())
            raise

    def _exit_handler(self, signum, frame=None):
        self._canceled = True
        logger.log(logging.INFO, "cancellation received. sign num=" + str(signum) + " frame=" + str(frame))

        if os.name == 'nt':
            return True

    def _do_delay(self, seconds):
        for i in xrange(0, seconds):
            time.sleep(1)
            if self._canceled:
                break

    @staticmethod
    def get_last_second_stamp_from_month_str(file_month):
        logger.debug('get_last_second_stamp_from_month_str')
        # param file_month: %Y-%m format
        date = datetime.datetime.strptime(file_month, '%Y-%m')
        if date.month < 12:
            date_str = str(date.year) + "-" + str(date.month+1)
        else:
            date_str = str(date.year+1) + "-01"

        date = datetime.datetime.strptime(date_str, '%Y-%m')
        delta = datetime.timedelta(seconds=1)
        date = date - delta
        time_stamp = int(time.mktime(date.timetuple()))
        logger.debug("get last second " + str(time_stamp) + " from month: " + file_month)

        return time_stamp

    def _stream_detail_billing_item(self, key, checkpoint_item, checkpointer):
        max_size = int(self.input_items.get("max_file_size_csv_zip_in_bytes") or 500000000)
        if key.size > max_size:
            msg = "ignore the file key: " + key.name + \
                  ", exceeds the maximum file, size=" + str(key.size) + " configured size=" + str(max_size)
            logger.warn(msg)
            logger.debug('_stream_detail_billing_item cancelled')
            return

        # update: check if the file's this month, if so, ignore it
        file_month_for_file_reg = DetailBillingReportFileMonthReg
        file_month = ""
        this_month = time.strftime('%Y-%m', time.localtime(time.time()))
        grps = re.search(file_month_for_file_reg, key.name)
        if grps is not None:
            grps = grps.groups()
            file_month = grps[len(grps)-1:][0].strip()

        if file_month == "":
            logger.error("fail to get month from file name, cancel processing file. file=" + key.name)
            return
        if file_month == this_month:
            logger.warn("Only detail billing report for non-current month is process. ignore it: " + key.name)
            return

        # prepare a default timestamp for this detail reprot using the file month's last seconds
        default_event_time_stamp = self.get_last_second_stamp_from_month_str(file_month)
        logger.info("prepare default timestamp for this file: " + str(default_event_time_stamp))
        logger.debug('create tempfile.NamedTemporaryFile')
        temp_csv_zip_file = tempfile.NamedTemporaryFile()
        logger.debug('create tempfile.NamedTemporaryFile done')
        # temp_csv_folder_path = os.path.splitext(temp_csv_zip_file.name)[0]
        try:
            logger.debug("key.get_contents_to_file : start to download file from S3 bucket to local. remote="
                         + key.name + " local=" + temp_csv_zip_file.name)
            key.get_contents_to_file(temp_csv_zip_file)
            logger.debug('key.get_contents_to_file done')
            temp_csv_zip_file.seek(0)
        except boto.exception.S3ResponseError as emsg:
            logger.fatal('S3ResponseError : key:' + key.name + ' ' + str(emsg[0])+' '+emsg[1]+' '+str(emsg[2])+'\n')
            logger.error(traceback.format_exc())
            return

        csv_file_list_set = dict()

        with FileHolder() as csv_files_monitor:
            # extract downloaded file to csv
            with FileHolder(temp_csv_zip_file):
                # add unzip folder into monitor list
                logger.debug('create zipfile.ZipFile')
                zip_handle = zipfile.ZipFile(temp_csv_zip_file)
                logger.debug('create zipfile.ZipFile done')
                logger.debug('loop zip_handle.namelist')
                for sub_file in zip_handle.namelist():
                    if self._canceled:
                        logger.warning("canceled by external...exit loop")
                        break

                    temp_file = tempfile.NamedTemporaryFile()
                    csv_files_monitor.add_monitor(temp_file)

                    logger.info("extracting file from: " + sub_file + ", to: " + temp_file.name)
                    with zip_handle.open(sub_file) as sub_file_handler:
                        for piece in read_in_chunks(sub_file_handler):
                            if self._canceled:
                                logger.warning("canceled by external...exit loop")
                                break
                            temp_file.write(piece)

                    temp_file.seek(0)
                    csv_file_list_set[sub_file] = temp_file
                logger.debug('loop zip_handle.namelist done')
            # process csv file and record the line number process now
            #  load previous process line number and recover
            #  - extract header
            for (file_name, opened_file) in csv_file_list_set.items():
                try:
                    logger.info("start to processing file: " + file_name + ", temp file path: " + opened_file.name)
                    if self._canceled:
                        logger.warning("canceled by external...exit loop")
                        raise Exception("canceled by external. exist parsing current file. ")

                    check_point_name = file_name
                    logger.debug('create csv.reader')
                    # On windows, we need to reuse the file because cannot open it for multiple times.
                    if os.name == 'nt':
                        reader = csv.reader(opened_file,
                                            skipinitialspace=True, delimiter=',', quotechar='"')
                    else:
                        try:
                            reader = csv.reader(open(opened_file.name, "rU"),
                                                skipinitialspace=True, delimiter=',', quotechar='"')
                        except Exception as e:
                            logger.warn("fail to open the reader, use original file handler. reason:" + e.message)
                            reader = csv.reader(opened_file,
                                                skipinitialspace=True, delimiter=',', quotechar='"')

                    logger.debug('create csv.reader done')
                    # get header_dict
                    look_up_header_count = int(self.input_items.get("header_look_up_max_lines") or 3)

                    # get header_dict
                    header = None
                    try:
                        header_magic_reg = self.input_items.get("header_magic_regex") or ".+,.+,"
                        logger.debug("get header magic reg: " + header_magic_reg)
                        pattern = re.compile(header_magic_reg)
                        for i in xrange(look_up_header_count):
                            header = next(reader)
                            if pattern.search(','.join(header)):
                                logger.debug("find header: " + str(header))
                                break

                        if header is None:
                            logger.error("There's no suitable header found.")
                            raise Exception("There's no suitable header found.")
                    except StopIteration as e:
                        logger.error("there's no header_dict in the csv file. exit. ex: " + e.message)
                        raise

                    index = 0
                    header_dict = dict()
                    for el in header:
                        header_dict[el] = index
                        index += 1

                    #
                    # get first unchecked reserved id
                    #

                    ts_cl_lst_str = self.input_items.get("detail_timestamp_select_column_list") or ""

                    timestamp_column_index_list = list()
                    for column in ts_cl_lst_str.rstrip().split("|"):
                        try:
                            timestamp_column_index_list.append(header_dict[column.strip()])
                        except KeyError:
                            logger.warn("there's no column in the header: " + column)

                    if not timestamp_column_index_list:
                        logger.error("there's no any timestamp column in header")
                        raise Exception("there's no any timestamp column in header")

                    # iterate till first non scanned row
                    length = len(header_dict)

                    last_scanned_detail_record_set = checkpoint_item.position
                    last_scanned_record_number = -1
                    if check_point_name in last_scanned_detail_record_set:
                        last_scanned_record_number = last_scanned_detail_record_set[check_point_name] or -1

                    # continue process
                    row_id = -1
                    first_row = True
                    for row in reader:
                        if self._canceled:
                            logger.warning("canceled by external...exit loop")
                            raise Exception("canceled by external. exist parsing current row. ")

                        # jump to last scanned row id
                        row_id += 1
                        if row_id <= last_scanned_record_number:
                            logger.debug("current row is scanned, skip it. row_id=" + str(row_id)
                                         + " last scanned id=" + str(last_scanned_record_number))
                            continue

                        try:
                            if not isinstance(row, list):
                                raise Exception("unexpected row, is not list: " + row)

                            if len(row) <= min(timestamp_column_index_list):
                                raise Exception("row column count doesn't contains date column, row: " + str(row))

                            # check invoice ID, if it's not final version, ignore it.
                            if first_row:
                                first_row = False
                                invoice_index = header_dict.get(InvoiceIDColumnName, -1)
                                if invoice_index == -1:
                                    logger.error("cannot get invoice ID column, exit processing this file.")
                                    break
                                invoice_str = row[invoice_index]
                                try:
                                    invoice_id = int(invoice_str)
                                except ValueError:
                                    logger.warn("fail to get the invoice id, it's not the final version, skip it, "
                                                "invoice str= " + invoice_str)
                                    break

                            #
                            # now, process this row
                            #

                            # get time

                            time_str = ""
                            for timestamp_index in timestamp_column_index_list:
                                time_str = row[timestamp_index]
                                if time_str:
                                    break
                            if not time_str:
                                event_time = default_event_time_stamp
                                logger.info("there's no available timestamp for current row, use default one: "
                                            + str(row))
                            else:
                                event_time = self.get_event_time(time_str)

                            # get data, row to K-V
                            data = ""
                            for i in range(length):
                                if ',' in row[i]:
                                    data += '{}="{}", '.format(header[i], row[i])
                                else:
                                    data += '{}={}, '.format(header[i], row[i])

                            if data.endswith(", "):
                                data = data[:-2]

                            logger.debug("index line, time= " + str(event_time) + " row=" + str(row) + " data=" + data)

                            logger.debug('create smi.Event')
                            event = smi.Event(time=event_time,
                                              source="s3://" + self.input_items.get('bucket_name')
                                                     + "/" + key.name + ":" + key.last_modified,
                                              data=data, unbroken=True, done=True)
                            logger.debug('create smi.Event done')

                            logger.debug('write_event')
                            self._ew.write_event(event)
                            logger.debug('write_event done')
                        except Exception as e:
                            logger.error("there's error to handle this row: " + e.message
                                         + ", record its checksum as handled.")
                            logger.error(traceback.format_exc())
                        else:
                            checkpoint_item.position[check_point_name] = row_id
                            last_scanned_record_number = row_id
                            checkpoint_item.position = checkpoint_item.position  # trigger LMT changes
                            checkpointer.save()

                except Exception as e:
                    logger.error("fail to parse key: " + key.name + " get error: " + e.message + ", file: " + file_name)
                    logger.error(traceback.format_exc())
                    return
                else:
                    checkpoint_item.eof_reached = True
                finally:
                    checkpointer.save(True)

    def _stream_monthly_billing_item(self, key, checkpoint_item, checkpointer):
        max_size = int(self.input_items.get("max_file_size_csv_in_bytes") or 1000000)
        if key.size > max_size:
            raise Exception("this file size exceeds the maximum file, size="
                            + str(key.size) + " configured size=" + str(max_size))

        # update: check if the file's this month, if so, ignore it
        file_month_for_file_reg = MonthlyBillingReportFileMonthReg
        file_month = ""
        grps = re.search(file_month_for_file_reg, key.name)
        if grps is not None:
            grps = grps.groups()
            file_month = grps[len(grps)-1:][0].strip()

        if file_month == "":
            logger.error("fail to get month from file name, cancel processing file. file=" + key.name)
            return

        # prepare a default timestamp for this detail reprot using the file month's last seconds
        default_event_time_stamp = self.get_last_second_stamp_from_month_str(file_month)
        logger.info("prepare default timestamp for this file: " + str(default_event_time_stamp))

        # read content from key
        try:
            logger.info("start to get the file content from s3, file: " + key.name)
            logger.debug('key.get_contents_as_string')
            content = key.get_contents_as_string()
            logger.debug('key.get_contents_as_string done')
        except boto.exception.S3ResponseError as emsg:
            logger.fatal('S3ResponseError : key:' + key.name + ' ' + str(emsg[0])+' '+emsg[1]+' '+str(emsg[2])+'\n')
            return

        try:
            # try to extract real timestamp
            global_timestamp_field_reg_list = [x.strip() for x in
                                               (self.input_items.get("monthly_real_timestamp_format_reg_list") or "")
                                               .strip().split("|")]
            global_timestamp_extraction = self.input_items.get("monthly_real_timestamp_extraction") or ""
            for reg in global_timestamp_field_reg_list:
                global_timestamp_extraction = global_timestamp_extraction.replace("%TIME_FORMAT_REGEX%", reg)
            global_time_str = None

            try:
                if global_timestamp_extraction != "":
                    re_search = re.search(global_timestamp_extraction, content)
                    if re_search is not None:
                        grps = re.search(global_timestamp_extraction, content)
                        if grps is not None:
                            grps = grps.groups()
                            global_time_str = grps[len(grps)-1:][0].strip()
                            logger.info("extract global time string for this report: " + global_time_str)
            except Exception as ex:
                logger.error("catch exception while parsing global time str: " + ex.message +
                             ", extraciton setting: " + global_timestamp_extraction)

            logger.debug('create csv.reader')
            reader = csv.reader(content.splitlines(), skipinitialspace=True, delimiter=',', quotechar='"')
            logger.debug('create csv.reader done')
            # get header_dict
            look_up_header_count = int(self.input_items.get("header_look_up_max_lines") or 3)

            header = None
            try:
                header_magic_reg = self.input_items.get("header_magic_regex") or ".+,.+,"
                logger.debug("get header magic reg: " + header_magic_reg)
                pattern = re.compile(header_magic_reg)
                for i in xrange(look_up_header_count):
                    header = next(reader)
                    if pattern.search(','.join(header)):
                        logger.debug("find header: " + str(header))
                        break

                if header is None:
                    raise Exception("There's no suitable header found.")
            except StopIteration as e:
                logger.error("there's no header_dict in the csv file. exit. ex: " + e.message)
                raise

            index = 0
            header_dict = dict()
            for el in header:
                header_dict[el] = index
                index += 1

            # check date time column if global timestamp is empty
            ts_cl_lst_str = self.input_items.get("monthly_timestamp_select_column_list") or ""

            timestamp_column_index_list = list()
            for column in ts_cl_lst_str.rstrip().split("|"):
                try:
                    timestamp_column_index_list.append(header_dict[column.strip()])
                except KeyError:
                    pass

            if not timestamp_column_index_list:
                raise Exception("there's no any timestamp column in header")
            logger.info("get time stamp column index list: " + str(timestamp_column_index_list))

            length = len(header_dict)

            # continue process
            for row in reader:
                if self._canceled:
                    logger.warning("canceled by external...exit loop")
                    raise Exception("canceled by external. exist parsing current row. ")

                # count current row's MD5
                md5 = hashlib.md5(str(row) + str(key.etag)).hexdigest()
                last_scanned_non_reserved_record_set = checkpoint_item.position

                try:
                    if not isinstance(row, list):
                        raise Exception("unexpected row, is not list: " + row)
                    if len(row) <= min(timestamp_column_index_list):
                        raise Exception("row column count doesn't contains date column, row: " + str(row))

                    if md5 in last_scanned_non_reserved_record_set:
                        logger.debug("skip current row because it's already scanned, row: " + str(row))
                        continue

                    #
                    # now, process this row
                    #
                    event_time = 0
                    if global_time_str:
                        event_time = self.get_event_time(global_time_str)

                    if event_time == 0 or default_event_time_stamp < event_time:
                        event_time = default_event_time_stamp

                    for timestamp_index in timestamp_column_index_list:
                        time_str = row[timestamp_index]
                        if time_str:
                            event_time1 = self.get_event_time(time_str)
                            # get current time's timestamp and compare it with global one if any ,
                            #    if it's earlier, use it instead
                            #    this logic is added because, for example.
                            #    billing report for Aug, it will keep updated in
                            #    the early month of next month and put a global time stamp in the final version.
                            if event_time == 0 or event_time1 < event_time:
                                event_time = event_time1
                                logger.info("use current row's time: " + time_str
                                            + " , global time:" + str(global_time_str)
                                            + ", event time=" + str(event_time))
                                continue

                    if event_time == 0:
                        logger.warn("there's no available timestamp for current event, skip it. row: " + str(row))
                        continue

                    # get data, row to K-V
                    data = ""
                    for i in range(length):
                        if ',' in row[i]:
                            data += '{}="{}", '.format(header[i], row[i])
                        else:
                            data += '{}={}, '.format(header[i], row[i])

                    if data.endswith(", "):
                        data = data[:-2]

                    logger.debug("index line, time= " + str(event_time) + " row=" + str(row) + " data=" + data)
                    logger.debug('create smi.Event')
                    event = smi.Event(time=event_time,
                                      source="s3://" + self.input_items.get('bucket_name')
                                             + "/" + key.name + ":" + key.last_modified,
                                      data=data, unbroken=True, done=True)
                    logger.debug('create smi.Event done')
                    logger.debug('write_event')
                    self._ew.write_event(event)
                    logger.debug('write_event done')
                except Exception as e:
                    logger.error("there's error to handle this row: " + e.message + ", record its checksum as handled.")
                    logger.error(traceback.format_exc())
                else:
                    checkpoint_item.position[md5] = 1
                    checkpoint_item.position = checkpoint_item.position  # Trigger LMT change
                    checkpointer.save()

        except Exception as e:
            logger.error("fail to parse key: " + key.name + " get error: " + e.message)
            logger.error(traceback.format_exc())
            return
        else:
            checkpoint_item.eof_reached = True

        finally:
            checkpointer.save(True)

    def _get_normalized_name(self, file_name, file_name_prefix=''):

        logger.debug('_get_normalized_name')
        stanza_name = self.input_name
        bucket_name = self.input_items.get("bucket_name")
        checkpoint_dir = self.input_items.get("checkpoint_dir")
        start_offset = 0

        if stanza_name.startswith('aws-billing://'):
            start_offset = len('aws-billing://')

        safe_filename_prefix = "".join([c if c.isalnum() else '_' for c in stanza_name[start_offset:start_offset + 20]])
        stanza_hexdigest = hashlib.md5("{}_{}_{}_{}".format(stanza_name, bucket_name,
                                                            file_name_prefix, file_name)).hexdigest()
        file_path = os.path.join(checkpoint_dir, "cp_{}_{}_{}".format(safe_filename_prefix,
                                                                      stanza_hexdigest, os.path.splitext(file_name)[1]))

        logger.debug('_get_normalized_name done')
        return file_path

    def get_event_time(self, time_str=""):
        time_format_list = self.input_items.get("time_format_list") or None
        if not time_format_list:
            return self.convert_timestamp(time_str, time_format_list)

        format_list = time_format_list.strip().split("|")
        new_format_list = [x.strip() for x in format_list]
        return self.convert_timestamp(time_str, new_format_list)

    @staticmethod
    def convert_timestamp(time_str="", time_format_list=("%m/%d/%y %H:%M",
                                                         "%Y-%m-%d %H:%M:%S",
                                                         "%Y/%m/%d %H:%M:%S")):
        """
        :param time_str: time string in certain format, now below format is supported:
            mm/dd/yy HH:MM
            YYYY-mm-dd HH:MM:SS
        :return: UTC timestamp second float
        """
        logger.debug('convert_timestamp')
        if not time_str:
            logger.warn("get_timestamp: there's no time passed, use now instead")
            return time.time()

        for time_format in time_format_list:
            try:
                d = datetime.datetime.strptime(time_str, time_format)
                time_sec_float = time.mktime(d.timetuple())
                logger.debug("convert time string: time str=" + time_str + " format=" + time_format
                             + " to stamp=" + str(time_sec_float))
                logger.debug('convert_timestamp done')
                return time_sec_float
            except ValueError:
                # logger.debug("fail to use time stamp to convert the time string, time str="
                #              + time_str + " format=" + time_format + " error=" + e.message)
                # logger.error(traceback.format_exc())
                continue

        logger.warn("fail to convert timestamp, use now instead. time str=" + time_str
                    + " format list=" + str(time_format_list))
        return time.time()

    def get_access_key_pwd(self):
        session_key = self.definition.metadata['session_key']
        aws_account_name = self.input_items.get("aws_account") or "default"

        return self.get_access_key_pwd_real(session_key=session_key, aws_account_name=aws_account_name)

    @staticmethod
    def get_access_key_pwd_real(session_key="", aws_account_name="default"):
        logger.debug('get_access_key_pwd_real')
        if not AwsAccessKeyManager:
            raise Exception("Access Key Manager needed is not imported successfully")
        logger.debug('create AwsAccessKeyManager')
        km = AwsAccessKeyManager(KEY_NAMESPACE, KEY_OWNER, session_key)
        logger.debug('create AwsAccessKeyManager done')

        logger.info("get account name: " + aws_account_name)

        logger.debug('get_accesskey')
        acct = km.get_accesskey(name=aws_account_name)
        logger.debug('get_accesskey done')
        if not acct:
            # No recovering from this...
            logger.log(logging.FATAL, "No AWS Account is configured.")
            raise Exception("No AWS Account is configured.")
        logger.debug('get_access_key_pwd_real done')
        return acct.key_id, acct.secret_key

    def stream_events(self, inputs, ew):
        """overloaded splunklib modularinput method"""

        logger.setLevel(get_level(os.path.basename(__file__)[:-3],self.service.token, appName=APPNAME))
        logger.log(logging.INFO, "STARTED: {}".format(len(sys.argv) > 1 and sys.argv[1] or ''))
        self._ew = ew

        try:
            if os.name == 'nt':
                import win32api
                win32api.SetConsoleCtrlHandler(self._exit_handler, True)
            else:
                import signal
                signal.signal(signal.SIGTERM, self._exit_handler)
                signal.signal(signal.SIGINT, self._exit_handler)
        except Exception as e:
            logger.warn("Fail to set signal, skip this step: %s: %s", type(e).__name__, e)
            logger.error(traceback.format_exc())

        self.definition = inputs
        self.input_name, self.input_items = inputs.inputs.popitem()

        file_match_list = {"Monthly report": "\d+-aws-billing-csv-\d{4}-\d{2}\.csv",
                           "Monthly cost allocation report": "\d+-aws-cost-allocation-\d{4}-\d{2}\.csv",
                           "Detailed billing report": "\d+-aws-billing-detailed-line-items-\d{4}-\d{2}\.csv\.zip",
                           "Detailed billing report with resources and tags":
                           "\d+-aws-billing-detailed-line-items-with-resources-and-tags-\d{4}-\d{2}\.csv\.zip"}

        file_match = (self.input_items.get('report_file_match_reg') or "").strip()
        if file_match == "":
            month_reg = file_match_list.get(self.input_items.get("monthly_report_type") or "None") or ""
            detail_reg = file_match_list.get(self.input_items.get("detail_report_type") or "None") or ""

            if month_reg != "" and detail_reg != "":
                file_match = month_reg + "|" + detail_reg
            else:
                file_match = month_reg + detail_reg

        logger.info("use white list: " + file_match)

        # copy check point dir

        self.input_items["checkpoint_dir"] = inputs.metadata['checkpoint_dir']

        logger.debug('get_access_key_pwd')
        (key_id, secret_key) = self.get_access_key_pwd()
        logger.debug('get_access_key_pwd done')
        session_key = self.service.token

        try:
            logger.info("start to connecting...input name: " + str(self.input_name)
                        + " input items: " + str(self.input_items))
            logger.debug('connect_s3')
            s3_conn = connect_s3(key_id,
                                 secret_key,
                                 session_key,
                                 is_secure=internal_is_secure)
            logger.debug('connect_s3 done')
            logger.debug('s3_conn.get_bucket : %s',self.input_items['bucket_name'])
            s3_bucket = s3_conn.get_bucket(self.input_items['bucket_name'])
            logger.debug('s3_conn.get_bucket done')

            scan_start_datetime = datetime.datetime.utcnow()
            with CheckPointer(inputs.metadata['checkpoint_dir'],
                              self.input_name, self.input_items['bucket_name']) as checkpointer:
                logger.debug('loop taaws.s3util.get_keys')
                for key in taaws.s3util.get_keys(s3_bucket,
                                                 recursion_depth=int(self.input_items.get('recursion_depth') or 0),
                                                 blacklist=internal_blacklist,
                                                 whitelist=file_match,
                                                 # last_modified_after=self._get_initial_scan_datetime(),
                                                 object_keys=True,
                                                 prefix_keys=False):
                    if self._canceled:
                        logger.warning("canceled by external...exit loop")
                        break

                    try:
                        cp_item = checkpointer.get_item(key)
                        if not cp_item:
                            logger.info("cancel processing this file,"
                                        " because it's already been scanned completely and no change: " + key.name)
                            continue

                        logger.info("start to processing key: %s", key.name)

                        start_position = cp_item.position
                        logger.log(logging.INFO, "Starting: bucket: %r key: %r etag: %s retry_count: %d"
                                                 " orig_size: %d stream_position: %d"
                                                 " storage_class: %s last_modified: %s"
                                                 " datetime_delta: %s",
                                   s3_bucket.name, key.name, key.etag, cp_item.attempt_count,
                                   key.size, len(start_position),
                                   key.storage_class, key.last_modified,
                                   datetime.datetime.utcnow() - boto.utils.parse_ts(key.last_modified))

                        if key.name.endswith(".csv"):
                            logger.info("the file is csv monthly report. ")
                            self._stream_monthly_billing_item(key, cp_item, checkpointer)
                        elif key.name.endswith(".csv.zip"):
                            logger.info("the file is csv.zip detail report. ")
                            self._stream_detail_billing_item(key, cp_item, checkpointer)

                    except CheckPointer.MaxTrackedItemsReached as e:
                        logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
                        break
                    except Exception as e:
                        if isinstance(e, boto.exception.S3ResponseError):
                            msg = "{} {} - {} - {}".format(e.status, e.reason, e.error_code, e.error_message)
                        else:
                            msg = e
                        logger.log(logging.ERROR, "Incomplete: bucket: %r key: %r etag: %s attempt_number: %d"
                                                  " orig_size: %d stream_position: %d"
                                                  " Exception: %s: %s",
                                   s3_bucket.name, key.name, key.etag, cp_item.attempt_count,
                                   key.size, len(cp_item.position),
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
                                          " orig_size: %d bytes_streamed: %d",
                                   status, s3_bucket.name, key.name, key.etag, cp_item.attempt_count,
                                   key.size, len(cp_item.position))

                else:
                    logger.log(logging.INFO, "Completed full scan: %s",
                               datetime.datetime.utcnow() - scan_start_datetime)
                    checkpointer.last_completed_scan_datetime = scan_start_datetime
                logger.debug('loop taaws.s3util.get_keys done')

        except boto.exception.S3ResponseError as e:
                msg = "{} {} - {} - {}".format(e.status, e.reason, e.error_code, e.error_message)
                logger.log(logging.FATAL, "Outer catchall: %s: %s", type(e).__name__, msg)
                logger.error(traceback.format_exc())
                raise
        except Exception as e:
            logger.log(logging.FATAL, "Outer catchall: %s: %s", type(e).__name__, e)
            logger.error(traceback.format_exc())
            raise


class FileHolder(object):
    """Tracks file hold and delete them while existing. """

    def __init__(self, file_name=None):

        logger.debug('create FileHolder')
        self.file_list = set()
        self.folder_list = set()
        self.file_open_list = set()
        if isinstance(file_name, basestring):
            self.file_list.add(file_name)
        elif isinstance(file_name, file):
            self.file_open_list.add(file_name)
        logger.debug('create FileHolder done')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for open_file in self.file_open_list:
            try:
                logger.debug("try to remove file: " + str(open_file))
                open_file.close()
            except Exception as _e:
                logger.error("fail to delete file: file name=" + str(open_file) + " error=" + _e.message
                             + str(exc_type) + str(exc_val) + str(exc_tb))

        for file_name in self.file_list:
            try:
                logger.debug("try to remove file: " + file_name)
                os.remove(file_name)
            except Exception as _e:
                logger.error("fail to delete file: file name=" + file_name + " error=" + _e.message
                             + str(exc_type) + str(exc_val) + str(exc_tb))

        for folder_name in self.folder_list:
            try:
                logger.debug("try to remove folder: " + folder_name)
                os.rmdir(folder_name)
            except Exception as _e:
                logger.error("fail to delete file: folder name=" + folder_name + " error=" + _e.message
                             + str(exc_type) + str(exc_val) + str(exc_tb))

    def __len__(self):
        return len(self.file_list)

    def add_monitor(self, file_name):
        if isinstance(file_name, basestring):
            self.file_list.add(file_name)
        elif isinstance(file_name, file):
            self.file_open_list.add(file_name)
        return self

    def remove_monitor(self, file_name):
        self.file_list.remove(file_name)
        return self

    def add_folder_monitor(self, folder_name):
        self.folder_list.add(folder_name)

    def file_list(self):
        return self.file_list

if __name__ == "__main__":

    exitcode = MyScript().run(sys.argv)

    sys.exit(exitcode)
