"""
Modular Input for AWS CloudWatch
"""

import sys
import os
import time
import shutil
import threading
import Queue
import collections
import datetime
import calendar
import json
import errno
import traceback
from taaws.aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER

from splunklib import modularinput as smi
from taaws.aws_accesskeys import AwsAccessKeyManager

import boto.ec2
import boto.ec2.cloudwatch
import boto.exception

import logging
from taaws.log import setup_logger

logger = setup_logger(os.path.basename(__file__), level=logging.ERROR)
from taaws.log_settings import get_level

import taaws.s3util
import regex_dimensions


class Checkpointer(object):
    """Tracks state of current MetricQueryState being processed."""

    DATA_EMPTY = {
        'version': '1.0.0',
        'items': {},
    }
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    OUTDATED_TIME = datetime.timedelta(days=7)

    def __init__(self, checkpoint_dir, wait_queue=None):
            """
            @param checkpoint_dir: base directory for checkpoint data
            @param wait_queue: wait_queue of MQS
            @return:
            """
            self.filepath=checkpoint_dir+os.path.sep+'checkpointer.json'
            if wait_queue is None:
                try:
                    logger.log(logging.DEBUG, 'open file for json load : %s ',self.filepath)
                    with open(self.filepath, mode='r') as f:
                        self._data = json.load(f)
                    logger.log(logging.DEBUG, 'close file: %s ',self.filepath)
                    self.prune()
                except ValueError as e:
                    self._data = self.DATA_EMPTY
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        self._data = self.DATA_EMPTY
                    else:
                        logger.log(logging.ERROR, "%s: %s", type(e).__name__, e.message)
                        raise
                logger.log(logging.INFO, "Checkpointer recovered from %s", checkpoint_dir)
                return
            else:
                self._data = self.DATA_EMPTY
                for mqs in wait_queue:
                    self.set_time_by_mqs(mqs)
                self._last_save_time = 0
                logger.log(logging.INFO, "Checkpointer initialized from wait queue")


    def build_key(self, stanza_name, region, metric_name, dimension):
        try:
            return json.dumps([stanza_name, region, metric_name, json.dumps(dimension, sort_keys=True)])
        except Exception as e:
            logger.log(logging.ERROR, "build_key() failed in %s. %s: %s", stanza_name, type(e).__name__, e.message)

    def delete_item(self, stanza_name, region, metric_name, dimension):
        try:
            del self._data['items'][self.build_key(stanza_name, region, metric_name, dimension)]
        except NameError as e:
            logger.log(logging.ERROR, "Delete item failed in %s. %s: %s", stanza_name, type(e).__name__, e.message)
        except TypeError as e:
            logger.log(logging.ERROR, "Delete item failed in %s. %s: %s", stanza_name, type(e).__name__, e.message)

    def get_time(self, stanza_name, region, metric_name, dimension, default=None):
        try:
            time_str = self._data['items'].get(self.build_key(stanza_name, region, metric_name, dimension))
            if time_str is None:
                return default
            return datetime.datetime.strptime(time_str,self.TIME_FORMAT)
        except ValueError as e:
            logger.log(logging.ERROR, "get time failed in %s. %s: %s", stanza_name, type(e).__name__, e.message)
            return default

    def get_time_by_mqs(self, mqs, default=None):
        return self.get_time(mqs.input_name, mqs.aws_region, mqs.metric_name, mqs.metric_dimensions, default)

    def set_time(self, stanza_name, region, metric_name, dimension, last_query_endtime):
        try:
            time_str = last_query_endtime.strftime(self.TIME_FORMAT)
            self._data['items'][self.build_key(stanza_name, region, metric_name, dimension)] = time_str
        except Exception as e:
            logger.log(logging.ERROR, "set_time failed in %s. %s: %s", stanza_name, type(e).__name__, e.message)


    def set_time_by_mqs(self, mqs):
        self.set_time(mqs.input_name, mqs.aws_region, mqs.metric_name, mqs.metric_dimensions, mqs._previous_query_endtime)


    def save(self, force=False):
        """Saves current checkpointer state to checkpoint_dir

        @param force:
        @return:
        """

        stime = time.time()
        if not force and stime - self._last_save_time < 15:
            return


        tfp = "%s.tmp" % self.filepath

        try:
            logger.log(logging.DEBUG, 'open file for json dump: %s',tfp)
            with open(tfp, 'w') as f:
                json.dump(self._data, f, indent=2)
            logger.log(logging.DEBUG, 'move file')
            shutil.move(tfp, self.filepath)
            self._last_save_time = stime
        except Exception as e:
            logger.log(logging.ERROR, "%s", type(e).__name__, e)
            raise

        logger.log(logging.INFO, "Checkpointer saved %d items in %f seconds to: %s", len(self._data['items']),
                   time.time() - stime, self.filepath)


    def prune(self):
        """
        @return: count pruned items.
        """

        pruned = 0
        prune_time = datetime.datetime.now()


        orig_size = len(self._data['items'])
        for key_name, last_query_time_str in self._data['items'].items():
            if datetime.datetime.strptime(last_query_time_str, self.TIME_FORMAT) + self.OUTDATED_TIME < prune_time:
                del self._data['items'][key_name]
                pruned += 1

        logger.log(logging.INFO, "Checkpointer pruned %d of %d items leaving %d items.",
                   pruned, orig_size, len(self._data['items']))

        return pruned



class MetricQueryState(object):
    """State information for metric being monitored."""

    StatisticQueryWindow = collections.namedtuple('StatisticQueryWindow', ['start_time', 'end_time'])

    # Currently this MUST be 1 hour or less. Method snap_to_interval would break.
    MAX_QUERY_WINDOW_DELTA = datetime.timedelta(hours=12)
    # Minimum is always self.period in case detailed granularity is enabled (60 seconds)
    # Otherwise +4 minutes to have enough time for SNS, SQS, etc. to become available.
    MAX_STAT_GRACE_PERIOD_SECS = 240
    MAX_PERIOD_SECS = 21600

    def __init__(self, input_name, aws_region, metric_namespace, statistics, metric_name, index, host, key_id,
                 secret_key, period=300, polling_interval=None, metric_dimensions=None, sourcetype='aws:cloudwatch',
                 session_key=None):
        """Initialize metric query state from input stanza.

        """
        self.input_name = input_name
        self.sourcetype = sourcetype
        self.aws_region = aws_region
        self.metric_namespace = metric_namespace
        self.metric_name = metric_name
        self.index = index
        self.host = host
        self.metric_dimensions = metric_dimensions
        self.key_id = key_id
        self.secret_key = secret_key
        self.session_key = session_key
        try:
            # Can't be empty
            self.statistics = json.loads(statistics)
        except Exception as e:
            raise ValueError("statistics: {}".format(e))

        if not set(self.statistics).issubset(('Average', 'Sum', 'SampleCount', 'Maximum', 'Minimum')):
            raise ValueError("statistics must be a subset of Average, Sum, SampleCount, Maximum, Minimum")

        self.period = period
        self.period_minutes = self.period / 60
        self.polling_interval = max(self.period, polling_interval or self.period)
        self.polling_interval_minutes = self.polling_interval / 60

        if self.period < 60 or self.period > self.MAX_PERIOD_SECS:
            raise ValueError("period must be between 60 and 21600")
        if self.polling_interval < 60 or self.polling_interval > self.MAX_PERIOD_SECS:
            raise ValueError("polling_interval must be between 60 and 21600")

        if self.period % 60 != 0:
            raise ValueError("period must be a multiple of 60")
        if self.polling_interval % self.period != 0:
            raise ValueError("polling_interval must be a multiple of period")

        self._previous_query_endtime = 0
        self._next_statistic_query_time = 0

        # snap to most recent previous period
        d = datetime.datetime.utcnow() - datetime.timedelta(seconds=min(self.period, self.MAX_STAT_GRACE_PERIOD_SECS))
        # snap to most recent previous polling interval for that period
        self.previous_query_endtime = self.snap_to_period(d, self.polling_interval_minutes)

    @property
    def previous_query_endtime(self):
        return self._previous_query_endtime

    @previous_query_endtime.setter
    def previous_query_endtime(self, value):

        self._previous_query_endtime = value
        # next min_interval + stat latency grace period
        delta = datetime.timedelta(seconds=self.polling_interval + min(self.period, self.MAX_STAT_GRACE_PERIOD_SECS))
        self._next_statistic_query_time = self.previous_query_endtime + delta

    @property
    def next_statistic_query_time(self):
        return self._next_statistic_query_time

    def get_statistic_query_window(self):

        start_time = self.previous_query_endtime

        # current_interval - original stat latency grace period
        d = datetime.datetime.utcnow() - datetime.timedelta(seconds=min(self.period, self.MAX_STAT_GRACE_PERIOD_SECS))
        end_time = datetime.datetime(d.year, d.month, d.day, d.hour,
                                     d.minute / self.period_minutes * self.period_minutes)

        # ensure the query window is under an hour to support the maximum # of datapoints in a query.
        if end_time - start_time > self.MAX_QUERY_WINDOW_DELTA:
            max_delta = int(self.MAX_QUERY_WINDOW_DELTA.total_seconds())
            end_time = start_time + datetime.timedelta(seconds=max_delta / self.period * self.period)

        return MetricQueryState.StatisticQueryWindow(start_time, end_time)

    @staticmethod
    def snap_to_period(dts, period_minutes):

        return datetime.datetime(dts.year, dts.month, dts.day, dts.hour, dts.minute / period_minutes * period_minutes)


class OutputWorker(threading.Thread):
    """Class for coalescing metric output into events."""

    METRIC_DATA_HEADER = "metric_name\tmetric_dimensions\tperiod\tSampleCount\tUnit\tAverage\tMinimum\tMaximum\tSum"

    def __init__(self, event_writer, output_queue, wait_queue, checkpointer,
                 multikv_chunksize=20, flush_interval_seconds=30,
                 autoflush_idle_timeout=2, autoflush_threshold_count=1000):
        """

        @param event_writer:
        @param output_queue:
        @param wait_queue:
        @param multikv_chunksize: Maximum number of metric statistics that can be combined into a single event.
        @param flush_interval_seconds: Maximum flush delay for buffered statistics.
        @param autoflush_idle_timeout: Autoflush if no new statistics are added to buffer.
        @param autoflush_threshold_count: Autoflush if buffered statistic count is reached.
        @return:
        """

        super(OutputWorker, self).__init__(name="OutputWorkerThread")

        self._ew = event_writer
        self._output_queue = output_queue
        self._wait_queue = wait_queue
        self._checkpointer = checkpointer

        self._multikv_chunksize = multikv_chunksize
        self._flush_interval_seconds = flush_interval_seconds
        self._autoflush_idle_timeout = autoflush_idle_timeout
        self._autoflush_threshold_count = autoflush_threshold_count

        self._buffer = {}
        self._mqs_pending_flush = []

        self._canceled = False
        self.daemon = True

    def cancel(self):
        self._canceled = True

    def run(self):

        while not self._canceled:

            try:
                self._process_output_queue()
            except Exception as e:
                logger.log(logging.ERROR, "Exception from OutputWorker - Traceback:\n"+traceback.format_exc())
            finally:
                self._flush()

        self._checkpointer.save(force=True)


    def _process_output_queue(self):
        """Processes MetricQueryState objects on ouput queue and moves them back to the wait queue."""

        def should_flush():

            if stat_count >= self._autoflush_threshold_count:
                return True
            if time.time() > interval_start + self._flush_interval_seconds:
                return True
            if idle_start and time.time() - idle_start > self._autoflush_idle_timeout:
                return True
            if self._canceled:
                return True
            return False

        stat_count = 0
        interval_start = time.time()
        idle_start = None

        while True:
            while True:

                try:
                    real_end_time = None # Get the latest timestamp from return stats
                    mqs, qw_endtime, stats = self._output_queue.popleft()

                    idle_start = time.time()

                    if stats:

                        for stat in stats:
                            buffer_key = self._build_buffer_key(self._get_splunk_utctimestamp(stat['Timestamp']),
                                                                mqs.index,
                                                                mqs.host,
                                                                self._build_splunk_source(mqs.aws_region,
                                                                                          mqs.metric_namespace))

                            buf = self._get_statistic_buffer(buffer_key)
                            buf.append((mqs, stat))
                            stat_count += 1
                            utime = self._get_splunk_utctimestamp(stat['Timestamp'])
                            if real_end_time == None or real_end_time < utime:
                                real_end_time = utime

                except IndexError:
                    # ensure flush interval is started after first stat received.
                    if len(self._mqs_pending_flush) == 0:
                        interval_start = time.time()
                    if not idle_start:
                        idle_start = time.time()
                    break

                else:
                    # self._mqs_pending_flush.append((mqs, qw_endtime))
                    mqs.previous_query_endtime = datetime.datetime.utcfromtimestamp(real_end_time + mqs.period)
                    self._mqs_pending_flush.append((mqs, mqs.previous_query_endtime))
                    self._checkpointer.set_time_by_mqs(mqs)

                    if should_flush():
                        break

            if should_flush():
                break

            # sleep 1 second before retrying the empty deque
            time.sleep(1)

    def _flush(self):

        # nothing to do?
        if not len(self._buffer) and not len(self._mqs_pending_flush):
            return

        start_time = time.time()

        total_stats = 0
        total_events = 0

        try:
            try:
                for buffer_key, buffer_data in sorted(self._buffer.items()):
                    if not buffer_data:
                        # this should never happen. right?
                        logger.log(logging.ERROR, "empty data for buffer_key: {}".format(buffer_key))
                        continue

                    mqs, stat = buffer_data[0]
                    splunk_time = self._get_splunk_utctimestamp(stat.get('Timestamp'))
                    splunk_index = mqs.index
                    splunk_host = mqs.host
                    splunk_source = self._build_splunk_source(mqs.aws_region, mqs.metric_namespace)

                    total_stats += len(buffer_data)
                    for buffer_data_chunk in tuple(self._chunkify(buffer_data, self._multikv_chunksize)):
                        event_data = "{}\n{}".format(self.METRIC_DATA_HEADER, '\n'.join(
                            [self._build_multikv_line(mqs, stat) for mqs, stat in buffer_data_chunk]))

                        logger.log(logging.DEBUG, "PerfLog = EventStart")
                        event = smi.Event(time=splunk_time, index=splunk_index, host=splunk_host, source=splunk_source,
                                          data=event_data, sourcetype=mqs.sourcetype, unbroken=False)
                        logger.log(logging.DEBUG, "PerfLog = EventEnd, EventNum = {} ".format(len(buffer_data_chunk)))
                        total_events += 1
                        logger.log(logging.DEBUG, "write_event")
                        self._ew.write_event(event)
                        logger.log(logging.DEBUG, "write_event done")

                # checkpoint
                for mqs, qw_endtime in self._mqs_pending_flush:
                    if qw_endtime:
                        mqs.previous_query_endtime = qw_endtime

            except:
                raise

            finally:
                total_metrics = len(self._mqs_pending_flush)
                # deque.extend() isn't documented as thread-safe
                # see: http://bugs.python.org/issue15329
                # self._wait_queue.extend([mqs for mqs, qw_endtime in self._mqs_pending_flush])
                for mqs, qw_endtime in self._mqs_pending_flush:
                    self._wait_queue.append(mqs)
                self._buffer.clear()
                del self._mqs_pending_flush[:]
                self._checkpointer.save()
        except Exception as e:
            # flush failed
            logger.log(logging.ERROR, "%s", e)
        else:

            logger.log(logging.INFO, "Flush/Checkpoint completed: total_events: {}"
                                     " total_metrics: {} total_stats: {}"
                                     " in {:.3f}s".format(total_events,
                                                          total_metrics, total_stats,
                                                          time.time() - start_time))

    def _get_statistic_buffer(self, buffer_key):

        try:
            buf = self._buffer[buffer_key]
        except KeyError:
            buf = []
            self._buffer[buffer_key] = buf
        return buf

    @staticmethod
    def _build_multikv_line(mqs, stat):
        """

        @param mqs: MetricQueryState object
        @param stat: AWS Statistic
        @return:
        """
        return "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(mqs.metric_name,
                                                           OutputWorker._build_multikv_dimensions(mqs),
                                                           mqs.period,
                                                           stat.get('SampleCount', ''),
                                                           stat.get('Unit'),
                                                           stat.get('Average', ''),
                                                           stat.get('Minimum', ''),
                                                           stat.get('Maximum', ''),
                                                           stat.get('Sum', '')
        )

    @staticmethod
    def _build_multikv_dimensions(mqs):
        """

        @param mqs: MetricQueryState object
        @return:
        """
        if mqs.metric_dimensions:
            return ','.join(
                ['{}={}'.format(json.dumps(k), json.dumps(v)) for k, v in sorted(mqs.metric_dimensions.items())])
        else:
            return ''


    @staticmethod
    def _get_splunk_utctimestamp(timestamp):
        """

        @param timestamp:
        @return:
        """
        return calendar.timegm(timestamp.timetuple())

    @staticmethod
    def _build_splunk_source(aws_region, metric_namespace):

        return "{}:{}".format(aws_region, metric_namespace)

    @staticmethod
    def _build_buffer_key(splunk_timestamp, splunk_index, splunk_host, splunk_source):

        return "{}\t{}\t{}\t{}".format(splunk_timestamp, splunk_index or '', splunk_host or '', splunk_source)

    @staticmethod
    def _chunkify(list_object, chunk_size):

        for i in xrange(0, len(list_object), chunk_size):
            yield list_object[i:i + chunk_size]


class QueryWorker(threading.Thread):
    """Class for performing AWS get_metric_statistic queries."""

    _thread_id = 0
    _thread_id_lock = threading.Lock()

    @classmethod
    def _create_thread_name(cls):
        cls._thread_id_lock.acquire()
        try:
            cls._thread_id += 1
            return "{}Thread-{}".format(cls.__name__, cls._thread_id)
        finally:
            cls._thread_id_lock.release()

    def __init__(self, event_writer, work_queue, output_queue, wait_queue):

        super(QueryWorker, self).__init__(name=self._create_thread_name())

        self._ew = event_writer
        self._work_queue = work_queue
        self._output_queue = output_queue
        self._wait_queue = wait_queue

        self._canceled = False
        self.daemon = True

    def cancel(self):
        self._canceled = True

    def run(self):

        while not self._canceled:

            try:
                self._main_work_loop()
            except Exception as e:
                logger.log(logging.ERROR, "Exception from OutputWorker - Traceback:\n"+traceback.format_exc())

    def _main_work_loop(self):

        # aws connection cache
        connections = {}

        query_stats = {'processed': 0, 'failures': 0, 'total_stats': 0}
        block = True
        start_time = None
        while not self._canceled:

            try:
                mqs = self._work_queue.get(block=block, timeout=1)
            except Queue.Empty:
                block = True
                if start_time:
                    logger.log(logging.INFO, "Queried {} metrics with {} failures totaling {} statistics"
                                             " in {:.3f}s before stalling.".format(
                        query_stats['processed'], query_stats['failures'], query_stats['total_stats'],
                        time.time() - start_time))
                    start_time = None

            else:
                # successful queries are moved to the output_queue
                # failures are moved back to the wait_queue
                try:
                    if not start_time:
                        block = False
                        query_stats['processed'] = 0
                        query_stats['failures'] = 0
                        query_stats['total_stats'] = 0
                        start_time = time.time()

                    query_stats['processed'] += 1

                    logger.log(logging.DEBUG, "get cloudwatch connection")
                    cw_conn = connections.get((mqs.key_id, mqs.aws_region))
                    if not cw_conn:
                        logger.log(logging.DEBUG, "no existing connection. connecting cloudwatch")
                        cw_conn = taaws.s3util.connect_cloudwatch(mqs.aws_region, mqs.key_id, mqs.secret_key,
                                                                  mqs.session_key)
                        logger.log(logging.DEBUG, "Connect to cloulwatch successful")
                        connections[(mqs.key_id, mqs.aws_region)] = cw_conn

                    query_window = mqs.get_statistic_query_window()

                    logger.log(logging.DEBUG, "PerfLog = QueryStart")
                    stats = cw_conn.get_metric_statistics(mqs.period,
                                                          start_time=query_window.start_time,
                                                          end_time=query_window.end_time,
                                                          namespace=mqs.metric_namespace,
                                                          metric_name=mqs.metric_name,
                                                          dimensions=mqs.metric_dimensions,
                                                          statistics=mqs.statistics)
                    if len(stats):
                        query_stats['total_stats'] += len(stats)

                    logger.log(logging.DEBUG,
                               "PerfLog = QueryEnd, Query Result, [{}]({}) start : {} end : {}, StatsNum = {} ".format(
                                   mqs.input_name, mqs.metric_name, str(query_window.start_time),
                                   str(query_window.end_time), len(stats)))

                except Exception as e:
                    self._wait_queue.append(mqs)
                    query_stats['failures'] += 1

                    logger.log(logging.ERROR, "[{}]({}) {}".format(
                        mqs.input_name, mqs.metric_name, e))
                    logger.log(logging.DEBUG, "Error from _main_work_loop - Traceback:\n"+traceback.format_exc())

                else:
                    if len(stats) == 0:
                        self._wait_queue.append(mqs)
                        query_stats['failures'] += 1

                        logger.log(logging.DEBUG, "No Query Result, put back to wait queue [{}]({})".format(
                            mqs.input_name, mqs.metric_name))
                    else:
                        self._output_queue.append((mqs, query_window.end_time, stats))


def validate_input_items(input_items):
    # check metric_names
    metric_names = input_items.get('metric_names')
    try:
        metric_names = json.loads(metric_names)
        if not isinstance(metric_names, list):
            raise Exception()
    except Exception as e:
        raise ValueError("metric_names: %s is not valid JSON list string" % metric_names)

    # check dimensions
    dimensions = input_items.get('metric_dimensions')
    try:
        dimensions = json.loads(dimensions)
    except Exception as e:
        raise ValueError("dimensions: %s is not valid JSON string" % dimensions)

    if not isinstance(dimensions, list):
        dimensions = [dimensions]

    for re_value_dict in dimensions:
        try:
            regex_dimensions.DimensionExactMatcher(re_value_dict)
        except Exception:
            raise ValueError("dimension: %s has invalid format. See README for correct format." % json.dumps(re_value_dict))

    # check statistics
    statistics = input_items.get('statistics')
    try:
        statistics = json.loads(statistics)
        if not isinstance(statistics, list):
            raise Exception()
    except Exception as e:
        raise ValueError("statistics: %s is not valid JSON list string" % statistics)

    STATS = ['Average', 'Sum', 'SampleCount', 'Maximum', 'Minimum']
    for stat in statistics:
        if not stat in STATS:
            raise ValueError("statistics: %s contains invalid stat: %s" % (json.dumps(statistics), stat))

    # check period
    period = input_items.get('period')
    try:
        period = int(period)
    except Exception:
        raise ValueError("Invalid granularity: %s. Granularity must be integer." % period)
    if period < 60 or period > MetricQueryState.MAX_PERIOD_SECS or period % 60 != 0:
        raise ValueError("Invalid granularity: %s. Granularity must be multiple of 60 and between 60 and %s" %
                         (period, MetricQueryState.MAX_PERIOD_SECS))

    # check polling_interval
    polling_interval = input_items.get('polling_interval')
    try:
        polling_interval = int(polling_interval)
    except Exception:
        raise ValueError("Invalid polling_interval: %s. Polling interval must be integer." % period)
    if polling_interval < 60 or polling_interval > MetricQueryState.MAX_PERIOD_SECS or polling_interval % period != 0:
        raise ValueError(
            "Invalid polling_interval: %s. Polling interval must be multiple of granularity and between 60 and %s" %
            (period, MetricQueryState.MAX_PERIOD_SECS))


class MyScript(smi.Script):
    # query threads
    MAX_QUERY_WORKERS = 4

    def __init__(self):
        super(MyScript, self).__init__()
        self._canceled = False
        self._ew = None

    def get_scheme(self):
        """overloaded splunklib modularinput method"""

        scheme = smi.Scheme("AWS CloudWatch")
        scheme.description = ("Collect and index metrics produced by AWS CloudWatch."
        )
        scheme.use_external_validation = True
        scheme.streaming_mode_xml = True
        scheme.use_single_instance = True
        # defaults != documented scheme defaults, so I'm being explicit.
        scheme.add_argument(smi.Argument("name", title="Name",
                                         description="Choose an ID or nickname for this configuration",
                                         required_on_create=True))
        scheme.add_argument(smi.Argument("aws_account", title="AWS Account",
                                         description="AWS account",
                                         required_on_create=False, required_on_edit=False))
        scheme.add_argument(smi.Argument("aws_region", title="AWS CloudWatch Region",
                                         description=("Name of the AWS region in which the metrics are located."
                                                      " Regions should be entered as"
                                                      " e.g., us-east-1, us-west-2, eu-west-1, ap-southeast-1, etc."),
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("metric_namespace", title="AWS CloudWatch Metric Namespace",
                                         description="Name of the AWS Metric Namespace.",
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("metric_names", title="AWS CloudWatch Metric Names",
                                         description="Names of the AWS Metrics.",
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("metric_dimensions", title="AWS CloudWatch Dimensions",
                                         description="JSON encoded",
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("statistics", title="AWS CloudWatch Metric Statistics Being Requested",
                                         description=("List of valid values (JSON encoded):"
                                                      " Average, Sum, SampleCount, Maximum, Minimum"),
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("period", title="AWS CloudWatch Metric Granularity",
                                         description=("The period in seconds. Must be at least 60 seconds, and a"
                                                      " multiple of 60. The default value is 300. The maximum is 21600"),
                                         required_on_create=True, required_on_edit=True))
        scheme.add_argument(smi.Argument("polling_interval", title="AWS CloudWatch Minimum Polling Interval",
                                         description=("The period in seconds. Must be a multiple of period."
                                                      " The default is the value of period. The maximum is 21600."),
                                         required_on_create=True, required_on_edit=True))

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

        input_items = definition.parameters

        session_key = definition.metadata['session_key']
        aws_account_name = input_items.get("aws_account") or "default"
        (key_id, secret_key) = self.get_access_key_pwd_real(session_key=session_key, aws_account_name=aws_account_name)

        validate_input_items(input_items)

        # TODO: Verify metric list exists?
        # TODO: Verify regular expressions

    @staticmethod
    def _expand_stanza_metrics(input_name, input_items, key_id, secret_key, session_key):
        """Loads/Creates MetricQueryState objects for the referenced metrics in input definition.

        @param input_name:
        @param input_items:
        @param key_id:
        @param secret_key:
        @param session_key
        @return:
        """

        logger.log(logging.DEBUG, "_expand_stanza_metrics")
        try:
            validate_input_items(input_items)
        except ValueError as e:
            logger.log(logging.ERROR, e.message)
            return []

        try:
            metric_names = json.loads(input_items.get('metric_names'))
        except Exception as e:
            raise ValueError("metric_names: {}".format(e))

        aws_region = input_items['aws_region']  # region is selected from list
        metric_namespace = input_items['metric_namespace']  # metric_namespace is selected from list
        statistics = input_items['statistics']  # must be filled when creating data input
        try:
            period = int(input_items.get('period'))
        except ValueError:
            period = 300

        try:
            polling_interval = int(input_items.get('polling_interval'))
        except ValueError:
            polling_interval = period
        index = input_items.get('index', '')
        host = input_items.get('host', '')
        sourcetype = input_items.get('sourcetype', 'aws:cloudwatch')
        dim_re_list = json.loads(input_items['metric_dimensions'])  # to be validated when creating data input

        logger.log(logging.DEBUG, "taaws.s3util.connect_cloudwatch : aws_region = %s",str(aws_region))
        cw_conn = taaws.s3util.connect_cloudwatch(aws_region, key_id, secret_key, session_key)
        logger.log(logging.DEBUG, "taaws.s3util.connect_cloudwatch done")
        try:
            logger.log(logging.DEBUG, "regex_dimensions.get_dimensions")
            dimensions = regex_dimensions.get_dimensions(cw_conn, metric_namespace, dim_re_list)
            logger.log(logging.DEBUG, "regex_dimensions.get_dimensions done")
        except Exception as e:
            logger.log(logging.ERROR, e.message)
            return []

        mqs_list = []
        for dim in dimensions:
            for n in metric_names:
                mqs_list.append(
                    MetricQueryState(
                        input_name=input_name,
                        aws_region=aws_region,
                        metric_namespace=metric_namespace,
                        statistics=statistics,
                        metric_name=n,
                        index=index,
                        host=host,
                        key_id=key_id,
                        secret_key=secret_key,
                        period=period,
                        polling_interval=polling_interval,
                        metric_dimensions=dim,
                        sourcetype=sourcetype,
                        session_key=session_key)
                )
        logger.log(logging.DEBUG, "_expand_stanza_metrics done")
        return mqs_list

    def _exit_handler(self, signum, frame=None):
        self._canceled = True
        logger.log(logging.INFO, "cancellation received.")

        if os.name == 'nt':
            return True

    def stream_events(self, inputs, ew):
        """overloaded splunklib modularinput method"""
        logger.setLevel(get_level(os.path.basename(__file__)[:-3],self.service.token, appName=APPNAME))
        logger.log(logging.INFO, "STARTED: {}".format(len(sys.argv) > 1 and sys.argv[1] or ''))
        logger.log(logging.DEBUG, "Start streaming.")

        # only the OutputWorker thread calls EventWriter.write_event()
        # all other threads only call .log()
        self._ew = ew

        if os.name == 'nt':
            import win32api

            win32api.SetConsoleCtrlHandler(self._exit_handler, True)
        else:
            import signal

            signal.signal(signal.SIGTERM, self._exit_handler)
            signal.signal(signal.SIGINT, self._exit_handler)

        # MetricQueryState objects are maintained on 1 of 3 queues:
        # wait_queue: waiting to be scheduled; moved to work_queue when ready to poll
        # work_queue: scheduled; picked up by a QueryWorker thread.
        # output_queue: output buffer; coalesced/streamed by the OutputWorker thread.
        # A MetricQueryState object can never be referenced in more than one queue at a time.
        wait_queue = collections.deque()
        work_queue = Queue.Queue()
        output_queue = collections.deque()
        checkpointer = Checkpointer(inputs.metadata['checkpoint_dir'])

        for input_name, input_items in inputs.inputs.iteritems():
            session_key = self.service.token
            aws_account_name = input_items.get("aws_account") or "default"
            (key_id, secret_key) = self.get_access_key_pwd_real(session_key=session_key,
                                                                aws_account_name=aws_account_name)
            logger.log(logging.DEBUG, "get account name: %s",aws_account_name)

            try:
                mqs_list = self._expand_stanza_metrics(input_name, input_items, key_id, secret_key, session_key)
                for mqs in mqs_list:
                    cp_time = checkpointer.get_time_by_mqs(mqs)
                    if cp_time:
                        mqs.previous_query_endtime = cp_time
                wait_queue.extend(mqs_list)
                logger.log(logging.INFO, "input_name: [{}] metric_count: {}".format(input_name, len(mqs_list)))

            except Exception as e:
                logger.log(logging.ERROR, "failed expanding metrics for input_name: {}".format(input_name))
                raise

        logger.log(logging.INFO, "total_metric_count: {}".format(len(wait_queue)))
        checkpointer = Checkpointer(inputs.metadata['checkpoint_dir'], wait_queue)
        checkpointer.save()
        query_workers = []
        for i in xrange(0, self.MAX_QUERY_WORKERS):
            w = QueryWorker(ew, work_queue, output_queue, wait_queue)
            w.start()
            logger.log(logging.DEBUG, "QueryWorker started")
            query_workers.append(w)

        output_worker = OutputWorker(ew, output_queue, wait_queue, checkpointer)
        output_worker.start()
        logger.log(logging.DEBUG, "OutputWorker started")

        try:
            while not self._canceled:

                if self._canceled:
                    break

                # If work is still pending, don't waste time scheduling more yet.
                wq_size = work_queue.qsize()
                if wq_size:
                    rescan_delay = min(1, max(5, wq_size / 100))
                    logger.log(logging.INFO, "work_queue is still being processed."
                                             " Current work_queue size: {}"
                                             " Delaying wait_queue rescan for {} seconds.".format(wq_size,
                                                                                                  rescan_delay))
                    self._do_delay(rescan_delay)
                    continue

                # scan for more work.
                start_time = time.time()
                stats = {'queued': 0, 'deferred': 0}
                try:
                    current_timestamp = datetime.datetime.utcnow()
                    for i in xrange(0, len(wait_queue)):
                        mqs = wait_queue.popleft()

                        if current_timestamp > mqs.next_statistic_query_time:
                            work_queue.put(mqs)
                            stats['queued'] += 1
                        else:
                            wait_queue.append(mqs)
                            stats['deferred'] += 1
                except IndexError:
                    pass

                logger.log(logging.INFO, "query work queued = {}, deferred = {} , scan_time = {:.3f}s".format(
                    stats['queued'], stats['deferred'], time.time() - start_time))

                # delay reprocessing until next minimum possible period window + 1
                delay = 60 - datetime.datetime.now().timetuple().tm_sec + 1
                self._do_delay(delay)

        except Exception as e:
            logger.log(logging.FATAL, "Outer catchall - Traceback:\n"+traceback.format_exc())
            raise

        for w in query_workers:
            w.cancel()
        output_worker.cancel()
        output_worker.join(timeout=2)

        # TODO: test/log output_worker isAlive() -- future

    def _do_delay(self, seconds):
        for i in xrange(0, seconds):
            time.sleep(1)
            if self._canceled:
                break


if __name__ == "__main__":
    exitcode = MyScript().run(sys.argv)
    sys.exit(exitcode)
