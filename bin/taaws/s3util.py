"""

"""

import re
import os

import boto.ec2
import boto.ec2.cloudwatch
import boto.s3.connection
import boto.exception

import logging
from log import setup_logger
logger = setup_logger(os.path.basename(__file__), level=logging.DEBUG)

from proxy_conf import ProxyManager 

def connect_s3(key_id,secret_key,session_key,host="s3.amazonaws.com",is_secure=True):
    logger.debug("Connect to s3")
    pm = ProxyManager(session_key)
    proxy = pm.get_proxy()
    if proxy is None or not proxy.get_enable():
        s3_conn = boto.s3.connection.S3Connection(aws_access_key_id=key_id,
                                                  aws_secret_access_key=secret_key,
                                                  host=host,
                                                  is_secure=is_secure)
        return s3_conn
    else:
        logger.debug("Connect to s3 with proxy!")
        proxy_info = pm.get_proxy_info()

        proxy_host = proxy_info['host']
        proxy_port = proxy_info['port']
        proxy_username = proxy_info['user']
        proxy_password = proxy_info['pass']

        if proxy_host is None:
            logger.error("Proxy host must be set!")
            return None

        try:
            s3_conn = boto.s3.connection.S3Connection(aws_access_key_id=key_id,
                                                  aws_secret_access_key=secret_key,
                                                  proxy=proxy_host,
                                                  proxy_port=proxy_port,
                                                  proxy_user=proxy_username,
                                                  proxy_pass=proxy_password,
                                                  host=host,
                                                  is_secure=is_secure)
        except Exception as e:
            logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
            raise e

        logger.debug("Connect to s3 success")
        return s3_conn

def connect_s3_to_region(key_id,secret_key,session_key,region,is_secure=True):
    logger.debug("Connect to s3")
    pm = ProxyManager(session_key)
    proxy = pm.get_proxy()
    if proxy is None or not proxy.get_enable():
        s3_conn = boto.s3.connect_to_region(region,
                                            aws_access_key_id=key_id,
                                            aws_secret_access_key=secret_key,
                                            is_secure=is_secure)
        return s3_conn
    else:
        logger.debug("Connect to s3 with proxy!")
        proxy_info = pm.get_proxy_info()

        proxy_host = proxy_info['host']
        proxy_port = proxy_info['port']
        proxy_username = proxy_info['user']
        proxy_password = proxy_info['pass']

        if proxy_host is None:
            logger.error("Proxy host must be set!")
            return None

        try:
            s3_conn = boto.s3.connect_to_region(region,
                                                aws_access_key_id=key_id,
                                                aws_secret_access_key=secret_key,
                                                proxy=proxy_host,
                                                proxy_port=proxy_port,
                                                proxy_user=proxy_username,
                                                proxy_pass=proxy_password,
                                                is_secure=is_secure)
        except Exception as e:
            logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
            raise e

        logger.debug("Connect to s3 success")
        return s3_conn

def connect_sqs(region,key_id,secret_key,session_key):
    logger.debug("Connect to sqs")
    pm = ProxyManager(session_key)
    proxy = pm.get_proxy()
    if proxy is None or not proxy.get_enable():
        sqs_conn = boto.sqs.connect_to_region(region,
                                              aws_access_key_id=key_id,
                                              aws_secret_access_key=secret_key)
        return sqs_conn
    else:
        logger.debug("Connect to sqs with proxy")
        proxy_info = pm.get_proxy_info()

        host = proxy_info['host']
        port = proxy_info['port']
        username = proxy_info['user']
        password = proxy_info['pass']

        if host is None:
            logger.error("Proxy host must be set!")
            return None

        logger.debug("Connect to sqs with proxy start")
        try:
            sqs_conn = boto.sqs.connect_to_region(region,
                                              aws_access_key_id=key_id,
                                              aws_secret_access_key=secret_key,
                                              proxy=host,
                                              proxy_port=port,
                                              proxy_user=username,
                                              proxy_pass=password)
        except Exception as e:
            logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
            raise e
        logger.debug("Connect to sqs success")
        return sqs_conn

def connect_cloudwatch(region,key_id,secret_key,session_key):
    logger.debug("Connect to cloudwatch ")
    pm = ProxyManager(session_key)
    proxy = pm.get_proxy()
    if proxy is None or not proxy.get_enable():
        logger.debug("Connect to cloudwatch without proxy")
        try:
            conn = boto.ec2.cloudwatch.connect_to_region(region,
                                                        aws_access_key_id=key_id,
                                                        aws_secret_access_key=secret_key)
        except Exception as e:
            logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
            raise e
        logger.debug("Connect to cloudwatch without proxy success")
        return conn
    else:
        logger.debug("Connect to cloudwatch with proxy")
        proxy_info = pm.get_proxy_info()

        host = proxy_info['host']
        port = proxy_info['port']
        username = proxy_info['user']
        password = proxy_info['pass']

        if host is None:
            logger.error("Proxy host must be set!")
            return None

        logger.debug("Connect to cloudwatch with proxy start")
        try:
            conn = boto.ec2.cloudwatch.connect_to_region(region,
                                              aws_access_key_id=key_id,
                                              aws_secret_access_key=secret_key,
                                              proxy=host,
                                              proxy_port=port,
                                              proxy_user=username,
                                              proxy_pass=password)
        except Exception as e:
            logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
            raise e
        logger.debug("Connect to cloudwatch success")
        return conn

def list_cloudwatch_namespaces(region,key_id,secret_key,session_key=None):
    conn = connect_cloudwatch(region,key_id,secret_key, session_key)
    metrics = conn.list_metrics()
    metrics_total = metrics

    while metrics.next_token is not None:
        metrics = conn.list_metrics(next_token=metrics.next_token)
        metrics_total = metrics_total + metrics

    namespaces = []
    for m in metrics_total:
        namespaces.append(m.namespace)
    return list(set(namespaces))

def exact_matcher(regex_str):
    if regex_str:
        exact_str = regex_str if regex_str[-1] == '$' else regex_str+'$'
        return re.compile(exact_str)
    else:
        return None

def blacklisted(key_name, black_matcher, white_matcher):
    # re.match() matches from the begining
    # Add '$' to match end of word for strict match
    if white_matcher:
        if white_matcher.match(key_name):
            return False
    return True if (black_matcher and black_matcher.match(key_name)) else False

def get_keys(bucket, prefix="", delimiter="/", recursion_depth=0, object_keys=True, prefix_keys=True,
             whitelist=None, blacklist=None, last_modified_after=None, last_modified_before=None):

    """

    @param bucket:
    @param prefix:
    @param delimiter:
    @param recursion_depth:
    @param object_keys:
    @param prefix_keys:
    @param whitelist:
    @param blacklist:
    @param last_modified_after:
    @param last_modified_before:
    @return:
    """
    black_matcher=exact_matcher(blacklist)
    white_matcher=exact_matcher(whitelist)
    x=bucket.list(prefix=prefix, delimiter=delimiter)
    for key in x:

        if isinstance(key, boto.s3.prefix.Prefix):
            if prefix_keys and not blacklisted(key.name, black_matcher, white_matcher):
                yield key
            if recursion_depth or recursion_depth == -1:
                for child_key in get_keys(bucket, key.name, delimiter, max(-1, recursion_depth - 1),
                                          object_keys, prefix_keys, whitelist, blacklist,
                                          last_modified_after, last_modified_before):
                    yield child_key

        elif object_keys and key.name[-1] != delimiter and not blacklisted(key.name, black_matcher, white_matcher):
            do_yield = True
            if last_modified_after and key.last_modified < last_modified_after:
                do_yield = False
            if last_modified_before and key.last_modified >= last_modified_before:
                do_yield = False

            if do_yield:
                yield key
