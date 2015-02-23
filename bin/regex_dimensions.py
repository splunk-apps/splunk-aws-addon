import re
import json
import os

import logging
from taaws.log import setup_logger
logger = setup_logger(os.path.basename(__file__), level=logging.DEBUG)

def list_metrics(aws_connection, namespace):
    try:
        all_metrics = aws_connection.list_metrics(namespace=namespace)
    except Exception as e:
        logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
        raise e
    logger.debug("List metrics done")
    next=all_metrics.next_token
    while next!=None:
        metrics=aws_connection.list_metrics(next_token=next, namespace=namespace)
        all_metrics+=metrics
        next=metrics.next_token
    return all_metrics

def list_dimension_in_metrics(metrics):
    dimensions = {}
    for m in metrics:
        dimensions[json.dumps(m.dimensions, sort_keys=True)] = m.dimensions
        #print m.name
        #print m.dimensions
    return dimensions.values()


class DimensionExactMatcher():

    def __init__(self, re_value_dict):
        self.regexes = {}
        for key in re_value_dict:
            if not isinstance(re_value_dict[key], list):
                re_value_dict[key] = [re_value_dict[key]]
            self.regexes[key] = []
            for regex_str in re_value_dict[key]:
                exact_str = regex_str if regex_str[-1] == '$' else regex_str + '$'
                self.regexes[key].append(re.compile(exact_str))

    def exact_match(self, dimension):
        if len(self.regexes)!=len(dimension):
            return False
        for key in self.regexes:
            if not key in dimension:
                return False
            for regex in self.regexes[key]:
                if isinstance(dimension[key], list):
                    matched = False
                    for value in dimension[key]:
                        if regex.match(value):
                            matched = True
                            break
                    if not matched:
                        return False
                else:
                    if not regex.match(dimension[key]):
                        return False
        return True


def get_dimension_list(all_dimensions, dimension_re_list):
    matched = []
    matcher_list = []
    if dimension_re_list==None:
        return all_dimensions
    if type(dimension_re_list) != list:
        dimension_re_list = [dimension_re_list]
    for re_value_dict in dimension_re_list:
        matcher_list.append(DimensionExactMatcher(re_value_dict))
    for dimension in all_dimensions:
        for matcher in matcher_list:
            if matcher.exact_match(dimension):
                matched.append(dimension)
                break
    return matched

def get_dimensions(cw_conn, namespace, dimension_re_list):
    try:
        metrics=list_metrics(cw_conn, namespace)
        all_dimensions = list_dimension_in_metrics(metrics)
        return get_dimension_list(all_dimensions,dimension_re_list)
    except Exception as e:
        logger.log(logging.ERROR, "{}: {}", type(e).__name__, e)
        raise e