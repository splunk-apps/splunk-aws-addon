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
logger = setup_logger(APPNAME + '-RestEndpoints', level=logging.DEBUG)

from taaws.proxy_conf import ProxyManager 

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

        if self.requestedAction == splunk.admin.ACTION_EDIT:
            for arg in ['enable', 'proxy']:
                self.supportedArgs.addOptArg(arg)

    @log_enter_exit
    def handleList(self, confInfo):

        pm  = ProxyManager(self.getSessionKey())

        proxy = pm.get_proxy() 

        if proxy is None :
            confInfo['proxy'].append('enable', 0)
            confInfo['proxy'].append('proxy', '')
        else :
            confInfo['proxy'].append('enable', 1 if proxy.get_enable() else 0)
            confInfo['proxy'].append('proxy', proxy.get_proxy())

    @log_enter_exit
    def handleEdit(self, confInfo):
        try:
            pm = ProxyManager(self.getSessionKey())
            enable = self.callerArgs.data['enable'][0]
            enable = enable and enable.strip() or False
            proxy = self.callerArgs.data['proxy'][0]
            proxy = proxy and proxy.strip() or ''
            logger.info("Start to edit {}".format(proxy))
            pm.set(proxy,enable)
        except Exception as e:
            logger.error("Failed to edit {}".format(str(e)))


@log_enter_exit
def main():
    splunk.admin.init(ConfigHandler, splunk.admin.CONTEXT_NONE)


if __name__ == '__main__':

    main()
