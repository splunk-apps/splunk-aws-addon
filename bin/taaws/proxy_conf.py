'''
Copyright (C) 2005 - 2013 Splunk Inc. All Rights Reserved.
'''
import os
import ConfigParser

from aws_accesskeys import APPNAME, KEY_NAMESPACE, KEY_OWNER

SECTION = 'default'
DOMAIN = '_aws_proxy'

import logging
from log import setup_logger
logger = setup_logger(os.path.basename(__file__), level=logging.DEBUG)

from splunk.appserver.mrsparkle.lib.util import make_splunkhome_path
from credentials import CredentialManager

class ProxyInfo(object):

    def __init__(self, proxystr):
        self.proxystr = proxystr
        self.enable = None
        self.proxy = None
        self._host = None
        self._port = None
        self._user = None
        self._pass = None
        self._parse()


    def _parse(self):
        proxystr = self.proxystr
        if proxystr is None:
            return

        parts = proxystr.split('|')
        self.enable = parts[1]

        proxystr = parts[0]
        self.proxy = proxystr
        account = None
        url = None
        parts = proxystr.split('@')
        if len(parts) == 1:
            url = parts[0]
        elif len(parts) == 2:
            url = parts[1]
            account = parts[0]
        else:
            logger.error("Invalue proxy string {}".format(proxystr))
            return

        parts = url.split(':')
        if len(parts) == 1:
            self._host = parts[0]
        elif len(parts) == 2:
            self._host = parts[0]
            self._port = parts[1]
        else:
            logger.error("Invalue proxy string, wrong url {}".format(proxystr))
            return

        if account is not None:
            parts = account.split(':')
            if len(parts) == 2:
                self._user = parts[0]
                self._pass = parts[1]
            else:
                logger.error("Invalue proxy string, wrong user account {}".format(proxystr))
                return

    def get_enable(self):
        return self.enable in ('1', 'true', 'yes', 'y', 'on')

    def get_proxy(self):
        return self.proxy

    def get_proxy_info(self):
        info = { 'host':self._host, 'port':self._port, 'user':self._user,'pass':self._pass}
        return info

class ProxyManager(object):
    
    def __init__(self, sessionKey):
        self._cred_mgr = CredentialManager(sessionKey=sessionKey)
        logger.debug("Proxy Manager Initialized {}".format(sessionKey))

    def get_proxy_info(self):
        '''Get the proxy info object.
        
        @return: The proxy info object.
        '''
        try:
            c = self._cred_mgr.all().filter_by_app(KEY_NAMESPACE).filter_by_user(KEY_OWNER).filter(realm=DOMAIN)[0]
            proxy = ProxyInfo(c.clear_password)

            return proxy.get_proxy_info()
        except IndexError as e:
            logger.error("Failed to get proxy information {} ".format(type(e).__name__))
            return None

    def get_proxy(self):
        '''Get the proxy object.
        
        @return: The proxy object.
        '''
        logger.debug("Get Proxy of ProxyManager")
        try:
            c = self._cred_mgr.all().filter_by_app(KEY_NAMESPACE).filter_by_user(KEY_OWNER).filter(realm=DOMAIN)
            if len(c) == 0:
                logger.debug("The proxy is not set")
                return None
            proxy = ProxyInfo(c[0].clear_password)
            return proxy
        except IndexError as e:
            logger.error("Failed to get proxy {} ".format(type(e).__name__))
            return None
        except Exception as e:
            logger.error("Failed to get proxy {} ".format(type(e).__name__))
            return None
    
    def set(self, proxy, enable):
        info = proxy + "|" + enable
        try:
            self._cred_mgr.create_or_set(SECTION,DOMAIN,info,KEY_NAMESPACE,KEY_OWNER)
        except Exception as e:
            logger.error("Failed to set proxy {} ".format(type(e).__name__))
            raise e
        