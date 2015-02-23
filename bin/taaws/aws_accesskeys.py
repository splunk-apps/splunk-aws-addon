"""
This should probably be rewritten to just use SplunkAppObjModel directly...
"""

import os
PARENT = os.path.sep+os.path.pardir
APPNAME = os.path.basename(os.path.abspath(__file__+PARENT+PARENT+PARENT))
KEY_NAMESPACE = APPNAME
KEY_OWNER = 'admin'

from credentials import CredentialManager


class AwsAccessKey(object):

    def __init__(self, key_id, secret_key, name='default'):
        self.name = name
        self.key_id = key_id and key_id.strip() or ''
        self.secret_key = secret_key and secret_key.strip() or ''


class AwsAccessKeyManager(object):

    def __init__(self, namespace, owner, session_key):
        self.namespace = namespace
        self.owner = owner
        self._cred_mgr = CredentialManager(sessionKey=session_key)

    def set_accesskey(self, key_id, secret_key, name='default'):
        if name is None:
            name = ''
        # create_or_set() will raise if empty username or password strings are passed
        key_id = key_id and key_id.strip() or ' '
        secret_key = secret_key and secret_key.strip() or ' '
        c = self.get_accesskey(name)
        if c and c.key_id != key_id:
            self.delete_accesskey(name)
        self._cred_mgr.create_or_set(key_id, name, secret_key, self.namespace, self.owner)

    def get_accesskey(self, name='default'):
        if name is None:
            name = ''
        try:
            c = self._cred_mgr.all().filter_by_app(self.namespace).filter_by_user(self.owner).filter(realm=name)[0]
            return AwsAccessKey(c.username, c.clear_password, c.realm)
        except IndexError:
            return None

    def all_accesskeys(self):
        class AccessKeyIterator(object):

            def __init__(self, mgr):
                self.creds = mgr._cred_mgr.all().filter_by_app(mgr.namespace).filter_by_user(mgr.owner)

            def __iter__(self):
                for c in self.creds:
                    yield AwsAccessKey(c.username, c.clear_password, c.realm)
        return AccessKeyIterator(self)

    def delete_accesskey(self, name='default'):
        if name is None:
            name = ''
        c = self.get_accesskey(name)
        if c:
            self._cred_mgr.delete(c.key_id, c.name, self.namespace, self.owner)


