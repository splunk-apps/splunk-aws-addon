"""

"""

import tarfile

import boto.ec2
import boto.s3.connection
import boto.exception


def get_stream_reader(source, source_start_func=None, source_stop_func=None):
    """

    @param source:
    @param source_start_func:
    @param source_stop_func:
    @return:
    """
    reader = _get_stream_reader_class(source)
    return reader(source, source_start_func=source_start_func, source_stop_func=source_stop_func)


def _get_stream_reader_class(source):
    """

    @param source:
    @return:
    """
    readers = [d for d in S3KeyRawStreamReader.__subclasses__() if d is not S3KeyRawStreamReader]

    for reader in readers:
        if reader.can_read(source):
            return reader
    else:
        return S3KeyRawStreamReader


class _S3KeyPersistentEofReadShim(object):
    """ Needed to adapt boto.s3.key.Key.read() to work with tarfile._Stream.read()"""

    def __init__(self, s3key):
        self._s3key = s3key
        self._eof = False

    def read(self, size=0):
        if self._eof:
            return ''
        else:
            if size > 0:
                data = self._s3key.read(size)
            else:
                data = self._s3key.read()
            if not data:
                self._eof = True
            return data

    def __getattr__(self, item):
        return getattr(self._s3key, item)


def unicode_source_name(bucket_name, file_name):
    return u"s3://{}/{}".format(bucket_name.decode('utf8'), file_name)

class S3KeyRawStreamReader(object):
    """Base class"""

    DEFAULT_READ_SIZE = 8192

    _buffer = ""
    _source_name = ""
    _position = 0

    @property
    def position(self):
        return self._position

    @property
    def source_name(self):
        return self._source_name

    def __init__(self, source, source_start_func=None, source_stop_func=None):
        assert issubclass(self.__class__, S3KeyRawStreamReader)
        self._source_start_func = source_start_func
        self._source_started = False
        self._source_stop_func = source_stop_func
        self._source_stopped = False

        if isinstance(source, boto.s3.key.Key):
            self._source_name = unicode_source_name(source.bucket.name, source.name)
        self._source = source

    def _source_start(self):
        """callback to notify new source starting"""
        if not self._source_started:
            self._source_started = True
            self._source_stopped = False
            if self._source_start_func:
                self._source_start_func(type(self))

    def _source_stop(self):
        """callback to notify source eof"""
        if not self._source_stopped:
            # empty file condition check
            # self._source_start() wouldn't have been called.
            self._source_start()

            self._source_stopped = True
            self._source_started = False
            if self._source_stop_func:
                self._source_stop_func(type(self), self.position)

    def seek_position(self, position):

        toread = position - self.position
        if toread < 0:
            raise IndexError()
        elif toread == 0:
            return

        while True:
            rsize = min(self.DEFAULT_READ_SIZE, toread)
            data = self.read(rsize)
            if not data:
                break
            toread -= len(data)
            if toread == 0:
                break

        if toread:
            raise IndexError()

    def read(self, size=DEFAULT_READ_SIZE):

        if self._buffer:
            buffers = []
            if size > 0:
                pos = min(size, len(self._buffer))
            else:
                pos = len(self._buffer)

            buffers.append(self._buffer[:pos])
            self._buffer = self._buffer[pos:]

            if size - pos > 0:
                buffers.append(self._read(size - pos))
            else:
                buffers.append(self._read())

            buf = "".join(buffers)
        else:
            buf = self._read(size)

        if buf:
            self._position += len(buf)
        return buf

    def _read(self, size):

        if size > 0:
            data = self._source.read(size)
        else:
            data = self._source.read()
        if data:
            self._source_start()
        else:
            self._source_stop()

        return data

    def readline(self):

        pos = self._buffer.find('\n') + 1
        if not pos:
            buffers = [self._buffer]
            while True:
                buf = self.read(size=self.DEFAULT_READ_SIZE)
                self._position -= len(buf)
                buffers.append(buf)
                pos = buf.find('\n') + 1
                if pos > 0:
                    self._buffer = "".join(buffers)
                    break
                elif not buf:
                    self._buffer = "".join(buffers)
                    pos = len(self._buffer)
                    break

        buf = self._buffer[:pos]
        self._buffer = self._buffer[pos:]
        self._position += len(buf)
        return buf

    def __iter__(self):

        while True:
            line = self.readline()
            if not line:
                break
            yield line

    @classmethod
    def can_read(cls, source):
        return True


class S3KeyTarStreamReader(S3KeyRawStreamReader):

    def __init__(self, source, source_start_func=None, source_stop_func=None):
        super(S3KeyTarStreamReader, self).__init__(source,
                                                   source_start_func=source_start_func,
                                                   source_stop_func=source_stop_func)

        if isinstance(source, boto.s3.key.Key):
            self._source = _S3KeyPersistentEofReadShim(source)

        self._member = None
        self._member_f = None
        self._reader = None

        self._source = tarfile.open(None, "r|*", self._source)
        self._next()

    @property
    def source_name(self):
        """override"""

        parts = [self._source_name, self._member.name, (self._reader and self._reader.source_name)]
        return ":".join(p for p in parts if p)

    def _next(self):

        self._member = None
        self._member_f = None
        self._reader = None

        while True:

            member = self._source.next()
            if not member:
                return False
            if member.isfile():
                self._member = member
                break

        self._member_f = self._source.extractfile(self._member)
        decoder = _get_stream_reader_class(self._member.name)
        self._reader = decoder(self._member_f,
                               source_start_func=self._source_start_func, source_stop_func=self._source_stop_func)
        return True

    def read(self, size=S3KeyRawStreamReader.DEFAULT_READ_SIZE):
        """override"""

        if not self._reader:
            return ''

        if size > 0:
            data = self._reader.read(size)
        else:
            data = self._reader.read()

        if not data:
            if not self._next():
                data = ''
            else:
                if size > 0:
                    data = self._reader.read(size)
                else:
                    data = self._reader.read()

        if data:
            self._position += len(data)
        return data

    @classmethod
    def can_read(cls, source):
        """override"""

        if isinstance(source, boto.s3.key.Key):
            source = source.name
        if (source.endswith('.tar') or source.endswith('.tgz') or
                source.endswith('.tar.gz') or source.endswith('tar.bz2')):
            return True

        return False


class S3KeyGzippedRawStreamReader(S3KeyRawStreamReader):

    def __init__(self, source, source_start_func=None, source_stop_func=None):
        super(S3KeyGzippedRawStreamReader, self).__init__(source,
                                                          source_start_func=source_start_func,
                                                          source_stop_func=source_stop_func)

        if isinstance(source, boto.s3.key.Key):
            self._source = _S3KeyPersistentEofReadShim(source)

        self._source = tarfile._Stream('', 'r', 'gz', self._source, tarfile.RECORDSIZE)

    @classmethod
    def can_read(cls, source):
        """override"""

        if isinstance(source, boto.s3.key.Key):
            source = source.name
        if source.endswith('.gz') and not source.endswith('.tar.gz'):
            return True

        return False
