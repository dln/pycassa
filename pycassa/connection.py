from exceptions import Exception
import logging
import random
import socket
import threading
import time

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra
from cassandra.ttypes import AuthenticationRequest

__all__ = ['connect', 'connect_thread_local', 'NoServerAvailable']

DEFAULT_SERVER = 'localhost:9160'

log = logging.getLogger('pycassa')

class NoServerAvailable(Exception):
    pass

def create_client_transport(server, framed_transport, timeout, logins):
    host, port = server.split(":")
    socket = TSocket.TSocket(host, int(port))
    if timeout is not None:
        socket.setTimeout(timeout*1000.0)
    if framed_transport:
        transport = TTransport.TFramedTransport(socket)
    else:
        transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    transport.open()

    if logins is not None:
        for keyspace, credentials in logins.iteritems():
            request = AuthenticationRequest(credentials=credentials)
            client.login(keyspace, request)

    return client, transport



def connect(servers=None, framed_transport=False, timeout=None, logins=None, retry_time=60, recycle=None):
    """
    Constructs a single Cassandra connection. Connects to a randomly chosen
    server on the list.

    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Failing servers are kept on a separate list and eventually retried, no
    sooner than `retry_time` seconds after failure.

    Parameters
    ----------
    servers : [server]
              List of Cassandra servers with format: "hostname:port"

              Default: ['localhost:9160']
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5)

              Default: None (it will stall forever)
    retry_time: float
              Minimum time in seconds until a failed server is , retry_time=60reinstated. (e.g. 0.5)

              Default: 60
    logins : dict
              Dictionary of Keyspaces and Credentials

              Example: {'Keyspace1' : {'username':'jsmith', 'password':'havebadpass'}}
    recycle: float
              Max time in seconds before an open connection is closed and returned to the pool.

              Default: None (Never recycle)

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return SingleConnection(servers, framed_transport, timeout, retry_time, recycle, logins)

def connect_thread_local(servers=None, round_robin=None, framed_transport=False, timeout=None, logins=None, retry_time=60, recycle=None):
    """
    Constructs a Cassandra connection for each thread.

    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Failing servers are kept on a separate list and eventually retried, no
    sooner than `retry_time` seconds after failure.

    Parameters

    Parameters
    ----------
    servers : [server]
              List of Cassandra servers with format: "hostname:port"

              Default: ['localhost:9160']
    round_robin: bool
              *DEPRECATED*
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5 for half a second)

              Default: None (it will stall forever)
    logins : dict
              Dictionary of Keyspaces and Credentials

              Example: {'Keyspace1' : {'username':'jsmith', 'password':'havebadpass'}}
    retry_time: float
              Minimum time in seconds until a failed server is reinstated. (e.g. 0.5)

              Default: 60
    recycle: float
              Max time in seconds before an open connection is closed and returned to the pool.

              Default: None (Never recycle)
    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    if round_robin is not None:
        log.warning('connect_thread_local: `round_robin` parameter is deprecated.')
    return ThreadLocalConnection(servers, framed_transport, timeout, retry_time, recycle, logins)


class SingleConnection(object):
    def __init__(self, servers, framed_transport, timeout, retry_time, recycle, logins):
        self._servers = list(servers)
        self._dead = []
        self._framed_transport = framed_transport
        self._timeout = timeout
        self._retry_time = retry_time
        self._recycle = recycle
        self._recycle_time = None
        log.debug('Using configuration: servers=%r, framed_transport=%r, timeout=%r, retry_time=%r, recycle=%r, logins=%r',
                  servers, framed_transport, timeout, retry_time, recycle, logins)
        if logins is None:
            self._logins = {}
        else:
            self._logins = logins
        self._client = None
        self._lock = threading.RLock()


    def login(self, keyspace, credentials):
        self._logins[keyspace] = credentials

    def __getattr__(self, attr):
        def _client_call(*args, **kwargs):
            while True:
                if self._recycle_time and self._recycle_time < time.time():
                    log.debug('Recycling connection.')
                    self.close()
                self.connect()
                try:
                    return getattr(self._client, attr)(*args, **kwargs)
                except (Thrift.TException, socket.timeout, socket.error), exc:
                    log.exception('Client error: %s', exc)
                    self.close()

        setattr(self, attr, _client_call)
        return getattr(self, attr)

    def connect(self):
        if self._client is None:
            self._client, self._transport = self._connect()

    def close(self):
        self._transport.close()
        self._client = None
        log.debug('Connection closed')

    def _get_server(self):
        if self._dead:
            ts, revived = self._dead.pop()
            if ts > time.time():  # Not yet, put it back
                self._dead.append((ts, revived))
            else:
                self._servers.append(revived)
                log.info('Server %r reinstated into working pool', revived)
        if not self._servers:
            log.critical('No servers available')
            raise NoServerAvailable()
        return random.choice(self._servers)

    def _connect(self):
        try:
            server = self._get_server()
            log.info('Connecting to %s', server)
            if self._recycle:
                self._recycle_time = time.time() + self._recycle + random.uniform(0, self._recycle * 0.1)
                log.debug('Connection will be forcefully recycled in %.2fs', self._recycle)
            return create_client_transport(server, self._framed_transport, self._timeout, self._logins)
        except (Thrift.TException, socket.timeout, socket.error):
            self._servers.remove(server)
            self._dead.insert(0, (time.time() + self._retry_time, server))
            log.warning('Connection to %r failed.', server)
            return self._connect()


class ThreadLocalConnection(SingleConnection):
    def __init__(self, servers, framed_transport, timeout, retry_time, recycle, logins):
        super(ThreadLocalConnection, self).__init__(servers, framed_transport, timeout, retry_time, recycle, logins)
        self._local = threading.local()

        def connect(self):
            if getattr(self._local, 'client', None) is None:
                self._local.client, self._local.transport = self._servers.connect()

        def close(self):
            self._local.transport.close()
            self._local.client = None
            log.debug('Connection closed')

