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


class ClientTransport(object):
    """Encapsulation of a client session."""

    def __init__(self, server, framed_transport, timeout, logins, recycle):
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
                client.login(request)

        self.client = client
        self.transport = transport

        if recycle:
            self.recycle = time.time() + recycle + random.uniform(0, recycle * 0.1)
        else:
            self.recycle = None


def connect(servers=None, framed_transport=False, timeout=None,
            logins=None, retry_time=60, recycle=None, round_robin=None):
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
              Minimum time in seconds until a failed server is reinstated. (e.g. 0.5)

              Default: 60
    logins : dict
              Dictionary of Keyspaces and Credentials

              Example: {'Keyspace1' : {'username':'jsmith', 'password':'havebadpass'}}
    recycle: float
              Max time in seconds before an open connection is closed and returned to the pool.

              Default: None (Never recycle)
    round_robin: bool
              *DEPRECATED*

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]

    return ThreadLocalConnection(servers, framed_transport, timeout,
                                 retry_time, recycle, logins)

connect_thread_local = connect


class ServerSet(object):
    """Automatically balanced set of servers.
       Manages a separate stack of failed servers, and automatic
       retrial."""

    def __init__(self, servers, retry_time=10):
        self._lock = threading.RLock()
        self._servers = list(servers)
        self._retry_time = retry_time
        self._dead = []

    def get(self):
        self._lock.acquire()
        try:
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
        finally:
            self._lock.release()

    def mark_dead(self, server):
        self._lock.acquire()
        try:
            self._servers.remove(server)
            self._dead.insert(0, (time.time() + self._retry_time, server))
        finally:
            self._lock.release()


class ThreadLocalConnection(object):
    def __init__(self, servers, framed_transport=False, timeout=None,
                 retry_time=10, recycle=None, logins=None):
        self._servers = ServerSet(servers, retry_time)
        self._framed_transport = framed_transport
        self._timeout = timeout
        self._recycle = recycle
        if logins is None:
            self._logins = {}
        else:
            self._logins = logins
        self._local = threading.local()
        self._local.conn = None

    def login(self, keyspace, credentials):
        self._logins[keyspace] = credentials

    def __getattr__(self, attr):
        def _client_call(*args, **kwargs):
            try:
                conn = self._ensure_connection()
                return getattr(conn.client, attr)(*args, **kwargs)
            except (Thrift.TException, socket.timeout, socket.error), exc:
                log.exception('Client error: %s', exc)
                self.close()
                return _client_call(*args, **kwargs) # Retry
        setattr(self, attr, _client_call)
        return getattr(self, attr)

    def _ensure_connection(self):
        """Make certain we have a valid connection and return it."""
        conn = self.connect()
        if conn.recycle and conn.recycle < time.time():
            log.debug('Client session expired after %is. Recycling.', self._recycle)
            self.close()
            conn = self.connect()
        return conn

    def connect(self):
        """Create new connection unless we already have one."""
        if not self._local.conn:
            try:
                server = self._servers.get()
                log.debug('Connecting to %s', server)
                self._local.conn = ClientTransport(server, self._framed_transport,
                                                   self._timeout, self._logins, self._recycle)
            except (Thrift.TException, socket.timeout, socket.error):
                log.warning('Connection to %s failed.', server)
                self._servers.mark_dead(server)
                return self.connect()
        return self._local.conn

    def close(self):
        """If a connection is open, close its transport."""
        if self._local.conn:
            self._local.conn.transport.close()
        self._local.conn = None


