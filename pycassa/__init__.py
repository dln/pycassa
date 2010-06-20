"""pycassa is a Cassandra library with the following features:

1. Auto-failover single or thread-local connections
2. A simplified version of the thrift interface
3. A method to map an existing class to a Cassandra ColumnFamily.
4. Support for SuperColumns
"""

__version_info__ = (0, 3, 0)
__version__ = '.'.join(map(str, __version_info__))

from pycassa.columnfamily import *
from pycassa.columnfamilymap import *
from pycassa.types import *
from pycassa.connection import *

from cassandra.ttypes import ConsistencyLevel, InvalidRequestException, \
    NotFoundException, UnavailableException, TimedOutException
