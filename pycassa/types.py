from datetime import datetime
import struct
import time

try:
    from pymongo import bson
except ImportError:
    bson = None

try:
    import wbin
except ImportError:
    wbin = None

try:
    from struct import Struct as _Struct
except ImportError:
    class _Struct(object):
        def __init__(self, fmt):
            self.fmt = fmt
            self.size = struct.calcsize(fmt)

        def pack(self, *vs):
            return struct.pack(self.fmt, *vs)

        def unpack(self, string):
            return struct.unpack(self.fmt, string)

__all__ = ['BSON', 'Column', 'DateTime', 'DateTimeString', 'Float64', 'FloatString',
           'Int64', 'IntString', 'String', 'Wirebin']


class Column(object):
    def __init__(self, default=None):
        self.default = default

class DateTime(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = _Struct('q')

    def pack(self, val):
        if not isinstance(val, datetime):
            raise TypeError('expected datetime, %s found' % type(val).__name__)
        return self.struct.pack(int(time.mktime(val.timetuple())))

    def unpack(self, val):
        return datetime.fromtimestamp(self.struct.unpack(val)[0])

class DateTimeString(Column):
    format = '%Y-%m-%d %H:%M:%S'
    def pack(self, val):
        if not isinstance(val, datetime):
            raise TypeError('expected datetime, %s found' % type(val).__name__)
        return val.strftime(self.format)

    def unpack(self, val):
        return datetime.strptime(val, self.format)

class Float64(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = _Struct('d')

    def pack(self, val):
        if not isinstance(val, float):
            raise TypeError('expected float, %s found' % type(val).__name__)
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class FloatString(Column):
    def pack(self, val):
        if not isinstance(val, float):
            raise TypeError('expected float, %s found' % type(val).__name__)
        return str(val)

    def unpack(self, val):
        return float(val)

class Int64(Column):
    def __init__(self, *args, **kwargs):
        Column.__init__(self, *args, **kwargs)
        self.struct = _Struct('q')

    def pack(self, val):
        if not isinstance(val, (int, long)):
            raise TypeError('expected int or long, %s found' % type(val).__name__)
        return self.struct.pack(val)

    def unpack(self, val):
        return self.struct.unpack(val)[0]

class IntString(Column):
    def pack(self, val):
        if not isinstance(val, (int, long)):
            raise TypeError('expected int or long, %s found' % type(val).__name__)
        return str(val)

    def unpack(self, val):
        return int(val)

class String(Column):
    def pack(self, val):
        if not isinstance(val, basestring):
            raise TypeError('expected str or unicode, %s found' % type(val).__name__)
        return val

    def unpack(self, val):
        return val


if not bson:
    class BSON(Column):
        def __getattr__(self): raise ImportError('BSON column type requires `pymongo`. See http://bsonspec.org/')

else:
    class BSON(Column):
        """Binary-encoded serialization of JSON-like documents."""
        def pack(self, val):
            if not isinstance(val, dict):
                raise TypeError('expected dict, %s found' % type(val).__name__)
            return str(bson.BSON.from_dict(val))

        def unpack(self, val):
            return bson.BSON(val).to_dict()

if not wbin:
    class Wirebin(Column):
        def __getattr__(self): raise ImportError('BSON column type requires `wbin`. See http://github.com/slideinc/wirebin/.')

else:
    class Wirebin(Column):
        """Fast binary [de]serialization of native Python types."""
        def pack(self, val):
            return wbin.serialize(val)

        def unpack(self, val):
            return wbin.deserialize(val)

