from pycassa import connect, connect_thread_local, index, ColumnFamily,\
                    ConsistencyLevel, NotFoundException, QueuePool,\
                    SingletonThreadPool, StaticPool, NullPool, AssertionPool

from nose.tools import assert_raises

import struct

_pool_classes = [QueuePool, SingletonThreadPool, StaticPool, NullPool, AssertionPool]

class TestDict(dict):
    pass

class TestColumnFamilyWithPools:
    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pools = []
        self.clients = []
        self.cfs = []
        for pool_cls in _pool_classes:
            pool = pool_cls(keyspace='Keyspace1', credentials=credentials)
            self.pools.append(pool)
            client = pool.get()
            self.clients.append(client)
            cf = ColumnFamily(client, 'Standard%s' % pool_cls.__name__,
                              write_consistency_level=ConsistencyLevel.ONE,
                              buffer_size=2, timestamp=self.timestamp,
                              dict_class=TestDict)
            self.cfs.append(cf)

        self.timestamp_n = 0
        for cf in self.cfs:
            try:
                self.timestamp_n = max(self.timestamp_n,
                                       int(cf.get('meta')['timestamp']))
            except NotFoundException:
                pass
        self.clear()

    def tearDown(self):
        for cf in self.cfs:
            cf.insert('meta', {'timestamp': str(self.timestamp_n)})

    # Since the timestamp passed to Cassandra will be in the same second
    # with the default timestamp function, causing problems with removing
    # and inserting (Cassandra doesn't know which is later), we supply our own
    def timestamp(self):
        self.timestamp_n += 1
        return self.timestamp_n

    def clear(self):
        for cf in self.cfs:
            for key, columns in cf.get_range(include_timestamp=True):
                for value, timestamp in columns.itervalues():
                    self.timestamp_n = max(self.timestamp_n, timestamp)
                cf.remove(key)

    def test_empty(self):
        key = 'TestColumnFamily.test_empty'
        for cf in self.cfs:
            assert_raises(NotFoundException, cf.get, key)
            assert len(cf.multiget([key])) == 0
            for key, columns in cf.get_range():
                assert len(columns) == 0

    def test_insert_get(self):
        key = 'TestColumnFamily.test_insert_get'
        for cf in self.cfs:
            columns = {'1': 'val1', '2': 'val2'}
            assert_raises(NotFoundException, cf.get, key)
            cf.insert(key, columns)
            assert cf.get(key) == columns

    def test_insert_multiget(self):
        key1 = 'TestColumnFamily.test_insert_multiget1'
        columns1 = {'1': 'val1', '2': 'val2'}
        key2 = 'test_insert_multiget1'
        columns2 = {'3': 'val1', '4': 'val2'}
        missing_key = 'key3'

        for cf in self.cfs:
            cf.insert(key1, columns1)
            cf.insert(key2, columns2)
            rows = cf.multiget([key1, key2, missing_key])
            assert len(rows) == 2
            assert rows[key1] == columns1
            assert rows[key2] == columns2
            assert missing_key not in rows

    def test_insert_get_count(self):
        key = 'TestColumnFamily.test_insert_get_count'
        columns = {'1': 'val1', '2': 'val2'}
        for cf in self.cfs:
            cf.insert(key, columns)
            assert cf.get_count(key) == 2

    def test_insert_get_range(self):
        keys = ['TestColumnFamily.test_insert_get_range%s' % i for i in xrange(5)]
        columns = {'1': 'val1', '2': 'val2'}
        for cf in self.cfs:
            for key in keys:
                cf.insert(key, columns)

            rows = list(cf.get_range(start=keys[0], finish=keys[-1]))
            assert len(rows) == len(keys)
            for i, (k, c) in enumerate(rows):
                assert k == keys[i]
                assert c == columns

    def test_remove(self):
        key = 'TestColumnFamily.test_remove'
        for cf in self.cfs:
            columns = {'1': 'val1', '2': 'val2'}
            cf.insert(key, columns)

            cf.remove(key, columns=['2'])
            del columns['2']
            assert cf.get(key) == {'1': 'val1'}

            cf.remove(key)
            assert_raises(NotFoundException, cf.get, key)

    def test_dict_class(self):
        key = 'TestColumnFamily.test_dict_class'
        for cf in self.cfs:
            cf.insert(key, {'1': 'val1'})
            assert isinstance(cf.get(key), TestDict)

class TestSuperColumnFamily:
    def setUp(self):
        credentials = {'username': 'jsmith', 'password': 'havebadpass'}
        self.pools = []
        self.clients = []
        self.cfs = []
        for pool_cls in _pool_classes:
            pool = pool_cls(keyspace='Keyspace1', credentials=credentials)
            self.pools.append(pool)
            client = pool.get()
            self.clients.append(client)
            cf = ColumnFamily(client, 'Super%s' % pool_cls.__name__,
                              write_consistency_level=ConsistencyLevel.ONE,
                              buffer_size=2, timestamp=self.timestamp,
                              super=True)
            self.cfs.append(cf)

        self.timestamp_n = 0
        for cf in self.cfs:
            try:
                self.timestamp_n = max(self.timestamp_n,
                                       int(cf.get('meta')['meta']['timestamp']))
            except NotFoundException:
                pass
        self.clear()

    def tearDown(self):
        for cf in self.cfs:
            cf.insert('meta', {'meta': {'timestamp': str(self.timestamp_n)}})

    # Since the timestamp passed to Cassandra will be in the same second
    # with the default timestamp function, causing problems with removing
    # and inserting (Cassandra doesn't know which is later), we supply our own
    def timestamp(self):
        self.timestamp_n += 1
        return self.timestamp_n

    def clear(self):
        for cf in self.cfs:
            for key, columns in cf.get_range(include_timestamp=True):
                for subcolumns in columns.itervalues():
                    for value, timestamp in subcolumns.itervalues():
                        self.timestamp_n = max(self.timestamp_n, timestamp)
                cf.remove(key)

    def test_super(self):
        key = 'TestSuperColumnFamily.test_super'
        columns = {'1': {'sub1': 'val1', 'sub2': 'val2'}, '2': {'sub3': 'val3', 'sub4': 'val4'}}
        for cf in self.cfs:
            assert_raises(NotFoundException, cf.get, key)
            cf.insert(key, columns)
            assert cf.get(key) == columns
            assert cf.multiget([key]) == {key: columns}
            assert list(cf.get_range(start=key, finish=key)) == [(key, columns)]

    def test_super_column_argument(self):
        key = 'TestSuperColumnFamily.test_super_columns_argument'
        sub12 = {'sub1': 'val1', 'sub2': 'val2'}
        sub34 = {'sub3': 'val3', 'sub4': 'val4'}
        columns = {'1': sub12, '2': sub34}
        for cf in self.cfs:
            cf.insert(key, columns)
            assert cf.get(key, super_column='1') == sub12
            assert_raises(NotFoundException, cf.get, key, super_column='3')
            assert cf.multiget([key], super_column='1') == {key: sub12}
            assert list(cf.get_range(start=key, finish=key, super_column='1')) == [(key, sub12)]
