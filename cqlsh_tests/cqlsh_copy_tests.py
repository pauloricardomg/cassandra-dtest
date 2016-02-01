# coding: utf-8
import csv
import datetime
import json
import glob
import os
import sys
import time
from collections import namedtuple
from contextlib import contextmanager
from decimal import Decimal
from dtest import warning
from tempfile import NamedTemporaryFile, gettempdir, template
from uuid import uuid1, uuid4

from cassandra.murmur3 import murmur3
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.util import SortedSet
from ccmlib.common import is_win

from cqlsh_tools import (DummyColorMap, assert_csvs_items_equal, csv_rows,
                         monkeypatch_driver, random_list,
                         strip_timezone_if_time_string, unmonkeypatch_driver,
                         write_rows_to_csv)
from dtest import Tester, canReuseCluster, freshCluster, debug, DISABLE_VNODES
from tools import rows_to_list, since

DEFAULT_FLOAT_PRECISION = 5  # magic number copied from cqlsh script
DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S%z'  # based on cqlsh script

PARTITIONERS = {
    "murmur3": "org.apache.cassandra.dht.Murmur3Partitioner",
    "random": "org.apache.cassandra.dht.RandomPartitioner",
    "byte": "org.apache.cassandra.dht.ByteOrderedPartitioner",
    "order": "org.apache.cassandra.dht.OrderPreservingPartitioner"
}


class UTC(datetime.tzinfo):
    """
    A utility class to specify a UTC timezone.
    """

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)


@canReuseCluster
class CqlshCopyTest(Tester):
    """
    Tests the COPY TO and COPY FROM features in cqlsh.
    @jira_ticket CASSANDRA-3906
    """

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)
        self._tempfiles = []

    @classmethod
    def setUpClass(cls):
        cls._cached_driver_methods = monkeypatch_driver()

    @classmethod
    def tearDownClass(cls):
        unmonkeypatch_driver(cls._cached_driver_methods)

    def tearDown(self):
        self.delete_temp_files()
        super(CqlshCopyTest, self).tearDown()

    def get_temp_file(self, prefix=template, suffix=""):
        """
        On windows we cannot open temporary files after creating them unless we close them first.
        For this reason we must also create them with delete=False (or they would be deleted immediately when closed)
        and we want to make sure that the test object owns a reference to the file objects by adding them
        to self._tempfiles, so that they can be deleted when the test finishes.
        """
        ret = NamedTemporaryFile(delete=False, prefix=prefix, suffix=suffix)
        self._tempfiles.append(ret)
        if is_win():
            ret.close()
        return ret

    def delete_temp_files(self):
        for tempfile in self._tempfiles:
            if os.path.isfile(tempfile.name):
                if is_win():
                    tempfile.close()
                os.unlink(tempfile.name)

        for pattern in ['import_ks_*.err*', 'import_keyspace1_*.err*']:
            for err_file in glob.glob(pattern):
                os.unlink(err_file)

    def prepare(self, nodes=1, partitioner="murmur3", configuration_options=None, tokens=None):
        p = PARTITIONERS[partitioner]
        if not self.cluster.nodelist():
            self.cluster.set_partitioner(p)
            if configuration_options:
                self.cluster.set_configuration_options(values=configuration_options)
            self.cluster.populate(nodes, tokens=tokens).start(wait_for_binary_proto=True)
        else:
            self.assertEqual(self.cluster.partitioner, p, "Cannot reuse cluster: different partitioner")
            self.assertEqual(len(self.cluster.nodelist()), nodes, "Cannot reuse cluster: different number of nodes")
            self.assertIsNone(configuration_options)

        self.node1 = self.cluster.nodelist()[0]
        self.session = self.patient_cql_connection(self.node1)

        self.session.execute('DROP KEYSPACE IF EXISTS ks')
        self.create_ks(self.session, 'ks', 1)

    def all_datatypes_prepare(self):
        self.prepare()

        self.session.execute('CREATE TYPE name_type (firstname text, lastname text)')
        self.session.execute('''
            CREATE TYPE address_type (name frozen<name_type>, number int, street text, phones set<text>)
            ''')

        self.session.execute('''
            CREATE TABLE testdatatype (
                a ascii PRIMARY KEY,
                b bigint,
                c blob,
                d boolean,
                e decimal,
                f double,
                g float,
                h inet,
                i int,
                j text,
                k timestamp,
                l timeuuid,
                m uuid,
                n varchar,
                o varint,
                p list<int>,
                q set<text>,
                r map<timestamp, text>,
                s tuple<int, text, boolean>,
                t frozen<address_type>,
                u frozen<list<list<address_type>>>,
                v frozen<map<map<int,int>,set<text>>>,
                w frozen<set<set<inet>>>,
            )''')

        class Datetime(datetime.datetime):

            def __str__(self):
                return self.strftime(DEFAULT_TIME_FORMAT)

            def __repr__(self):
                return self.strftime(DEFAULT_TIME_FORMAT)

        def maybe_quote(s):
            """
            Return a quoted string representation for strings, unicode and date time parameters,
            otherwise return a string representation of the parameter.
            """
            return "'{}'".format(s) if isinstance(s, (str, unicode, Datetime)) else str(s)

        class ImmutableDict(frozenset):
            iteritems = frozenset.__iter__

            def __repr__(self):
                return '{{{}}}'.format(', '.join(['{}: {}'.format(maybe_quote(t[0]), maybe_quote(t[1]))
                                                  for t in sorted(self)]))

        class ImmutableSet(SortedSet):

            def __repr__(self):
                return '{{{}}}'.format(', '.join([maybe_quote(t) for t in sorted(self._items)]))

        class Name(namedtuple('Name', ('firstname', 'lastname'))):
            __slots__ = ()

            def __repr__(self):
                return "{{firstname: '{}', lastname: '{}'}}".format(self.firstname, self.lastname)

        class Address(namedtuple('Address', ('name', 'number', 'street', 'phones'))):
            __slots__ = ()

            def __repr__(self):
                phones_str = "{{{}}}".format(', '.join(maybe_quote(p) for p in sorted(self.phones)))
                return "{{name: {}, number: {}, street: '{}', phones: {}}}".format(self.name,
                                                                                   self.number,
                                                                                   self.street,
                                                                                   phones_str)

        self.session.cluster.register_user_type('ks', 'name_type', Name)
        self.session.cluster.register_user_type('ks', 'address_type', Address)

        date1 = Datetime(2005, 7, 14, 12, 30, 0, 0, UTC())
        date2 = Datetime(2005, 7, 14, 13, 30, 0, 0, UTC())

        addr1 = Address(Name('name1', 'last1'), 1, 'street 1', ImmutableSet(['1111 2222', '3333 4444']))
        addr2 = Address(Name('name2', 'last2'), 2, 'street 2', ImmutableSet(['5555 6666', '7777 8888']))
        addr3 = Address(Name('name3', 'last3'), 3, 'street 3', ImmutableSet(['1111 2222', '3333 4444']))
        addr4 = Address(Name('name4', 'last4'), 4, 'street 4', ImmutableSet(['5555 6666', '7777 8888']))

        self.data = ('ascii',  # a ascii
                     2 ** 40,  # b bigint
                     bytearray.fromhex('beef'),  # c blob
                     True,  # d boolean
                     Decimal(3.14),  # e decimal
                     2.444,  # f double
                     1.1,  # g float
                     '127.0.0.1',  # h inet
                     25,  # i int
                     'ヽ(´ー｀)ノ',  # j text
                     date1,  # k timestamp
                     uuid1(),  # l timeuuid
                     uuid4(),  # m uuid
                     'asdf',  # n varchar
                     2 ** 65,  # o varint
                     [1, 2, 3],  # p list<int>,
                     ImmutableSet(['3', '2', '1']),  # q set<text>,
                     ImmutableDict([(date1, '1'), (date2, '2')]),  # r map<timestamp, text>,
                     (1, '1', True),  # s tuple<int, text, boolean>,
                     addr1,  # t frozen<address_type>,
                     [[addr1, addr2], [addr3, addr4]],  # u frozen<list<list<address_type>>>,
                     # v frozen<map<map<int,int>,set<text>>>
                     ImmutableDict([(ImmutableDict([(1, 1), (2, 2)]), ImmutableSet(['1', '2', '3']))]),
                     # w frozen<set<set<inet>>>, because of the SortedSet.__lt__() implementation, make sure the
                     # first set is contained in the second set or else they will not sort consistently
                     # and this will cause comparison problems when comparing with csv strings therefore failing
                     # some tests
                     ImmutableSet([ImmutableSet(['127.0.0.1']), ImmutableSet(['127.0.0.1', '127.0.0.2'])])
                     )

    @contextmanager
    def _cqlshlib(self):
        """
        Returns the cqlshlib module, as defined in self.cluster's first node.
        """
        # This method accomplishes its goal by manually adding the library to
        # sys.path, returning the module, then restoring the old path once the
        # context manager exits. This isn't great for maintainability and should
        # be replaced if cqlshlib is made easier to interact with.
        saved_path = list(sys.path)
        cassandra_dir = self.cluster.nodelist()[0].get_install_dir()

        try:
            sys.path = sys.path + [os.path.join(cassandra_dir, 'pylib')]
            import cqlshlib
            yield cqlshlib
        finally:
            sys.path = saved_path

    def assertCsvResultEqual(self, csv_filename, results):
        result_list = list(self.result_to_csv_rows(results))
        processed_results = [[strip_timezone_if_time_string(v) for v in row]
                             for row in result_list]

        csv_file = list(csv_rows(csv_filename))
        processed_csv = [[strip_timezone_if_time_string(v) for v in row]
                         for row in csv_file]

        self.maxDiff = None
        try:
            self.assertItemsEqual(processed_csv, processed_results)
        except Exception as e:
            if len(processed_csv) != len(processed_results):
                warning("Different # of entries. CSV: " + str(len(processed_csv)) +
                        " vs results: " + str(len(processed_results)))
            elif(processed_csv[0] is not None):
                for x in range(0, len(processed_csv[0])):
                    if processed_csv[0][x] != processed_results[0][x]:
                        warning("Mismatch at index: " + str(x))
                        warning("Value in csv: " + str(processed_csv[0][x]))
                        warning("Value in result: " + str(processed_results[0][x]))
            raise e

    def format_for_csv(self, val, time_format, float_precision):
        with self._cqlshlib() as cqlshlib:
            from cqlshlib.formatting import format_value
            try:
                from cqlshlib.formatting import DateTimeFormat
                date_time_format = DateTimeFormat()
                date_time_format.timestamp_format = time_format
            except ImportError:
                date_time_format = None

            #  try:
            #     from cqlshlib.formatting
        encoding_name = 'utf-8'  # codecs.lookup(locale.getpreferredencoding()).name

        # this seems gross but if the blob isn't set to type:bytearray is won't compare correctly
        if isinstance(val, str) and hasattr(self, 'data') and self.data[2] == val:
            var_type = bytearray
            val = bytearray(val)
        else:
            var_type = type(val)

        # different versions use time_format or date_time_format
        # but all versions reject spurious values, so we just use both
        # here
        return format_value(var_type,
                            val,
                            encoding=encoding_name,
                            date_time_format=date_time_format,
                            time_format=time_format,
                            float_precision=float_precision,
                            colormap=DummyColorMap(),
                            nullval=None,
                            decimal_sep=None,
                            thousands_sep=None,
                            boolean_styles=None).strval

    def result_to_csv_rows(self, result, time_format=DEFAULT_TIME_FORMAT, float_precision=DEFAULT_FLOAT_PRECISION):
        """
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        """
        # This has no real dependencies on Tester except that self._cqlshlib has
        # to grab self.cluster's install directory. This should be pulled out
        # into a bare function if cqlshlib is made easier to interact with.
        return [[self.format_for_csv(v, time_format, float_precision) for v in row] for row in result]

    def test_list_data(self):
        """
        Tests the COPY TO command with the list datatype by:

        - populating a table with lists of uuids,
        - exporting the table to a CSV file with COPY TO,
        - comparing the CSV file to the SELECTed contents of the table.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testlist (
                a int PRIMARY KEY,
                b list<uuid>
            )""")

        insert_statement = self.session.prepare("INSERT INTO testlist (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testlist"))

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testlist TO '{name}'".format(name=tempfile.name))

        self.assertCsvResultEqual(tempfile.name, results)

    def test_tuple_data(self):
        """
        Tests the COPY TO command with the tuple datatype by:

        - populating a table with tuples of uuids,
        - exporting the table to a CSV file with COPY TO,
        - comparing the CSV file to the SELECTed contents of the table.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testtuple (
                a int primary key,
                b tuple<uuid, uuid, uuid>
            )""")

        insert_statement = self.session.prepare("INSERT INTO testtuple (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4, n=3)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testtuple"))

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testtuple TO '{name}'".format(name=tempfile.name))

        self.assertCsvResultEqual(tempfile.name, results)

    def non_default_delimiter_template(self, delimiter):
        """
        @param delimiter the delimiter to use for the CSV file.

        Test exporting to CSV files using delimiters other than ',' by:

        - populating a table with integers,
        - exporting to a CSV file, specifying a delimiter, then
        - comparing the contents of the csv file to the SELECTed contents of the table.
        """

        self.prepare()
        self.session.execute("""
            CREATE TABLE testdelimiter (
                a int primary key
            )""")
        insert_statement = self.session.prepare("INSERT INTO testdelimiter (a) VALUES (?)")
        args = [(i,) for i in range(10000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testdelimiter"))

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testdelimiter TO '{name}'".format(name=tempfile.name)
        cmds += " WITH DELIMITER = '{d}'".format(d=delimiter)
        self.node1.run_cqlsh(cmds=cmds)

        self.assertCsvResultEqual(tempfile.name, results)

    def test_colon_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter ':'.
        """
        self.non_default_delimiter_template(':')

    def test_letter_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter 'a'.
        """
        self.non_default_delimiter_template('a')

    def test_number_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter '1'.
        """
        self.non_default_delimiter_template('1')

    def custom_null_indicator_template(self, indicator):
        """
        @param indicator the null indicator to be used in COPY

        A parametrized test that tests COPY with a given null indicator.
        """
        self.all_datatypes_prepare()
        self.session.execute("""
            CREATE TABLE testnullindicator (
                a int primary key,
                b text
            )""")
        insert_non_null = self.session.prepare("INSERT INTO testnullindicator (a, b) VALUES (?, ?)")
        execute_concurrent_with_args(self.session, insert_non_null,
                                     [(1, 'eggs'), (100, 'sausage')])
        insert_null = self.session.prepare("INSERT INTO testnullindicator (a) VALUES (?)")
        execute_concurrent_with_args(self.session, insert_null, [(2,), (200,)])

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testnullindicator TO '{name}'".format(name=tempfile.name)
        cmds += " WITH NULL = '{d}'".format(d=indicator)
        self.node1.run_cqlsh(cmds=cmds)

        results = list(self.session.execute("SELECT a, b FROM ks.testnullindicator"))
        results = [[indicator if value is None else value for value in row]
                   for row in results]

        self.assertCsvResultEqual(tempfile.name, results)

    def test_undefined_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = undefined.
        """
        self.custom_null_indicator_template('undefined')

    def test_null_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = 'null'.
        """
        self.custom_null_indicator_template('null')

    def test_writing_use_header(self):
        """
        Test that COPY can write a CSV with a header by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY WITH
        HEADER = true
        - checking that the contents of the CSV file are the written values plus
        the header.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testheader (
                a int primary key,
                b int
            )""")
        insert_statement = self.session.prepare("INSERT INTO testheader (a, b) VALUES (?, ?)")
        args = [(1, 10), (2, 20), (3, 30)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testheader TO '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        with open(tempfile.name, 'r') as csvfile:
            csv_values = list(csv.reader(csvfile))

        self.assertItemsEqual(csv_values,
                              [['a', 'b'], ['1', '10'], ['2', '20'], ['3', '30']])

    def test_reading_counter(self):
        """
        Test that COPY can read a CSV of COUNTER by:

        - creating a table,
        - writing a CSV with COUNTER data with header,
        - importing the contents of the CSV file using COPY with header,
        - checking that the contents of the table are the written values.
        @jira_ticket CASSANDRA-9043
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcounter (
                a int primary key,
                b counter
            )""")

        tempfile = self.get_temp_file()

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            writer.writeheader()
            for a, b in data:
                writer.writerow({'a': a, 'b': b})

        cmds = "COPY ks.testcounter FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        result = self.session.execute("SELECT * FROM testcounter")
        self.assertItemsEqual(data, rows_to_list(result))

    def test_reading_use_header(self):
        """
        Test that COPY can read a CSV with a header by:

        - creating a table,
        - writing a CSV with a header,
        - importing the contents of the CSV file using COPY WITH HEADER = true,
        - checking that the contents of the table are the written values.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testheader (
                a int primary key,
                b int
            )""")

        tempfile = self.get_temp_file()

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            writer.writeheader()
            for a, b in data:
                writer.writerow({'a': a, 'b': b})

        cmds = "COPY ks.testheader FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        result = self.session.execute("SELECT * FROM testheader")
        self.assertItemsEqual([tuple(d) for d in data],
                              [tuple(r) for r in rows_to_list(result)])

    def test_datetimeformat_round_trip(self):
        """
        @jira_ticket CASSANDRA-10633
        @jira_ticket CASSANDRA-9303

        Test COPY TO and COPY FORM with the time format specified in the WITH option by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY TO WITH DATETIMEFORMAT,
        - checking the time format written to csv.
        - importing the CSV back into the table
        - comparing the table contents before and after the import

        CASSANDRA-9303 renamed TIMEFORMAT to DATETIMEFORMAT
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testdatetimeformat (
                a int primary key,
                b timestamp
            )""")
        insert_statement = self.session.prepare("INSERT INTO testdatetimeformat (a, b) VALUES (?, ?)")
        args = [(1, datetime.datetime(2015, 1, 1, 0o7, 00, 0, 0, UTC())),
                (2, datetime.datetime(2015, 6, 10, 12, 30, 30, 500, UTC())),
                (3, datetime.datetime(2015, 12, 31, 23, 59, 59, 999, UTC()))]
        execute_concurrent_with_args(self.session, insert_statement, args)
        exported_results = list(self.session.execute("SELECT * FROM testdatetimeformat"))

        format = '%Y/%m/%d %H:%M'

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testdatetimeformat TO '{name}'".format(name=tempfile.name)
        cmds += " WITH DATETIMEFORMAT = '{}'".format(format)
        self.node1.run_cqlsh(cmds=cmds)

        with open(tempfile.name, 'r') as csvfile:
            csv_values = list(csv.reader(csvfile))

        self.assertItemsEqual(csv_values,
                              [['1', '2015/01/01 07:00'],
                               ['2', '2015/06/10 12:30'],
                               ['3', '2015/12/31 23:59']])

        self.session.execute("TRUNCATE testdatetimeformat")
        cmds = "COPY ks.testdatetimeformat FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH DATETIMEFORMAT = '{}'".format(format)
        self.node1.run_cqlsh(cmds=cmds)

        imported_results = list(self.session.execute("SELECT * FROM testdatetimeformat"))
        self.assertItemsEqual(self.result_to_csv_rows(exported_results, time_format=format),
                              self.result_to_csv_rows(imported_results, time_format=format))

    @since('3.2')
    def test_reading_with_ttl(self):
        """
        @jira_ticket CASSANDRA-9494
        Test COPY FROM with TTL specified in the WITH option by:

        - creating a table,
        - writing a csv,
        - importing the contents of the CSV file using COPY TO WITH TTL,
        - checking the data has been imported,
        - checking again after TTL * 2 seconds that the data has expired.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testttl (
                a int primary key,
                b int
            )""")

        tempfile = self.get_temp_file()

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            for a, b in data:
                writer.writerow({'a': a, 'b': b})

        self.node1.run_cqlsh(cmds="COPY ks.testttl FROM '{name}' WITH TTL = '5'".format(name=tempfile.name))

        result = rows_to_list(self.session.execute("SELECT * FROM testttl"))
        self.assertItemsEqual(data, result)

        time.sleep(10)

        result = rows_to_list(self.session.execute("SELECT * FROM testttl"))
        self.assertItemsEqual([], result)

    def test_reading_with_skip_and_max_rows(self):
        """
        Test importing a rows from a CSV file with maxrows and skiprows:

        - create a large CSV file via stress write and COPY TO
        - For a specified number of rows:
        -- truncate the table
        -- import the CSV file with max rows set to this number
        -- compare the number of rows imported via select count

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        tempfile = self.get_temp_file()
        stress_table = 'keyspace1.standard1'
        num_file_rows = 10000

        debug('Running stress to generate a large CSV via COPY TO')
        self.node1.stress(['write', 'n={}'.format(num_file_rows), '-rate', 'threads=50'])
        self.node1.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))
        self.assertEqual(num_file_rows, len(open(tempfile.name).readlines()))

        def do_test(num_rows, skip_rows):
            debug('Preparing to test {} max rows and {} skip rows by truncating table'.format(num_rows, skip_rows))
            self.session.execute("TRUNCATE {}".format(stress_table))
            result = rows_to_list(self.session.execute("SELECT * FROM {}".format(stress_table)))
            self.assertItemsEqual([], result)

            debug('Importing {} rows'.format(num_rows))
            self.node1.run_cqlsh(cmds="COPY {} FROM '{}' WITH MAXROWS = '{}' AND SKIPROWS='{}'"
                                 .format(stress_table, tempfile.name, num_rows, skip_rows))

            expected_rows = num_rows if 0 <= num_rows < num_file_rows else num_file_rows
            expected_rows -= min(num_file_rows, max(0, skip_rows))
            self.assertEqual([[expected_rows]],
                             rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table))))
            debug('Imported {} as expected'.format(expected_rows))

        # max rows tests
        do_test(-1, 0)
        do_test(0, 0)
        do_test(1, 0)
        do_test(100, 0)
        do_test(num_file_rows, 0)
        do_test(num_file_rows + 1, 0)

        # skip rows tests
        do_test(-1, 100)
        do_test(num_file_rows, 100)
        do_test(100, 100)
        do_test(num_file_rows, num_file_rows)
        do_test(num_file_rows, num_file_rows + 1)
        do_test(num_file_rows, -1)

    def test_reading_with_skip_cols(self):
        """
        Test importing a CSV file but skipping some columns:

        - create a table
        - create a csv file with all column values
        - import the csv file with skip_columns
        - check only the columns that were not skipped are in the table

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testskipcols (
                a int primary key,
                b int,
                c int,
                d int,
                e int
            )""")

        tempfile = self.get_temp_file()
        data = [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c', 'd', 'e'])
            for a, b, c, d, e in data:
                writer.writerow({'a': a, 'b': b, 'c': c, 'd': d, 'e': e})

        def do_test(skip_cols, expected_results):
            self.session.execute('TRUNCATE ks.testskipcols')
            debug("Importing csv file {} with skipcols '{}'".format(tempfile, skip_cols))
            out, err = self.node1.run_cqlsh(cmds="COPY ks.testskipcols FROM '{}' WITH SKIPCOLS = '{}'"
                                            .format(tempfile.name, skip_cols), return_output=True, cqlsh_options=['--debug'])
            debug(out)
            self.assertItemsEqual(expected_results, rows_to_list(self.session.execute("SELECT * FROM ks.testskipcols")))

        do_test('c, d ,e', [[1, 2, None, None, None], [6, 7, None, None, None]])
        do_test('b,', [[1, None, 3, 4, 5], [6, None, 8, 9, 10]])
        do_test('b', [[1, None, 3, 4, 5], [6, None, 8, 9, 10]])
        do_test('c', [[1, 2, None, 4, 5], [6, 7, None, 9, 10]])
        do_test(',e', [[1, 2, 3, 4, None], [6, 7, 8, 9, None]])
        do_test('e', [[1, 2, 3, 4, None], [6, 7, 8, 9, None]])
        do_test('a,b,c,d,e', [])
        do_test('a,', [])  # primary key cannot be skipped, should refuse to import with an error
        do_test('a', [])  # primary key cannot be skipped, should refuse to import with an error

    def test_reading_counters_with_skip_cols(self):
        """
        Test importing a CSV file for a counter table but skipping some columns:

        - create a table
        - create a csv file with all column values
        - import the csv file with skip_columns
        - check only the columns that were not skipped are in the table

        Because COPY FROM for counters is not idempotent we expect that the values inserted continually increase.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testskipcols (
                a int primary key,
                b counter,
                c counter,
                d counter,
                e counter
            )""")

        tempfile = self.get_temp_file()
        data = [[1, 1, 1, 1, 1], [2, 1, 1, 1, 1]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c', 'd', 'e'])
            for a, b, c, d, e in data:
                writer.writerow({'a': a, 'b': b, 'c': c, 'd': d, 'e': e})

        def do_test(skip_cols, expected_results):
            debug("Importing csv file {} with skipcols '{}'".format(tempfile, skip_cols))
            out, err = self.node1.run_cqlsh(cmds="COPY ks.testskipcols FROM '{}' WITH SKIPCOLS = '{}'"
                                            .format(tempfile.name, skip_cols), return_output=True)
            debug(out)
            self.assertItemsEqual(expected_results, rows_to_list(self.session.execute("SELECT * FROM ks.testskipcols")))

        do_test('c, d ,e', [[1, 1, None, None, None], [2, 1, None, None, None]])
        do_test('b', [[1, 1, 1, 1, 1], [2, 1, 1, 1, 1]])
        do_test('b', [[1, 1, 2, 2, 2], [2, 1, 2, 2, 2]])
        do_test('e', [[1, 2, 3, 3, 2], [2, 2, 3, 3, 2]])

    def test_writing_with_token_boundaries(self):
        """
        Test COPY TO with the begin and end tokens specified in the WITH option by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY TO WITH MINTOKEN AND MAXTOKEN,
        - checking that only the values with the token in the specified range were exported.

        @jira_ticket CASSANDRA-9303
        """
        self._test_writing_with_token_boundaries(10, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(100, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(1000, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(10000, 1000000000000000000, 2000000000000000000)
        self._test_writing_with_token_boundaries(10000, 1000000000000000000, 1000000000000000001)
        self._test_writing_with_token_boundaries(10000, None, 2000000000000000000)
        self._test_writing_with_token_boundaries(10000, 1000000000000000000, None)
        self._test_writing_with_token_boundaries(100, 1000000000000000000, 1000000000000000000)
        self._test_writing_with_token_boundaries(100, 2000000000000000000, 1000000000000000000)

    def _test_writing_with_token_boundaries(self, num_records, begin_token, end_token):
        self.prepare(partitioner="murmur3")
        self.session.execute("CREATE TABLE testtokens(a text primary key)")

        insert_statement = self.session.prepare("INSERT INTO testtokens (a) VALUES (?)")
        execute_concurrent_with_args(self.session, insert_statement, [(str(i),) for i in xrange(num_records)])

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testtokens TO '{}'".format(tempfile.name)
        if begin_token and end_token:
            cmds += "WITH BEGINTOKEN = '{}' AND ENDTOKEN = '{}'".format(begin_token, end_token)
        elif begin_token:
            cmds += "WITH BEGINTOKEN = '{}'".format(begin_token)
        elif end_token:
            cmds += "WITH ENDTOKEN = '{}'".format(end_token)

        self.node1.run_cqlsh(cmds=cmds)

        max_long = 2 ** 63 - 1
        min_long = -max_long - 1
        if not begin_token:
            begin_token = min_long
        if not end_token:
            end_token = max_long

        tokens = [murmur3(str(i)) for i in xrange(num_records)]
        result = sorted([(str(i), tokens[i]) for i in xrange(num_records) if begin_token <= tokens[i] <= end_token])

        with open(tempfile.name, 'r') as csvfile:
            csv_values = sorted([(v[0], tokens[int(v[0])]) for v in csv.reader(csvfile)])

        self.assertItemsEqual(csv_values, result)

    def test_reading_max_parse_errors(self):
        """
        Test that importing a csv file is aborted when we reach the maximum number of parse errors:

        - create a table
        - create a csv file with some invalid rows
        - import the csv file
        - check that we import fewer rows that the total number of valid rows and
        that we display the correct message

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testmaxparseerrors (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()
        num_rows = 10000
        max_parse_errors = 10

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
            for i in xrange(num_rows):
                if i % 2 == 0:
                    writer.writerow({'a': i, 'b': 0, 'c': 'abc'})  # invalid
                else:
                    writer.writerow({'a': i, 'b': 0, 'c': 2.0})  # valid

        debug("Importing csv file {} with {} max parse errors".format(tempfile.name, max_parse_errors))
        out, err = self.node1.run_cqlsh(cmds="COPY ks.testmaxparseerrors FROM '{}' WITH MAXPARSEERRORS='{}'"
                                        .format(tempfile.name, max_parse_errors),
                                        return_output=True, cqlsh_options=['--debug'])

        self.assertIn('Exceeded maximum number of parse errors {}'.format(max_parse_errors), err)
        num_rows_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.testmaxparseerrors"))[0][0]
        self.assertTrue(num_rows_imported < (num_rows / 2))  # less than the maximum number of valid rows in the csv

    def test_reading_max_insert_errors(self):
        """
        Test that importing a csv file is aborted when we reach the maximum number of insert errors:

        - create a table
        - create a csv file with some data
        - fail one batch permanently (via CQLSH_COPY_TEST_FAILURES so that chunk_size rows will fail a # of times higher than
        the maximum number of attempts)
        - import the csv file
        - check that:
         - if chunk_size is bigger than max_insert_errors the import is aborted (we import fewer rows that the total
         number of allowed rows and we display the correct error message)
         - otherwise the import operation completes for all rows except for the failed batch

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testmaxinserterrors (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()
        num_rows = 10000

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
            for i in xrange(num_rows):
                writer.writerow({'a': i, 'b': 0, 'c': 2.0})

        failures = {'failing_batch': {'id': 3, 'failures': 2}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)

        def do_test(max_insert_errors, chunk_size):
            self.session.execute("TRUNCATE ks.testmaxinserterrors")
            num_expected_rows = num_rows - chunk_size  # one batch will fail

            debug("Importing csv file {} with {} max insert errors and chunk size {}"
                  .format(tempfile.name, max_insert_errors, chunk_size))
            # Note: we use one attempt because each attempt counts as a failure
            out, err = self.node1.run_cqlsh(cmds="COPY ks.testmaxinserterrors FROM '{}' WITH MAXINSERTERRORS='{}' "
                                                 "AND CHUNKSIZE='{}' AND MAXATTEMPTS='1'"
                                            .format(tempfile.name, max_insert_errors, chunk_size),
                                            return_output=True, cqlsh_options=['--debug'])

            num_rows_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM ks.testmaxinserterrors"))[0][0]
            if max_insert_errors < chunk_size:
                self.assertIn('Exceeded maximum number of insert errors {}'.format(max_insert_errors), err)
                self.assertTrue(num_rows_imported <= num_expected_rows,
                                "{} < {}".format(num_rows_imported, num_expected_rows))
            else:
                self.assertNotIn('Exceeded maximum number of insert errors {}'.format(max_insert_errors), err)
                self.assertIn('Failed to process {} rows'.format(chunk_size), err)
                self.assertEquals(num_expected_rows, num_rows_imported)

        do_test(50, 100)
        do_test(100, 50)
        do_test(50, 50)

    def test_reading_with_parse_errors(self):
        """
        Test importing a CSV file where not all rows can be parsed:

        - create a table
        - create a csv file with some invalid rows
        - import the csv file
        - check that the valid rows are imported and the invalid rows are saved in a bad file.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testparseerrors (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()

        def do_test(num_chunks, chunk_size, num_failing_per_chunk, err_file):
            invalid_rows = []
            valid_rows = []
            with open(tempfile.name, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
                for i in xrange(num_chunks):
                    for k in xrange(chunk_size):
                        if k < num_failing_per_chunk:  # invalid
                            if i == 0 and k == 0:  # fail on a primary key (only once)
                                writer.writerow({'a': 'bb', 'b': k, 'c': 1.0})
                                invalid_rows.append(['bb', k, '1.0'])
                            else:  # fail on a value
                                writer.writerow({'a': i, 'b': k, 'c': 'abc'})
                                invalid_rows.append([i, k, 'abc'])
                        else:
                            writer.writerow({'a': i, 'b': k, 'c': 2.0})  # valid
                            valid_rows.append([i, k, 2.0])

            err_file_name = err_file.name if err_file else 'import_ks_testparseerrors.err'
            self.session.execute("TRUNCATE testparseerrors")

            debug("Importing csv file {} with err_file {} and {}/{}/{}"
                  .format(tempfile.name, err_file_name, num_chunks, chunk_size, num_failing_per_chunk))
            cmd = "COPY ks.testparseerrors FROM '{}' WITH CHUNKSIZE={}".format(tempfile.name, chunk_size)
            if err_file:
                cmd += " AND ERRFILE='{}'".format(err_file.name)
            self.node1.run_cqlsh(cmds=cmd)

            debug('Sorting')
            results = sorted(rows_to_list(self.session.execute("SELECT * FROM ks.testparseerrors")))
            debug('Checking valid rows')
            self.assertItemsEqual(valid_rows, results)
            debug('Checking invalid rows')
            self.assertCsvResultEqual(err_file_name, invalid_rows)

        do_test(100, 2, 1, self.get_temp_file())
        do_test(10, 50, 1, self.get_temp_file())
        do_test(10, 100, 10, self.get_temp_file())
        do_test(10, 100, 100, self.get_temp_file())

        # at least two default files to make sure old default err file gets renamed to .YYYYMMDD_HHMMSS
        do_test(100, 2, 1, None)
        do_test(10, 50, 1, None)

    def test_reading_with_wrong_number_of_columns(self):
        """
        Test importing a CSV file where not all rows have the correct number of columns:

        - create a table
        - create a csv file with some invalid rows
        - import the csv file
        - check that the valid rows are imported and the invalid rows are saved in a bad file.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testwrongnumcols (
                a int,
                b int,
                c float,
                d float,
                e float,
                PRIMARY KEY (a, b)
            )""")

        tempfile = self.get_temp_file()
        err_file = self.get_temp_file()

        invalid_rows = []
        valid_rows = []
        with open(tempfile.name, 'w') as csvfile:  # c, d is missing
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'e'])
            writer.writerow({'a': 0, 'b': 0, 'e': 1.0})
            invalid_rows.append([0, 0, '1.0'])

            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c', 'd', 'e'])
            for i in xrange(1, 100):
                writer.writerow({'a': i, 'b': i, 'c': 2.0, 'd': 3.0, 'e': 4.0})
                valid_rows.append([i, i, 2.0, 3.0, 4.0])

        debug("Importing csv file {} with err_file {}".format(tempfile.name, err_file.name))
        cmd = "COPY ks.testwrongnumcols FROM '{}' WITH ERRFILE='{}'".format(tempfile.name, err_file.name)
        self.node1.run_cqlsh(cmds=cmd)

        debug('Sorting')
        results = sorted(rows_to_list(self.session.execute("SELECT * FROM ks.testwrongnumcols")))
        debug('Checking valid rows')
        self.assertItemsEqual(valid_rows, results)
        debug('Checking invalid rows')
        self.assertCsvResultEqual(err_file.name, invalid_rows)

        os.unlink(err_file.name)

    def test_reading_with_multiple_files(self):
        """
        Test importing multiple CSV files

        - create a table
        - create a several csv files
        - import the csv files
        - check that all rows were imported

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testmultifiles (
                a int,
                b int,
                c float,
                PRIMARY KEY (a, b)
            )""")

        num_rows_per_file = 100
        num_files = 10
        tempfiles = []

        for i in xrange(num_files):
            tempfiles.append(self.get_temp_file(prefix='testreadmult{}'.format(i), suffix='.csv'))

        for i in xrange(num_files):
            with open(tempfiles[i].name, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['a', 'b', 'c'])
                for k in xrange(num_rows_per_file):
                    writer.writerow({'a': i, 'b': k, 'c': 2.0})

        def import_and_check(temp_files_str):
            self.session.execute("TRUNCATE testmultifiles")

            debug("Importing csv files {}".format(temp_files_str))
            self.node1.run_cqlsh(cmds="COPY ks.testmultifiles FROM '{}'".format(temp_files_str))

            self.assertEqual([[num_rows_per_file * len(tempfiles)]],
                             rows_to_list(self.session.execute("SELECT COUNT(*) FROM testmultifiles")))

        import_and_check(','.join([tempfile.name for tempfile in tempfiles]))
        import_and_check(os.path.join(gettempdir(), 'testreadmult*.csv'))
        import_and_check(','.join([os.path.join(gettempdir(), 'testreadmult[0-4]*.csv'),
                                   os.path.join(gettempdir(), 'testreadmult[5-9]*.csv')]))

    def test_writing_with_max_output_size(self):
        """
        Test writing to multiple CSV files:

        - create a table and populate it with some data
        - export the data with maxoutputsize
        - check that the correct number of CSV files has been created and that
          they have the expected number of lines

        @jira_ticket CASSANDRA-9303
        """
        num_records = 10000
        stress_table = 'keyspace1.standard1'

        self.prepare()
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        def do_test(max_size, header):
            tempfile = self.get_temp_file(prefix='testwritemult', suffix='.csv')
            debug('Exporting to csv file: {} with max size {} and header {}'
                  .format(tempfile.name, max_size, header))
            cmd = "COPY {} TO '{}' WITH MAXOUTPUTSIZE='{}'".format(stress_table, tempfile.name, max_size)
            if header:
                cmd += " AND HEADER='True'"
            self.node1.run_cqlsh(cmds=cmd)

            output_files = glob.glob(os.path.join(gettempdir(), 'testwritemult*.csv*'))
            num_lines = []
            for f in output_files:
                num_lines.append(len(open(os.path.join(gettempdir(), f)).readlines()))
                os.unlink(f)

            num_expected_files = num_records / max_size if num_records % max_size == 0 else (num_records / max_size + 1)
            self.assertEquals(num_expected_files, len(output_files))
            self.assertEquals(num_records + 1 if header else num_records, sum(num_lines))

            for i, n in enumerate(sorted(num_lines, reverse=True)):
                if i < num_records / max_size:
                    num_expected_lines = max_size + 1 if i == 0 and header else max_size
                    self.assertEquals(num_expected_lines, n)
                else:
                    self.assertEquals(num_records % max_size, n)

        do_test(1000, False)
        do_test(1000, True)
        do_test(900, False)
        do_test(500, False)
        do_test(100, False)

    def test_explicit_column_order_writing(self):
        """
        Test that COPY can write to a CSV file when the order of columns is
        explicitly specified by:

        - creating a table,
        - COPYing to a CSV file with columns in a different order than they
        appeared in the CREATE TABLE statement,
        - writing a CSV file with the columns in that order, and
        - asserting that the two CSV files contain the same values.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testorder (
                a int primary key,
                b int,
                c text
            )""")

        data = [[1, 20, 'ham'], [2, 40, 'eggs'],
                [3, 60, 'beans'], [4, 80, 'toast']]
        insert_statement = self.session.prepare("INSERT INTO testorder (a, b, c) VALUES (?, ?, ?)")
        execute_concurrent_with_args(self.session, insert_statement, data)

        tempfile = self.get_temp_file()

        self.node1.run_cqlsh(
            "COPY ks.testorder (a, c, b) TO '{name}'".format(name=tempfile.name))

        reference_file = self.get_temp_file()
        with open(reference_file.name, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])

        assert_csvs_items_equal(tempfile.name, reference_file.name)

    def test_explicit_column_order_reading(self):
        """
        Test that COPY can write to a CSV file when the order of columns is
        explicitly specified by:

        - creating a table,
        - writing a CSV file containing columns with the same types as the
        table, but in a different order,
        - COPYing the contents of that CSV into the table by specifying the
        order of the columns,
        - asserting that the values in the CSV file match those in the table.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testorder (
                a int primary key,
                b text,
                c int
            )""")

        data = [[1, 20, 'ham'], [2, 40, 'eggs'],
                [3, 60, 'beans'], [4, 80, 'toast']]

        tempfile = self.get_temp_file()
        write_rows_to_csv(tempfile.name, data)

        self.node1.run_cqlsh(
            "COPY ks.testorder (a, c, b) FROM '{name}'".format(name=tempfile.name))

        results = list(self.session.execute("SELECT * FROM testorder"))
        reference_file = self.get_temp_file()
        with open(reference_file.name, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])

        self.assertCsvResultEqual(reference_file.name, results)

    def quoted_column_names_reading_template(self, specify_column_names):
        """
        @param specify_column_names if truthy, specify column names in COPY statement
        A parameterized test. Tests that COPY can read from a CSV file into a
        table with quoted column names by:

        - creating a table with quoted column names,
        - writing test data to a CSV file,
        - COPYing that CSV file into the table, explicitly naming columns, and
        - asserting that the CSV file and the table contain the same data.

        If the specify_column_names parameter is truthy, the COPY statement
        explicitly names the columns.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )""")

        data = [[1, 'no'], [2, 'Yes'],
                [3, 'True'], [4, 'false']]

        tempfile = self.get_temp_file()
        write_rows_to_csv(tempfile.name, data)

        stmt = ("""COPY ks.testquoted ("IdNumber", "select") FROM '{name}'"""
                if specify_column_names else
                """COPY ks.testquoted FROM '{name}'""").format(name=tempfile.name)

        self.node1.run_cqlsh(stmt)

        results = list(self.session.execute("SELECT * FROM testquoted"))
        self.assertCsvResultEqual(tempfile.name, results)

    def test_quoted_column_names_reading_specify_names(self):
        """
        Use quoted_column_names_reading_template to test reading from a CSV file
        into a table with quoted column names, explicitly specifying the column
        names in the COPY statement.
        """
        self.quoted_column_names_reading_template(specify_column_names=True)

    def test_quoted_column_names_reading_dont_specify_names(self):
        """
        Use quoted_column_names_reading_template to test reading from a CSV file
        into a table with quoted column names, without explicitly specifying the
        column names in the COPY statement.
        """
        self.quoted_column_names_reading_template(specify_column_names=False)

    def quoted_column_names_writing_template(self, specify_column_names):
        """
        @param specify_column_names if truthy, specify column names in COPY statement
        A parameterized test. Test that COPY can write to a table with quoted
        column names by:

        - creating a table with quoted column names,
        - inserting test data into that table,
        - COPYing that table into a CSV file into the table, explicitly naming columns,
        - writing that test data to a CSV file,
        - asserting that the two CSV files contain the same rows.

        If the specify_column_names parameter is truthy, the COPY statement
        explicitly names the columns.
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )""")

        data = [[1, 'no'], [2, 'Yes'],
                [3, 'True'], [4, 'false']]
        insert_statement = self.session.prepare("""INSERT INTO testquoted ("IdNumber", "select") VALUES (?, ?)""")
        execute_concurrent_with_args(self.session, insert_statement, data)

        tempfile = self.get_temp_file()
        stmt = ("""COPY ks.testquoted ("IdNumber", "select") TO '{name}'"""
                if specify_column_names else
                """COPY ks.testquoted TO '{name}'""").format(name=tempfile.name)
        self.node1.run_cqlsh(stmt)

        reference_file = self.get_temp_file()
        write_rows_to_csv(reference_file.name, data)

        assert_csvs_items_equal(tempfile.name, reference_file.name)

    def test_quoted_column_names_writing_specify_names(self):
        self.quoted_column_names_writing_template(specify_column_names=True)

    def test_quoted_column_names_writing_dont_specify_names(self):
        self.quoted_column_names_writing_template(specify_column_names=False)

    def data_validation_on_read_template(self, data, expected_err='Failed to import'):
        """
        @param data - the data to be written to the csv file
        @param expected_err - the error we expect or None if the test should succeed

        Test that reading from CSV files fails when there is a type mismatch
        between the value being loaded and the type of the column or when data is missing
        in the file. Perform the following:

        - create a table,
        - write a CSV file containing the data passed in as parameter
        - COPY that csv file into the table

        If expected_err is not None, this test will succeed when the COPY command fails and it
        returns a matching error.
        If expected_err is None, this test will succeed when the COPY command prints
        no errors and the table matches the loaded CSV file.

        @jira_ticket CASSANDRA-9302
        @jira_ticket CASSANDRA-10854
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testvalidate (
                a int,
                b int,
                c int,
                PRIMARY KEY(a, b)
            )""")

        tempfile = self.get_temp_file()
        debug('Writing {}'.format(tempfile.name))
        write_rows_to_csv(tempfile.name, data)

        cmd = """COPY ks.testvalidate (a, b, c) FROM '{name}'""".format(name=tempfile.name)
        out, err = self.node1.run_cqlsh(cmd, return_output=True)
        results = list(self.session.execute("SELECT * FROM testvalidate"))

        if expected_err:
            self.assertIn(expected_err, err)
            self.assertFalse(results)
        else:
            self.assertFalse(err)
            self.assertCsvResultEqual(tempfile.name, results)

    def test_read_valid_data(self):
        """
        Use data_validation_on_read_template to test COPYing an int value from a
        CSV into an int column. This test exists to make sure the parameterized
        test works.
        """
        # make sure the template works properly
        self.data_validation_on_read_template([[1, 1, 2]], expected_err=None)

    def test_read_invalid_float(self):
        """
        Use data_validation_on_read_template to test COPYing a float value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template([[1, 1, 2.14]])

    def test_read_invalid_uuid(self):
        """
        Use data_validation_on_read_template to test COPYing a uuid value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template([[1, 1, uuid4()]])

    def test_read_invalid_text(self):
        """
        Use data_validation_on_read_template to test COPYing a text value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template([[1, 1, 'test']])

    def test_read_missing_partition_key(self):
        """
        Use data_validation_on_read_template to test COPYing an empty partition key.
        @jira_ticket CASSANDRA-10854
        """
        self.data_validation_on_read_template([['', 1, 'test']],
                                              expected_err="Failed to import 1 rows: ParseError - "
                                                           "Cannot insert null value for primary key column")

    def test_read_missing_clustering_key(self):
        """
        Use data_validation_on_read_template to test COPYing an empty clustering key.
        @jira_ticket CASSANDRA-10854
        """
        self.data_validation_on_read_template([[1, '', 'test']],
                                              expected_err="Failed to import 1 rows: ParseError - "
                                                           "Cannot insert null value for primary key column")

    def test_all_datatypes_write(self):
        """
        Test that, after COPYing a table containing all CQL datatypes to a CSV
        file, that the table contains the same values as the CSV by:

        - creating and populating a table containing all datatypes,
        - COPYing the contents of that table to a CSV file, and
        - asserting that the CSV file contains the same data as the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype TO '{name}'".format(name=tempfile.name))

        results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.assertCsvResultEqual(tempfile.name, results)

    def test_all_datatypes_read(self):
        """
        Test that, after COPYing a CSV file to a table containing all CQL
        datatypes, that the table contains the same values as the CSV by:

        - creating a table containing all datatypes,
        - writing a corresponding CSV file containing each datatype,
        - COPYing the CSV file into the table, and
        - asserting that the CSV file contains the same data as the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        tempfile = self.get_temp_file()

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            # serializing blob bytearray in friendly format
            data_set = list(self.data)
            data_set[2] = '0x{}'.format(''.join('%02x' % c for c in self.data[2]))
            writer.writerow(data_set)

        debug('Importing from csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype FROM '{name}'".format(name=tempfile.name))

        results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.assertCsvResultEqual(tempfile.name, results)

    def test_all_datatypes_round_trip(self):
        """
        Test that a table containing all CQL datatypes successfully round-trips
        to and from a CSV file via COPY by:

        - creating and populating a table containing every datatype,
        - COPYing that table to a CSV file,
        - SELECTing the contents of the table,
        - TRUNCATEing the table,
        - COPYing the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype TO '{name}'".format(name=tempfile.name))

        exported_results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.session.execute('TRUNCATE ks.testdatatype')

        self.node1.run_cqlsh(cmds="COPY ks.testdatatype FROM '{name}'".format(name=tempfile.name))

        imported_results = list(self.session.execute("SELECT * FROM testdatatype"))

        assert len(imported_results) == 1

        self.assertEqual(exported_results, imported_results)

    def test_boolstyle_round_trip(self):
        """
        Test that a CSV file with booleans in a different style successfully round-trips
        via COPY by:

        - creating and populating a table containing boolean values,
        - copying that table to a CSV file,
        - checking the contents of the CSV,
        - truncating the table,
        - copying the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.

        @jira_ticket CASSANDRA-9303
        """
        def do_round_trip(trueval, falseval, invalid=False):
            debug('Exporting to csv file: {} with bool style {},{}'.format(tempfile.name, trueval, falseval))
            _, err = self.node1.run_cqlsh(cmds="COPY ks.testbooleans TO '{}' WITH BOOLSTYLE='{}, {}'"
                                          .format(tempfile.name, trueval, falseval), return_output=True)
            if invalid:
                expected_err = "Invalid boolean styles [{}, {}]".format(
                    ', '.join(["'{}'".format(s.strip()) for s in trueval.split(',')]),
                    ', '.join(["'{}'".format(s.strip()) for s in falseval.split(',')]))
                self.assertIn(expected_err, err)
                return

            self.assertItemsEqual([['0', falseval], ['1', trueval]], list(csv_rows(tempfile.name)))
            exported_results = list(self.session.execute("SELECT * FROM testbooleans"))

            debug('Importing from csv file: {}'.format(tempfile.name))
            self.session.execute('TRUNCATE ks.testbooleans')
            self.node1.run_cqlsh(cmds="COPY ks.testbooleans FROM '{}' WITH BOOLSTYLE='{}, {}'"
                                 .format(tempfile.name, trueval, falseval))

            imported_results = list(self.session.execute("SELECT * FROM testbooleans"))
            assert len(imported_results) == 2
            self.assertEqual(exported_results, imported_results)

        self.prepare()
        self.session.execute("""
            CREATE TABLE testbooleans (
                a int PRIMARY KEY,
                b boolean
            )""")

        insert_statement = self.session.prepare("INSERT INTO testbooleans (a, b) VALUES (?, ?)")
        self.session.execute(insert_statement, [0, False])
        self.session.execute(insert_statement, [1, True])
        tempfile = self.get_temp_file()

        do_round_trip('True', 'False')
        do_round_trip('TRUE', 'FALSE')
        do_round_trip('yes', 'no')
        do_round_trip('1', '0')
        do_round_trip('TRUE', 'no')
        do_round_trip('True', '0')

        do_round_trip('TRUE', 'TRUE', invalid=True)
        do_round_trip('TRUE', '', invalid=True)
        do_round_trip('', 'FALSE', invalid=True)
        do_round_trip('', '', invalid=True)
        do_round_trip('yes, no', 'maybe', invalid=True)

    def test_number_separators_round_trip(self):
        """
        Test that a CSV file containing numbers with decimal and thousands separators in a different format
        successfully round-trips via COPY by:

        - creating and populating a table containing a numbers,
        - copying that table to a CSV file,
        - checking the contents of the CSV,
        - truncating the table,
        - copying the written CSV file back into the table, and
        - asserting that the previously selected contents of the table match the
        current contents of the table.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()

        if self.cluster.version() < '2.2':
            self.session.execute("""
                CREATE TABLE testnumberseps (
                    a int PRIMARY KEY,
                    b int,
                    c bigint,
                    d varint,
                    e decimal,
                    f float,
                    g double
                )""")

            insert_statement = self.session.prepare("INSERT INTO testnumberseps (a, b, c, d, e, f, g)"
                                                    " VALUES (?, ?, ?, ?, ?, ?, ?)")
            self.session.execute(insert_statement, [0, 10, 10, 10, Decimal(10), 10, 10])
            self.session.execute(insert_statement, [1, 1000, 1000, 1000, Decimal(5.5), 5.5, 5.12345678])
            self.session.execute(insert_statement, [2, 1000000, 1000000, 1000000, Decimal("0.001"), 0.001, 0.001])
            self.session.execute(insert_statement, [3, 1000000005, 1000000005, 1000000005,
                                                    Decimal("1234.56"), 1234.56, 123456789.56])
            self.session.execute(insert_statement, [4, -1000000005, -1000000005, -1000000005,
                                                    Decimal("-1234.56"), -1234.56, -1234.56])
            self.session.execute(insert_statement, [1000000, 0, 0, 0, Decimal(0), 0, 0])

            # comma as thousands sep and dot as decimal sep
            expected_vals_usual = [
                ['0', '10', '10', '10', '10', '10', '10'],
                ['1', '1,000', '1,000', '1,000', '5.5', '5.5', '5.12346'],
                ['2', '1,000,000', '1,000,000', '1,000,000', '0.001', '0.001', '0.001'],
                ['3', '1,000,000,005', '1,000,000,005', '1,000,000,005', '1,234.56', '1,234.56006', '123,456,789.56'],
                ['4', '-1,000,000,005', '-1,000,000,005', '-1,000,000,005', '-1,234.56', '-1,234.56006', '-1,234.56'],
                ['1,000,000', '0', '0', '0', '0', '0', '0']
            ]

            # dot as thousands sep and comma as decimal sep
            expected_vals_inverted = [
                ['0', '10', '10', '10', '10', '10', '10'],
                ['1', '1.000', '1.000', '1.000', '5,5', '5,5', '5,12346'],
                ['2', '1.000.000', '1.000.000', '1.000.000', '0,001', '0,001', '0,001'],
                ['3', '1.000.000.005', '1.000.000.005', '1.000.000.005', '1.234,56', '1.234,56006', '123.456.789,56'],
                ['4', '-1.000.000.005', '-1.000.000.005', '-1.000.000.005', '-1.234,56', '-1.234,56006', '-1.234,56'],
                ['1.000.000', '0', '0', '0', '0', '0', '0']
            ]

        else:
            self.session.execute("""
                CREATE TABLE testnumberseps (
                    a int PRIMARY KEY,
                    b tinyint,
                    c smallint,
                    d int,
                    e bigint,
                    f varint,
                    g decimal,
                    h float,
                    i double
                )""")

            insert_statement = self.session.prepare("INSERT INTO testnumberseps (a, b, c, d, e, f, g, h, i)"
                                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
            self.session.execute(insert_statement, [0, 10, 10, 10, 10, 10, Decimal(10), 10, 10])
            self.session.execute(insert_statement, [1, 127, 255, 1000, 1000, 1000, Decimal(5.5), 5.5, 5.12345678])
            self.session.execute(insert_statement, [2, 127, 255, 1000000, 1000000, 1000000,
                                                    Decimal("0.001"), 0.001, 0.001])
            self.session.execute(insert_statement, [3, 127, 255, 1000000005, 1000000005, 1000000005,
                                                    Decimal("1234.56"), 1234.56, 123456789.56])
            self.session.execute(insert_statement, [4, 127, 255, -1000000005, -1000000005, -1000000005,
                                                    Decimal("-1234.56"), -1234.56, -1234.56])
            self.session.execute(insert_statement, [1000000, 0, 0, 0, 0, 0, Decimal(0), 0, 0])

            # comma as thousands sep and dot as decimal sep
            expected_vals_usual = [
                ['0', '10', '10', '10', '10', '10', '10', '10', '10'],
                ['1', '127', '255', '1,000', '1,000', '1,000', '5.5', '5.5', '5.12346'],
                ['2', '127', '255', '1,000,000', '1,000,000', '1,000,000', '0.001', '0.001', '0.001'],
                ['3', '127', '255', '1,000,000,005', '1,000,000,005', '1,000,000,005',
                 '1,234.56', '1,234.56006', '123,456,789.56'],
                ['4', '127', '255', '-1,000,000,005', '-1,000,000,005', '-1,000,000,005',
                 '-1,234.56', '-1,234.56006', '-1,234.56'],
                ['1,000,000', '0', '0', '0', '0', '0', '0', '0', '0']
            ]

            # dot as thousands sep and comma as decimal sep
            expected_vals_inverted = [
                ['0', '10', '10', '10', '10', '10', '10', '10', '10'],
                ['1', '127', '255', '1.000', '1.000', '1.000', '5,5', '5,5', '5,12346'],
                ['2', '127', '255', '1.000.000', '1.000.000', '1.000.000', '0,001', '0,001', '0,001'],
                ['3', '127', '255', '1.000.000.005', '1.000.000.005', '1.000.000.005',
                 '1.234,56', '1.234,56006', '123.456.789,56'],
                ['4', '127', '255', '-1.000.000.005', '-1.000.000.005', '-1.000.000.005',
                 '-1.234,56', '-1.234,56006', '-1.234,56'],
                ['1.000.000', '0', '0', '0', '0', '0', '0', '0', '0']
            ]

        tempfile = self.get_temp_file()

        def do_test(expected_vals, thousands_sep, decimal_sep):
            debug('Exporting to csv file: {} with thousands_sep {} and decimal_sep {}'
                  .format(tempfile.name, thousands_sep, decimal_sep))
            self.node1.run_cqlsh(cmds="COPY ks.testnumberseps TO '{}' WITH THOUSANDSSEP='{}' AND DECIMALSEP='{}'"
                                 .format(tempfile.name, thousands_sep, decimal_sep))

            exported_results = list(self.session.execute("SELECT * FROM testnumberseps"))
            self.maxDiff = None
            self.assertItemsEqual(expected_vals, list(csv_rows(tempfile.name)))

            debug('Importing from csv file: {} with thousands_sep {} and decimal_sep {}'
                  .format(tempfile.name, thousands_sep, decimal_sep))
            self.session.execute('TRUNCATE ks.testnumberseps')
            self.node1.run_cqlsh(cmds="COPY ks.testnumberseps FROM '{}' WITH THOUSANDSSEP='{}' AND DECIMALSEP='{}'"
                                 .format(tempfile.name, thousands_sep, decimal_sep))

            imported_results = list(self.session.execute("SELECT * FROM testnumberseps"))
            self.assertEqual(len(expected_vals), len(imported_results))
            self.assertEqual(self.result_to_csv_rows(exported_results),  # we format as if we were comparing to csv
                             self.result_to_csv_rows(imported_results))  # to overcome loss of precision in the import

        do_test(expected_vals_usual, ',', '.')
        do_test(expected_vals_inverted, '.', ',')

    def test_round_trip_with_num_processes(self):
        """
        Test exporting a large number of rows into a csv file with a fixed number of child processes.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()

        num_records = 10000
        num_processes = 4
        stress_table = 'keyspace1.standard1'

        debug('Running stress without any user profile')
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {}'.format(tempfile.name))
        out, _ = self.node1.run_cqlsh(cmds="COPY {} TO '{}' WITH NUMPROCESSES='{}'"
                                      .format(stress_table, tempfile.name, num_processes),
                                      return_output=True)
        debug(out)
        self.assertIn('Using {} child processes'.format(num_processes), out)
        self.assertEqual(num_records, len(open(tempfile.name).readlines()))

        self.session.execute("TRUNCATE {}".format(stress_table))
        debug('Importing from csv file: {}'.format(tempfile.name))
        out, _ = self.node1.run_cqlsh(cmds="COPY {} FROM '{}' WITH NUMPROCESSES='{}'"
                                      .format(stress_table, tempfile.name, num_processes),
                                      return_output=True)
        debug(out)
        self.assertIn('Using {} child processes'.format(num_processes), out)
        self.assertEqual([[num_records]], rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}"
                                                                            .format(stress_table))))

    def test_round_trip_with_rate_file(self):
        """
        Test a round trip with a large number of rows and a rate file. Make sure the rate file contains
        output statistics

        @jira_ticket CASSANDRA-9303
        """
        num_rows = 200000
        report_frequency = 0.1  # every 100 milliseconds
        stress_table = 'keyspace1.standard1'
        ratefile = self.get_temp_file()
        tempfile = self.get_temp_file()

        def check_rate_file():
            # check that the rate file has at least 10 lines (given that the report
            # frequency is every 100 milliseconds this should be the number of lines written in 1 second)
            # and that the last line indicates all rows were processed
            lines = [line.rstrip('\n') for line in open(ratefile.name)]
            self.assertTrue(len(lines) >= 10, "Expected at least 10 lines but got {} lines".format(len(lines)))
            self.assertTrue(lines[-1].startswith('Processed: {} rows;'.format(num_rows)))

        self.prepare()

        debug('Running stress')
        self.node1.stress(['write', 'n={}'.format(num_rows), '-rate', 'threads=50'])

        debug('Exporting to csv file: {}'.format(tempfile.name))
        self.node1.run_cqlsh(cmds="COPY {} TO '{}' WITH RATEFILE='{}' AND REPORTFREQUENCY='{}'"
                             .format(stress_table, tempfile.name, ratefile.name, report_frequency))

        # check all records were exported
        self.assertEqual(num_rows, len(open(tempfile.name).readlines()))

        check_rate_file()

        # clean-up
        os.unlink(ratefile.name)
        self.session.execute("TRUNCATE {}".format(stress_table))

        debug('Importing from csv file: {}'.format(tempfile.name))
        self.node1.run_cqlsh(cmds="COPY {} FROM '{}' WITH RATEFILE='{}' AND REPORTFREQUENCY='{}'"
                             .format(stress_table, tempfile.name, ratefile.name, report_frequency))

        # check all records were imported
        self.assertEqual([[num_rows]], rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}"
                                                                         .format(stress_table))))

        check_rate_file()

    def test_copy_options_from_config_file(self):
        """
        Test that we can specify configuration options in a config file, optionally using multiple sections,
        and that we can still overwrite options from the command line.
        We must pass the debug flag --debug to cqlsh so that we can retrieve options from stdout in order to check,
        see maybe_read_config_file() in copy.py.

        @jira_ticket CASSANDRA-9303
        """
        tempfile = self.get_temp_file()
        self.prepare(nodes=1)

        debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n=1K', '-rate', 'threads=50'])

        def create_config_file(config_lines):
            config_file = self.get_temp_file()
            debug('Creating config file {}'.format(config_file.name))

            with open(config_file.name, 'wb') as config:
                for line in config_lines:
                    config.write(line + os.linesep)
                config.close()

            return config_file.name

        def extract_options(out):
            prefix = 'Using options: '
            for l in out.split('\n'):
                if l.startswith(prefix):
                    return l[len(prefix):].strip().strip("'").replace("'", "\"")
            return ''

        def check_options(out, expected_options):
            opts = extract_options(out)
            debug('Options: {}'.format(opts))
            d = json.loads(opts)
            for k, v in expected_options:
                self.assertEqual(v, d[k])

        def do_test(config_lines, expected_options):
            config_file = create_config_file(config_lines)

            cmd = "COPY {} {} '{}'".format(stress_table, direction, tempfile.name)
            if not use_default:
                cmd += " WITH CONFIGFILE = '{}'".format(config_file)

            cqlsh_options = ['--debug']
            if use_default:
                cqlsh_options.append('--cqlshrc={}'.format(config_file))

            debug('{} with options {}'.format(cmd, cqlsh_options))
            out, _ = self.node1.run_cqlsh(cmds=cmd, return_output=True, cqlsh_options=cqlsh_options)
            debug(out)
            check_options(out, expected_options)

        for use_default in [True, False]:
            for direction in ['TO', 'FROM']:
                do_test(['[copy]', 'header = True', 'maxattempts = 10'],
                        [('header', 'True'), ('maxattempts', '10')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy:{}]'.format(stress_table), 'maxattempts = 9'],
                        [('header', 'True'), ('maxattempts', '9')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy-from]', 'maxattempts = 9',
                         '[copy-to]', 'maxattempts = 8'],
                        [('header', 'True'), ('maxattempts', '8' if direction == 'TO' else '9')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy-from]', 'maxattempts = 9',
                         '[copy-to]', 'maxattempts = 8',
                         '[copy:{}]'.format(stress_table), 'maxattempts = 7'],
                        [('header', 'True'), ('maxattempts', '7')])

                do_test(['[copy]', 'header = True', 'maxattempts = 10',
                         '[copy-from]', 'maxattempts = 9',
                         '[copy-to]', 'maxattempts = 8',
                         '[copy:{}]'.format(stress_table), 'maxattempts = 7',
                         '[copy-from:{}]'.format(stress_table), 'maxattempts = 6',
                         '[copy-to:{}]'.format(stress_table), 'maxattempts = 5'],
                        [('header', 'True'), ('maxattempts', '5' if direction == 'TO' else '6')])

    def test_wrong_number_of_columns(self):
        """
        Test that a COPY statement will fail when trying to import from a CSV
        file with the wrong number of columns by:

        - creating a table with a single column,
        - writing a CSV file with two columns,
        - attempting to COPY the CSV file into the table, and
        - asserting that the COPY operation failed.

        @jira_ticket CASSANDRA-9302
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcolumns (
                a int PRIMARY KEY,
                b int
            )""")

        data = [[1, 2, 3]]
        tempfile = self.get_temp_file()
        write_rows_to_csv(tempfile.name, data)

        debug('Importing from csv file: {name}'.format(name=tempfile.name))
        out, err = self.node1.run_cqlsh("COPY ks.testcolumns FROM '{name}'".format(name=tempfile.name),
                                        return_output=True)

        self.assertFalse(self.session.execute("SELECT * FROM testcolumns"))
        self.assertIn('Failed to import', err)

    def _test_round_trip(self, nodes, partitioner, num_records=10000):
        """
        Test a simple round trip of a small CQL table to and from a CSV file via
        COPY.

        - creating and populating a table,
        - COPYing that table to a CSV file,
        - SELECTing the contents of the table,
        - TRUNCATEing the table,
        - COPYing the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.
        """
        self.prepare(nodes=nodes, partitioner=partitioner)
        self.session.execute("""
            CREATE TABLE testcopyto (
                a text PRIMARY KEY,
                b int,
                c float,
                d uuid
            )""")

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(str(i), i, float(i) + 0.5, uuid4()) for i in range(num_records)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testcopyto"))

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {}'.format(tempfile.name))
        out = self.node1.run_cqlsh(cmds="COPY ks.testcopyto TO '{}'".format(tempfile.name), return_output=True)
        debug(out)

        # check all records were exported
        self.assertEqual(num_records, sum(1 for line in open(tempfile.name)))

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        debug('Importing from csv file: {}'.format(tempfile.name))
        out = self.node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '{}'".format(tempfile.name), return_output=True)
        debug(out)

        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        self.assertEqual(results, new_results)

    @freshCluster()
    def test_round_trip_murmur3(self):
        self._test_round_trip(nodes=3, partitioner="murmur3")

    @freshCluster()
    def test_round_trip_random(self):
        self._test_round_trip(nodes=3, partitioner="random")

    @freshCluster()
    def test_round_trip_order_preserving(self):
        self._test_round_trip(nodes=3, partitioner="order")

    @freshCluster()
    def test_round_trip_byte_ordered(self):
        self._test_round_trip(nodes=3, partitioner="byte")

    @freshCluster()
    def test_source_copy_round_trip(self):
        """
        Like test_round_trip, but uses the SOURCE command to execute the
        COPY command.  This checks that we don't have unicode-related
        problems when sourcing COPY commands (CASSANDRA-9083).
        """
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )""")

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testcopyto"))

        tempfile = self.get_temp_file()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))

        commandfile = self.get_temp_file()
        with open(commandfile.name, 'w') as f:
            f.write('USE ks;\n')
            f.write("COPY ks.testcopyto TO '{name}' WITH HEADER=false;".format(name=tempfile.name))

        self.node1.run_cqlsh(cmds="SOURCE '{name}'".format(name=commandfile.name))

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        debug('Importing from csv file: {name}'.format(name=tempfile.name))

        commandfile = self.get_temp_file()
        with open(commandfile.name, 'w') as f:
            f.write('USE ks;\n')
            f.write("COPY ks.testcopyto FROM '{name}' WITH HEADER=false;".format(name=tempfile.name))

        self.node1.run_cqlsh(cmds="SOURCE '{name}'".format(name=commandfile.name))
        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        self.assertEqual(results, new_results)

    def _test_bulk_round_trip(self, nodes, partitioner,
                              num_operations, profile=None,
                              stress_table='keyspace1.standard1',
                              configuration_options=None,
                              skip_count_checks=False,
                              copy_to_options={'PAGETIMEOUT': 10, 'PAGESIZE': 1000},
                              copy_from_options=None):
        """
        Test exporting a large number of rows into a csv file.

        If skip_count_checks is True then it means we cannot use "SELECT COUNT(*)" as it may time out but
        it also means that we can be sure that one cassandra-stress operation is one record and hence
        num_records=num_operations.
        """
        self.prepare(nodes=nodes, partitioner=partitioner, configuration_options=configuration_options)

        def create_records():
            if not profile:
                debug('Running stress without any user profile')
                self.node1.stress(['write', 'n={}'.format(num_operations), '-rate', 'threads=50'])
            else:
                debug('Running stress with user profile {}'.format(profile))
                self.node1.stress(['user', 'profile={}'.format(profile), 'ops(insert=1)',
                                   'n={}'.format(num_operations), '-rate', 'threads=50'])

            if skip_count_checks:
                return num_operations
            else:
                ret = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
                debug('Generated {} records'.format(ret))
                self.assertTrue(ret >= num_operations, 'cassandra-stress did not import enough records')
                return ret

        def run_copy_to(filename):
            debug('Exporting to csv file: {}'.format(filename.name))
            start = datetime.datetime.now()
            copy_to_cmd = "CONSISTENCY ALL;"
            copy_to_cmd += "COPY {} TO '{}'".format(stress_table, filename.name)
            if copy_to_options:
                copy_to_cmd += ' WITH ' + ' AND '.join('{} = {}'.format(k, v) for k, v in copy_to_options.iteritems())
            debug(copy_to_cmd)
            self.node1.run_cqlsh(cmds=copy_to_cmd, show_output=True, cqlsh_options=['--debug'])
            debug("COPY TO took {} to export {} records".format(datetime.datetime.now() - start, num_records))

        def run_copy_from(filename):
            debug('Importing from csv file: {}'.format(filename.name))
            start = datetime.datetime.now()
            copy_from_cmd = "COPY {} FROM '{}'".format(stress_table, filename.name)
            if copy_from_options:
                copy_from_cmd += ' WITH ' + ' AND '.join('{} = {}'.format(k, v) for k, v in copy_from_options.iteritems())
            debug(copy_from_cmd)
            self.node1.run_cqlsh(cmds=copy_from_cmd, show_output=True, cqlsh_options=['--debug'])
            debug("COPY FROM took {} to import {} records".format(datetime.datetime.now() - start, num_records))

        num_records = create_records()

        # Copy to the first csv files
        tempfile1 = self.get_temp_file()
        run_copy_to(tempfile1)

        # check all records generated were exported
        self.assertEqual(num_records, sum(1 for _ in open(tempfile1.name)))

        # import records from the first csv file
        debug('Truncating {}...'.format(stress_table))
        self.session.execute("TRUNCATE {}".format(stress_table))
        run_copy_from(tempfile1)

        # export again to a second csv file
        tempfile2 = self.get_temp_file()
        run_copy_to(tempfile2)

        # check the length of both files is the same to ensure all exported records were imported
        self.assertEqual(sum(1 for _ in open(tempfile1.name)),
                         sum(1 for _ in open(tempfile2.name)))

    def test_bulk_round_trip_default(self):
        """
        Test bulk import with default stress import (one row per operation)

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=100000)

    @freshCluster()
    def test_bulk_round_trip_blogposts(self):
        """
        Test bulk import with a user profile that inserts 10 rows per operation and has a replication factor 3

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(nodes=5, partitioner="murmur3", num_operations=10000,
                                   configuration_options={'batch_size_warn_threshold_in_kb': '10'},
                                   profile=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'blogposts.yaml'),
                                   stress_table='stresscql.blogposts',
                                   copy_to_options={'PAGETIMEOUT': 60, 'PAGESIZE': 1000})

    @freshCluster()
    def test_bulk_round_trip_blogposts_with_max_connections(self):
        """
        Same as test_bulk_round_trip_blogposts but limit the maximum number of concurrent connections a host will
        accept to simulate a failed connection to a replica that is up. Here we are interested in testing COPY TO
        where we should have at most worker_processes * nodes connections plus 1 (the cqlsh connection). For COPY
        FROM the driver handles retries. We use only 2 worker processes to make sure it suceeds.

        @jira_ticket CASSANDRA-10938
        """
        self._test_bulk_round_trip(nodes=5, partitioner="murmur3", num_operations=10000,
                                   configuration_options={'native_transport_max_concurrent_connections': '12',
                                                          'batch_size_warn_threshold_in_kb': '10'},
                                   profile=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'blogposts.yaml'),
                                   stress_table='stresscql.blogposts',
                                   copy_to_options={'PAGETIMEOUT': 60, 'PAGESIZE': 1000,
                                                    'NUMPROCESSES': 5, 'MAXATTEMPTS': 20},
                                   copy_from_options={'NUMPROCESSES': 2})

    def test_bulk_round_trip_with_timeouts(self):
        """
        Test bulk import with very short read and write timeout values, this should exercise the
        retry and back-off policies. We cannot check the counts because "SELECT COUNT(*)" could timeout
        on Jenkins making the test flacky.

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(nodes=1, partitioner="murmur3", num_operations=100000,
                                   configuration_options={'range_request_timeout_in_ms': '200',
                                                          'write_request_timeout_in_ms': '100'},
                                   copy_to_options={'PAGETIMEOUT': 60, 'PAGESIZE': 1000},
                                   skip_count_checks=True)

    def test_bulk_round_trip_with_low_ingestrate(self):
        """
        Test bulk import with default stress import (one row per operation) and a low
        ingestrate of only 1500 rows per second.

        @jira_ticket CASSANDRA-9303
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=10000,
                                   copy_from_options={'INGESTRATE': 1500})

    def prepare_copy_to_with_failures(self):
        """
        Create a cluster for testing COPY TO with failure injection, we need at least 3 token ranges
        so if VNODES are disabled we need to manually fix them and specify the correct start and end
        tokens for injecting failures. If VNODES are enabled instead, we will have several ranges
        so we pick an arbitrary range.

        @jira_ticket CASSANDRA-10858
        """
        if DISABLE_VNODES:
            tokens = sorted(self.cluster.balanced_tokens(3))
            debug('Using tokens {}'.format(tokens))
            self.prepare(nodes=3, tokens=tokens)
            start = tokens[1]
            end = tokens[2]
        else:
            start = 0
            end = 5000000000000000000
            self.prepare(nodes=1)

        return start, end

    @freshCluster()
    def test_copy_to_with_more_failures_than_max_attempts(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        Here we set a token range that will fail more times than the maximum number of attempts, therefore
        we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        start, end = self.prepare_copy_to_with_failures()

        debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        failures = {'failing_range': {'start': start, 'end': end, 'num_failures': 5}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)

        debug('Exporting to csv file: {} with {} and 3 max attempts'
              .format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err = self.node1.run_cqlsh(cmds="COPY {} TO '{}' WITH MAXATTEMPTS='3'"
                                        .format(stress_table, tempfile.name),
                                        return_output=True)
        debug(out)
        debug(err)

        self.assertIn('some records might be missing', err)
        self.assertTrue(len(open(tempfile.name).readlines()) < num_records)

    @freshCluster()
    def test_copy_to_with_fewer_failures_than_max_attempts(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        Here we set a token range that will fail fewer times than the maximum number of attempts, therefore
        we expect this COPY TO job to succeed.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        start, end = self.prepare_copy_to_with_failures()

        debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        failures = {'failing_range': {'start': start, 'end': end, 'num_failures': 3}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        debug('Exporting to csv file: {} with {} and 5 max attemps'
              .format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err = self.node1.run_cqlsh(cmds="COPY {} TO '{}' WITH MAXATTEMPTS='5'"
                                        .format(stress_table, tempfile.name),
                                        return_output=True)
        debug(out)
        debug(err)

        self.assertNotIn('some records might be missing', err)
        self.assertEqual(num_records, len(open(tempfile.name).readlines()))

    @freshCluster()
    def test_copy_to_with_child_process_crashing(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        Here we set a token range that will cause a child process processing this range to exit, therefore
        we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        start, end = self.prepare_copy_to_with_failures()

        debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        failures = {'exit_range': {'start': start, 'end': end}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)

        debug('Exporting to csv file: {} with {}'
              .format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err = self.node1.run_cqlsh(cmds="COPY {} TO '{}'"
                                        .format(stress_table, tempfile.name),
                                        return_output=True)
        debug(out)
        debug(err)

        self.assertIn('some records might be missing', err)
        self.assertTrue(len(open(tempfile.name).readlines()) < num_records)

    @freshCluster()
    def test_copy_from_with_more_failures_than_max_attempts(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a batch to fail more times than the maximum number of attempts,
        therefore we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.node1.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))

        self.session.execute("TRUNCATE {}".format(stress_table))

        failures = {'failing_batch': {'id': 30, 'failures': 5}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        debug('Importing from csv file {} with {}'.format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err = self.node1.run_cqlsh(cmds="COPY {} FROM '{}' WITH CHUNKSIZE='1' AND MAXATTEMPTS='3'"
                                        .format(stress_table, tempfile.name), return_output=True)
        debug(out)
        debug(err)

        self.assertIn('Failed to process', err)
        num_records_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
        self.assertTrue(num_records_imported < num_records)

    @freshCluster()
    def test_copy_from_with_fewer_failures_than_max_attempts(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a batch to fail fewer times than the maximum number of attempts,
        therefore we expect this COPY TO job to succeed.

        We also set a low ingest rate to ensure we exercise the code path that might split a retry if it
        exceeds the intest rate.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.node1.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))

        self.session.execute("TRUNCATE {}".format(stress_table))

        failures = {'failing_batch': {'id': 3, 'failures': 3}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        debug('Importing from csv file {} with {}'.format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err = self.node1.run_cqlsh(cmds="COPY {} FROM '{}' WITH CHUNKSIZE=100 AND MAXATTEMPTS=5 AND INGESTRATE=101"
                                        .format(stress_table, tempfile.name), return_output=True)
        debug(out)
        debug(err)

        self.assertNotIn('Failed to process', err)
        num_records_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
        self.assertEquals(num_records, num_records_imported)

    @freshCluster()
    def test_copy_from_with_child_process_crashing(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a child process to exit, therefore we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        debug('Running stress')
        stress_table = 'keyspace1.standard1'
        self.node1.stress(['write', 'n={}'.format(num_records), '-rate', 'threads=50'])

        tempfile = self.get_temp_file()
        debug('Exporting to csv file {} to generate a file'.format(tempfile.name))
        self.node1.run_cqlsh(cmds="COPY {} TO '{}'".format(stress_table, tempfile.name))

        self.session.execute("TRUNCATE {}".format(stress_table))

        failures = {'exit_batch': {'id': 30}}
        os.environ['CQLSH_COPY_TEST_FAILURES'] = json.dumps(failures)
        debug('Importing from csv file {} with {}'.format(tempfile.name, os.environ['CQLSH_COPY_TEST_FAILURES']))
        out, err = self.node1.run_cqlsh(cmds="COPY {} FROM '{}' WITH CHUNKSIZE='1'"
                                        .format(stress_table, tempfile.name), return_output=True)
        debug(out)
        debug(err)

        self.assertIn('1 child process(es) died unexpectedly, aborting', err)
        num_records_imported = rows_to_list(self.session.execute("SELECT COUNT(*) FROM {}".format(stress_table)))[0][0]
        self.assertTrue(num_records_imported < num_records)
