import os

from ccmlib.node import ToolError
from dtest import Tester, debug
from tools.assertions import assert_all, assert_invalid
from tools.decorators import since
from tools.jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem


class TestNodetool(Tester):

    def test_decommission_after_drain_is_invalid(self):
        """
        @jira_ticket CASSANDRA-8741

        Running a decommission after a drain should generate
        an unsupported operation message and exit with an error
        code (which we receive as a ToolError exception).
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]
        node.drain(block_on_log=True)

        try:
            node.decommission()
            self.assertFalse("Expected nodetool error")
        except ToolError as e:
            self.assertEqual('', e.stderr)
            self.assertTrue('Unsupported operation' in e.stdout)

    def test_correct_dc_rack_in_nodetool_info(self):
        """
        @jira_ticket CASSANDRA-10382

        Test that nodetool info returns the correct rack and dc
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as snitch_file:
                for line in ["dc={}".format(node.data_center), "rack=rack{}".format(i % 2)]:
                    snitch_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        for i, node in enumerate(cluster.nodelist()):
            out, err, _ = node.nodetool('info')
            self.assertEqual(0, len(err), err)
            debug(out)
            for line in out.split(os.linesep):
                if line.startswith('Data Center'):
                    self.assertTrue(line.endswith(node.data_center),
                                    "Expected dc {} for {} but got {}".format(node.data_center, node.address(), line.rsplit(None, 1)[-1]))
                elif line.startswith('Rack'):
                    rack = "rack{}".format(i % 2)
                    self.assertTrue(line.endswith(rack),
                                    "Expected rack {} for {} but got {}".format(rack, node.address(), line.rsplit(None, 1)[-1]))

    @since('3.4')
    def test_nodetool_timeout_commands(self):
        """
        @jira_ticket CASSANDRA-10953

        Test that nodetool gettimeout and settimeout work at a basic level
        """
        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        types = ['read', 'range', 'write', 'counterwrite', 'cascontention',
                 'truncate', 'misc']
        if cluster.version() < '4.0':
            types.append('streamingsocket')
    
        # read all of the timeouts, make sure we get a sane response
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* \d+ ms')

        # set all of the timeouts to 123
        for timeout_type in types:
            _, err, _ = node.nodetool('settimeout {} 123'.format(timeout_type))
            self.assertEqual(0, len(err), err)

        # verify that they're all reported as 123
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* 123 ms')

    def test_meaningless_notice_in_status(self):
        """
        @jira_ticket CASSANDRA-10176

        nodetool status don't return ownership when there is more than one user keyspace
        define (since they likely have different replication infos making ownership
        meaningless in general) and shows a helpful notice as to why it does that.
        This test checks that said notice is only printed is there is indeed more than
        one user keyspace.
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]

        notice_message = r'effective ownership information is meaningless'

        # Do a first try without any keypace, we shouldn't have the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks1 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 1 keyspace, we should still not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 2 keyspaces with the same settings, we should not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks3 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':3}")

        # With a keyspace without the same replication factor, we should get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertRegexpMatches(out, notice_message)

    @since('4.0')
    def test_set_get_batchlog_replay_throttle(self):
        """
        @jira_ticket CASSANDRA-13614

        Test that batchlog replay throttle can be set and get through nodetool
        """
        cluster = self.cluster
        cluster.populate(2)
        node = cluster.nodelist()[0]
        cluster.start()

        # Test that nodetool help messages are displayed
        self.assertTrue('Set batchlog replay throttle' in node.nodetool('help setbatchlogreplaythrottle').stdout)
        self.assertTrue('Print batchlog replay throttle' in node.nodetool('help getbatchlogreplaythrottle').stdout)

        # Set and get throttle with nodetool, ensuring that the rate change is logged
        node.nodetool('setbatchlogreplaythrottle 2048')
        self.assertTrue(len(node.grep_log('Updating batchlog replay throttle to 2048 KB/s, 1024 KB/s per endpoint',
                                          filename='debug.log')) > 0)
        self.assertTrue('Batchlog replay throttle: 2048 KB/s' in node.nodetool('getbatchlogreplaythrottle').stdout)

    @since('3.0')
    def test_reloadlocalschema(self):
        """
        @jira_ticket CASSANDRA-13954

        Test that `nodetool reloadlocalschema` works as intended
        """
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node) # for jmx
        cluster.start()

        session = self.patient_cql_connection(node)

        query = "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2};"
        session.execute(query)

        query = 'CREATE TABLE test.test (pk int, ck int, PRIMARY KEY (pk, ck));'
        session.execute(query)

        ss = make_mbean('db', type='StorageService')

        schema_version = ''

        # get initial schema version
        with JolokiaAgent(node) as jmx:
            schema_version = jmx.read_attribute(ss, 'SchemaVersion')

        # manually add a regular column 'val' to test.test
        query = """
            INSERT INTO system_schema.columns
                (keyspace_name, table_name, column_name, clustering_order,
                 column_name_bytes, kind, position, type)
            VALUES
                ('test', 'test', 'val', 'none',
                 0x76616c, 'regular', -1, 'int');"""
        session.execute(query)

        # validate that schema version wasn't automatically updated
        with JolokiaAgent(node) as jmx:
            self.assertEqual(schema_version, jmx.read_attribute(ss, 'SchemaVersion'))

        # make sure the new column wasn't automagically picked up
        assert_invalid(session, 'INSERT INTO test.test (pk, ck, val) VALUES (0, 1, 2);')

        # force the node to reload schema from disk
        node.nodetool('reloadlocalschema')

        # validate that schema version changed
        with JolokiaAgent(node) as jmx:
            self.assertNotEqual(schema_version, jmx.read_attribute(ss, 'SchemaVersion'))

        # try an insert with the new column again and validate it succeeds this time
        session.execute('INSERT INTO test.test (pk, ck, val) VALUES (0, 1, 2);')
        assert_all(session, 'SELECT pk, ck, val FROM test.test;', [[0, 1, 2]])

    @since('4.0')
    def test_set_get_concurrent_view_builders(self):
        """
        @jira_ticket CASSANDRA-12245

        Test that the number of concurrent view builders can be set and get through nodetool
        """
        cluster = self.cluster
        cluster.populate(2)
        node = cluster.nodelist()[0]
        cluster.start()

        # Test that nodetool help messages are displayed
        self.assertTrue('Set the number of concurrent view' in node.nodetool('help setconcurrentviewbuilders').stdout)
        self.assertTrue('Get the number of concurrent view' in node.nodetool('help getconcurrentviewbuilders').stdout)

        # Set and get throttle with nodetool, ensuring that the rate change is logged
        node.nodetool('setconcurrentviewbuilders 4')
        self.assertTrue('Current number of concurrent view builders in the system is: \n4'
                        in node.nodetool('getconcurrentviewbuilders').stdout)

        # Try to set an invalid zero value
        try:
            node.nodetool('setconcurrentviewbuilders 0')
        except ToolError as e:
            self.assertTrue('concurrent_view_builders should be great than 0.' in e.stdout)
            self.assertTrue('Number of concurrent view builders should be greater than 0.', e.message)
        else:
            self.fail("Expected error when setting and invalid value")
