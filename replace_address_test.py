from time import sleep

from cassandra import ConsistencyLevel, ReadTimeout, Unavailable
from cassandra.query import SimpleStatement
from ccmlib.node import Node, NodeError
from dtest import DISABLE_VNODES, Tester, debug
from tools import InterruptBootstrap, known_failure, since
from threading import Thread
import os, time


def update_auth_keyspace_replication(session):
    # Change system_auth keyspace replication factor to 2, otherwise replace will fail
    session.execute("""
                ALTER KEYSPACE system_auth
                    WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};
            """)

class NodeUnavailable(Exception):
    pass

class TestReplaceAddress(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread',
            # ignore streaming error during bootstrap
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)
        self.allow_log_errors = True

    def replace_stopped_node_test(self):
        """
        Test that we can replace a node that is not shutdown gracefully.
        """
        self._replace_node_test(gently=False)

    def replace_shutdown_node_test(self):
        """
        @jira_ticket CASSANDRA-9871
        Test that we can replace a node that is shutdown gracefully.
        """
        self._replace_node_test(gently=True)

    def _replace_node_test(self, gently):
        """
        Check that the replace address function correctly replaces a node that has failed in a cluster.
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        session.default_timeout = 45
        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=gently, wait_other_notice=True)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
                session.execute(query)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")

        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.3', wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertTrue(len(movedTokensList) > 0)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start(wait_other_notice=False)
        checkCollision = node1.grep_log("Node /127.0.0.4 will complete replacement of /127.0.0.3 for tokens")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

    def write_topology_file(self, snitch_config_file, node, dc, rack):
        with open(os.path.join(node.get_conf_dir(), snitch_config_file), 'w') as topo_file:
            debug("Node {} has dc {} and rack {}.".format(node.name, dc, rack))
            topo_file.write("dc={}".format(dc) + os.linesep)
            topo_file.write("rack={}".format(rack) + os.linesep)

    def replace_overlapping_rack_primary_range_test(self):
        """
        Replaces node1 with a node from a rack that already
        replicates its primary range (rack1, node2) and check
        that data is present.
        """
        self._base_rack_aware_test("rack1")

    def replace_overlapping_rack_secondary_range_test(self):
        """
        Replaces node1 with a node from a rack that already
        replicates its secondary range (rack2, node3) and check
        that data is present.
        """
        self._base_rack_aware_test("rack2")

    def replace_non_overlapping_rack_test(self):
        """
        Replaces node1 with a node from a rack that does not
        share ranges with the failed node and check
        that data is present.
        """
        self._base_rack_aware_test("rack3")

    def replace_same_rack_test(self):
        """
        Replaces node1 with a node from the same rack and
        check that data is present.
        """
        self._base_rack_aware_test("rack0")

    def _base_rack_aware_test(self, replacement_rack):
        """
        This base test has the following topology:

        node1(token=-9223372036854775808, rack=rack0)
        node2(token=-3074457345618258603, rack=rack1)
        node3(token=3074457345618258602, rack=rack2)

        Since RF=2, node1 replicates data from rack0 (primary range) and rack2 (secondary range).

        Child tests will replace node1 with a new node4, changing the rack of node4.
        """
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3)

        tokens = [-9223372036854775808, -3074457345618258603, 3074457345618258602]
        nodes = cluster.nodelist()
        node1, node2, node3 = nodes

        for i, node in enumerate(nodes):
            node.set_configuration_options(values={'initial_token': tokens[i], 'endpoint_snitch': 'GossipingPropertyFileSnitch',
                                                   'num_tokens': 1})
            self.write_topology_file('cassandra-rackdc.properties', node, "dc1", "rack{}".format(i))

        cluster.start()

        debug("Inserting Data...")
        node2.stress(['write', 'n=10000', '-schema', 'replication(strategy=NetworkTopologyStrategy,dc1=2)'])

        session = self.patient_exclusive_cql_connection(node2)
        session.default_timeout = 45

        # Change system_auth keyspace replication factor to 2, otherwise replace will fail
        session = self.patient_cql_connection(node1)
        update_auth_keyspace_replication(session)

        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s' % stress_table, consistency_level=ConsistencyLevel.TWO)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 2
        debug("Stopping node 1.")
        node1.stop(wait_other_notice=True)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                query = SimpleStatement('select * from %s' % stress_table, consistency_level=ConsistencyLevel.TWO)
                session.execute(query)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 1 with node 4
        debug("Starting node 4 to replace node 1")

        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None,
                     binary_interface=('127.0.0.4', 9042))
        node4.set_configuration_options(values={'initial_token': tokens[i], 'endpoint_snitch': 'GossipingPropertyFileSnitch',
                                                'num_tokens': 1})
        self.write_topology_file('cassandra-rackdc.properties', node4, "dc1", replacement_rack)
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.1', wait_for_binary_proto=True)

        # cleanup to guarantee each node will only have sstables of its ranges
        cluster.cleanup()

        # query at CL=ONE, so if any node is missing data query will fail
        debug("Verifying querying works again.")
        query = SimpleStatement('select * from %s' % stress_table, consistency_level=ConsistencyLevel.ONE)
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token -9223372036854775808 changing ownership from /127.0.0.1 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertTrue(len(movedTokensList) > 0)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 1 (should fail)")
        node1.start()
        checkCollision = node3.grep_log("Node /127.0.0.4 will complete replacement of /127.0.0.1 for tokens")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

    def replace_active_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # replace active node 3 with node 4
        debug("Starting node 4 to replace active node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)

        mark = node4.mark_log()
        node4.start(replace_address='127.0.0.3', wait_other_notice=False)
        node4.watch_log_for("java.lang.UnsupportedOperationException: Cannot replace a live node...", from_mark=mark)
        self.check_not_running(node4)

    def replace_nonexistent_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        debug('Start node 4 and replace an address with no node')
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)

        # try to replace an unassigned ip address
        mark = node4.mark_log()
        node4.start(replace_address='127.0.0.5', wait_other_notice=False)
        node4.watch_log_for("java.lang.RuntimeException: Cannot replace_address /127.0.0.5 because it doesn't exist in gossip", from_mark=mark)
        self.check_not_running(node4)

    def check_not_running(self, node):
        attempts = 0
        while node.is_running() and attempts < 10:
            sleep(1)
            attempts = attempts + 1

        self.assertFalse(node.is_running())

    def replace_first_boot_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

        debug("Inserting Data...")
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])

        # Change system_auth keyspace replication factor to 2, otherwise replace will fail
        session = self.patient_cql_connection(node1)
        update_auth_keyspace_replication(session)

        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=False)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                session.execute(query, timeout=30)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"], wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertTrue(len(movedTokensList) > 0)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start(wait_other_notice=False)
        checkCollision = node1.grep_log("Node /127.0.0.4 will complete replacement of /127.0.0.3 for tokens")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

        # restart node4 (if error's might have to change num_tokens)
        node4.stop(gently=False)
        node4.start(wait_for_binary_proto=True)

        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

        # we redo this check because restarting node should not result in tokens being moved again, ie number should be same
        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertTrue(len(movedTokensList) > 0)

    def insert_data_during_replace_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

        # Change system_auth keyspace replication factor to 2, otherwise replace will fail
        session = self.patient_cql_connection(node1)
        update_auth_keyspace_replication(session)

        # run stress with 1 entry just to create schema
        node1.stress(['write', 'n=1', "no-warmup", '-schema', 'replication(factor=3)'],
                     whitelist=True)

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=False, wait_other_notice=True)

        # start node4 on write survey mode so it does not finish joining ring
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), 
                      '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3", 
                              "-Dcassandra.write_survey=true"], wait_for_binary_proto=True,
                              wait_other_notice=False)


        # now write data to node1, it should send the data to node4 that is joining the ring
        debug("Writing data to node1 - should replicate to replacement node")
        node1.stress(['write', 'n=1k', "no-warmup", '-schema', 'replication(factor=3)'],
                     whitelist=True)

        # finish joining the ring
        node4.nodetool("join")

        node1.watch_log_for("Node /127.0.0.4 will complete replacement of /127.0.0.3 for tokens", timeout=10)
        node2.watch_log_for("Node /127.0.0.4 will complete replacement of /127.0.0.3 for tokens", timeout=10)
        node4.watch_log_for("Node /127.0.0.4 will complete replacement of /127.0.0.3 for tokens", timeout=10)

        # stop other nodes to query data only from node4
        node1.stop(wait_other_notice=True)
        node2.stop(wait_other_notice=True)

        debug("Check data is present")
        # Let's check stream bootstrap completely transferred data
        stdout, stderr = node4.stress(['read', 'cl=ONE', 'n=1k', "no-warmup", '-schema',
                                       'replication(factor=3)', '-rate', 'threads=8'],
                                       whitelist=True, capture_output=True)

        debug(stdout)
        debug(stderr)
        if stdout and "FAILURE" in stdout:
            debug(stdout)
            assert False, "Cannot read inserted data after bootstrap"

        # Start nodes 1 and 2 again
        node1.stop(wait_other_notice=True)
        node2.stop(wait_other_notice=True)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start(wait_other_notice=False)
        node4.watch_log_for("java.lang.UnsupportedOperationException: Cannot replace a live node...")
        self.check_not_running(node3)

    @since('2.2')
    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-9831',
                   flaky=True)
    def resumable_replace_test(self):
        """Test resumable bootstrap while replacing node"""

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=3)'])

        # Change system_auth keyspace replication factor to 2, otherwise replace will fail
        session = self.patient_cql_connection(node1)
        update_auth_keyspace_replication(session)

        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        # keep timeout low so that test won't hang
        node4.set_configuration_options(values={'streaming_socket_timeout_in_ms': 1000})
        cluster.add(node4, False)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"])
        except NodeError:
            pass  # node doesn't start as expected
        t.join()

        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start()
        node4.nodetool('bootstrap resume')
        # check if we skipped already retrieved ranges
        node4.watch_log_for("already available. Skipping streaming.")
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...")

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)

    @since('2.2')
    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11246',
                   flaky=True,
                   notes='windows')
    def replace_with_reset_resume_state_test(self):
        """Test replace with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=3)'])

        # Change system_auth keyspace replication factor to 2, otherwise replace will fail
        session = self.patient_cql_connection(node1)
        update_auth_keyspace_replication(session)


        stress_table = 'keyspace1.standard1'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = list(session.execute(query))

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, binary_interface=('127.0.0.4', 9042))
        # keep timeout low so that test won't hang
        node4.set_configuration_options(values={'streaming_socket_timeout_in_ms': 1000})
        cluster.add(node4, False)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"], wait_other_notice=False)
        except NodeError:
            pass  # node doesn't start as expected
        t.join()
        node1.start()

        # restart node4 bootstrap with resetting bootstrap state
        node4.stop()
        mark = node4.mark_log()
        node4.start(jvm_args=[
                    "-Dcassandra.replace_address_first_boot=127.0.0.3",
                    "-Dcassandra.reset_bootstrap_progress=true"
                    ])
        # check if we reset bootstrap state
        node4.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...", from_mark=mark)

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = list(session.execute("SELECT bootstrapped FROM system.local WHERE key='local'"))
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        # query should work again
        debug("Verifying querying works again.")
        finalData = list(session.execute(query))
        self.assertListEqual(initialData, finalData)
