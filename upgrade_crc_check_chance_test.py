from ccmlib.common import is_win

from dtest import Tester, debug
from tools import since, rows_to_list

@since('3.0')
class TestCrcCheckChanceUpgrade(Tester):

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def crc_check_chance_upgrade_test(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="git:cassandra-2.2")
        cluster.populate(1).start()

        node1, = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) WITH compression = {
                          'sstable_compression': 'DeflateCompressor',
                          'chunk_length_kb': 256,
                          'crc_check_chance': 0.6 }
                        """)

        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
        debug(meta.options['compression_parameters'])
        self.assertEqual('{"crc_check_chance":"0.6","sstable_compression":"org.apache.cassandra.io.compress.DeflateCompressor","chunk_length_kb":"256"}',
                         meta.options['compression_parameters'])
        session.shutdown()

        # upgrade node1 to 3.0
        self.upgrade_to_version("cassandra-3.0", node1)

        session = self.patient_exclusive_cql_connection(node1)
        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']

        self.assertEqual('org.apache.cassandra.io.compress.DeflateCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        # driver still doesn't support top-level crc_check_chance property, so let's fetch directly from system_schema
        res = session.execute("SELECT crc_check_chance from system_schema.tables "
                              "WHERE keyspace_name = 'ks' AND table_name  = 'cf1';")
        assert rows_to_list(res) == [[0.6]], res

        debug('Test completed successfully')

    def upgrade_to_version(self, tag, node):
        format_args = {'node': node.name, 'tag': tag}
        debug('Upgrading node {node} to {tag}'.format(**format_args))
        # drain and shutdown
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False)
        debug('{node} stopped'.format(**format_args))

        # Ignore errors before upgrade on Windows
        if is_win():
            node.mark_log_for_errors()

        # Update Cassandra Directory
        debug('Updating version to tag {tag}'.format(**format_args))
        
        debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))
        node.set_install_dir(version='git:' + tag, verbose=True)
        # Restart node on new version
        debug('Starting {node} on new version ({tag})'.format(**format_args))
        # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
        node.set_log_level("INFO")
        node.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug('Running upgradesstables')
        node.nodetool('upgradesstables -a')
        debug('Upgrade of {node} complete'.format(**format_args))