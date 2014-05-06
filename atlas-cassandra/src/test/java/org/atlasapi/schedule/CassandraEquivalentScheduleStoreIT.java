package org.atlasapi.schedule;

import java.util.concurrent.TimeUnit;

import junit.framework.TestSuite;

import org.atlasapi.content.ContentStore;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

@RunWith(AllTests.class)
public class CassandraEquivalentScheduleStoreIT {

    public static junit.framework.Test suite() throws Exception {
        TestSuite suite = new TestSuite("CassandraEquivalentScheduleStoreIT");
        
        final TestCassandraPersistenceModule module = new TestCassandraPersistenceModule(){
            @Override
            protected void createTables(Session session, AstyanaxContext<Keyspace> context)
                    throws ConnectionException {
                session.execute("CREATE TABLE atlas_testing.equivalence_graph_index (resource_id bigint, graph_id bigint, PRIMARY KEY (resource_id));");
                session.execute("CREATE TABLE atlas_testing.equivalence_graph (graph_id bigint, graph blob, PRIMARY KEY (graph_id));");
                session.execute("CREATE TABLE atlas_testing.equivalent_schedule (source text, channel bigint, day timestamp, broadcast_id text, broadcast_start timestamp, broadcast blob, graph blob, content_count bigint, content blob, schedule_update timestamp, equiv_update timestamp, PRIMARY KEY ((source, channel, day), broadcast_id)) ");
                CassandraHelper.createColumnFamily(context, "content", LongSerializer.get(), StringSerializer.get());
                CassandraHelper.createColumnFamily(context, "content_aliases", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
            }
            
            @Override
            protected void clearTables(Session session, AstyanaxContext<Keyspace> context) throws ConnectionException {
                ImmutableList<String> tables = ImmutableList.of("equivalence_graph_index", 
                        "equivalence_graph", "equivalent_schedule");
                for (String table : tables) {
                    session.execute(String.format("TRUNCATE %s", table));
                }
                CassandraHelper.clearColumnFamily(context, "content");
                CassandraHelper.clearColumnFamily(context, "content_aliases");
            }
        };
        module.startAsync().awaitRunning(1, TimeUnit.MINUTES);

        suite.addTest(EquivalentScheduleStoreTestSuiteBuilder
            .using(new EquivalentScheduleStoreSubjectGenerator() {

                @Override
                public EquivalenceGraphStore getEquivalenceGraphStore() {
                    return module.contentEquivalenceGraphStore();
                }
                
                @Override
                public ContentStore getContentStore() {
                    return module.contentStore();
                }

                @Override
                public EquivalentScheduleStore getEquivalentScheduleStore() {
                    return module.equivalentScheduleStore();
                }
                
            })
            .withTearDown(new Runnable() {
                @Override
                public void run() {
                    try {
                        module.reset();
                    } catch (ConnectionException e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .named("CassandraEquivalentScheduleStoreIntegrationSuite")
            .createTestSuite());
        return suite;
     }
    
}
