package org.atlasapi.schedule;

import static org.junit.Assert.assertTrue;

import java.util.UUID;

import org.atlasapi.CassandraPersistenceModule;
import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.DatastaxCassandraService;
import org.atlasapi.PersistenceModule;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.entity.CassandraHelper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.ids.SequenceGenerator;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessageSenderFactory;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.queue.MessagingException;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassandraEquivalentScheduleStoreIT extends EquivalentScheduleStoreTestSuite {

    {
        //do not change this
        System.setProperty("messaging.destination.equivalence.content.graph.changes", "just-bloody-work");
        System.setProperty("messaging.destination.content.changes", "just-bloody-work");
        System.setProperty("messaging.destination.topics.changes", "just-bloody-work");
        System.setProperty("messaging.destination.schedule.changes", "just-bloody-work");
    }
    
    private static final ContentHasher hasher = new ContentHasher() {

        @Override
        public String hash(Content content) {
            return UUID.randomUUID().toString();
        }
    };

    private final ImmutableSet<String> seeds = ImmutableSet.of("localhost");
    private final String keyspace = "atlas_testing";
    private final AstyanaxContext<Keyspace> context
        = new ConfiguredAstyanaxContext("Build", keyspace, seeds, 9160, 5, 60).get();
    private final DatastaxCassandraService cassandraService = new DatastaxCassandraService(seeds);

    private MessageSenderFactory messageSenderFactory = new MessageSenderFactory() {

        @Override
        public <M extends Message> MessageSender<M> makeMessageSender(
                String destination, MessageSerializer<? super M> serializer) {
            return new MessageSender<M>() {

                @Override
                public void close() throws Exception {
                }

                @Override
                public void sendMessage(M message) throws MessagingException {
                }
            };
        }
        
    };
    private final CassandraPersistenceModule persistenceModule
        = new CassandraPersistenceModule(messageSenderFactory , context, cassandraService,
            keyspace, idGeneratorBuilder(), hasher);

    @Override
    PersistenceModule persistenceModule() throws ConnectionException {
        cassandraService.startAsync().awaitRunning();
        Session session = cassandraService.getCluster().connect();
        session.execute("CREATE KEYSPACE atlas_testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");
        session.execute("CREATE TABLE atlas_testing.equivalence_graph_index (resource_id bigint, graph_id bigint, PRIMARY KEY (resource_id));");
        session.execute("CREATE TABLE atlas_testing.equivalence_graph (graph_id bigint, graph blob, PRIMARY KEY (graph_id));");
        session.execute("CREATE TABLE atlas_testing.equivalent_schedule (source text, channel bigint, day timestamp, broadcast_id text, broadcast blob, graph blob, content_count bigint, content blob, schedule_update timestamp, equiv_update timestamp, PRIMARY KEY ((source, channel, day), broadcast_id)) ");
        context.start();
        CassandraHelper.createColumnFamily(context, "content", LongSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, "content_aliases", StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
        persistenceModule.startAsync().awaitRunning();
        return persistenceModule;
    }

    private static IdGeneratorBuilder idGeneratorBuilder() {
        return new IdGeneratorBuilder() {

            @Override
            public IdGenerator generator(String sequenceIdentifier) {
                return new SequenceGenerator();
            }
        };
    }

    @AfterClass(alwaysRun=true)
    public void tearDownKeyspace() {
        Session session = cassandraService.getCluster().connect();
        session.execute("DROP KEYSPACE atlas_testing");
    }
    
    @AfterMethod(alwaysRun=true)
    public void truncateCfs() {
        Session session = cassandraService.getCluster().connect();
        session.execute("TRUNCATE atlas_testing.equivalence_graph_index");
        session.execute("TRUNCATE atlas_testing.equivalence_graph");
        session.execute("TRUNCATE atlas_testing.equivalent_schedule");
    }
    
    @Test
    public void test() {
        assertTrue(true);
    }
}
