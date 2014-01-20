package org.atlasapi.content;

import java.io.IOException;
import java.util.UUID;

import org.atlasapi.CassandraPersistenceModule;
import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.DatastaxCassandraService;
import org.atlasapi.PersistenceModule;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageSender;
import org.atlasapi.messaging.ProducerQueueFactory;
import org.testng.annotations.AfterClass;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.ids.SequenceGenerator;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassandraEquivalentContentStoreIT extends EquivalentContentStoreTestSuite {

    {
        //do not change this
        System.setProperty("messaging.destination.equivalence.content.graph.changes", "just-bloody-work");
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

    private ProducerQueueFactory messageSenderFactory = new ProducerQueueFactory() {
        @Override
        public MessageSender makeMessageSender(String destinationName) {
            return new MessageSender() {
                @Override
                public void sendMessage(Message message) throws IOException {
                    //no-op
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
        session.execute("CREATE TABLE atlas_testing.equivalent_content_index (key bigint, value bigint, PRIMARY KEY (key));");
        session.execute("CREATE TABLE atlas_testing.equivalent_content (set_id bigint, graph blob, content map<bigint, blob>, PRIMARY KEY (set_id));");
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

}
