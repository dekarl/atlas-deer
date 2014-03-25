package org.atlasapi.content;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.atlasapi.CassandraPersistenceModule;
import org.atlasapi.ConfiguredAstyanaxContext;
import org.atlasapi.DatastaxCassandraService;
import org.atlasapi.PersistenceModule;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageSender;
import org.atlasapi.messaging.ProducerQueueFactory;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.ids.IdGeneratorBuilder;
import com.metabroadcast.common.ids.SequenceGenerator;
import com.metabroadcast.common.time.DateTimeZones;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassandraEquivalentContentStoreIT extends EquivalentContentStoreTestSuite {

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
        session.execute("CREATE TABLE atlas_testing.equivalent_content (set_id bigint, content_id bigint, graph blob, data blob, PRIMARY KEY (set_id,content_id));");
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
        session.execute("TRUNCATE atlas_testing.equivalent_content_index");
        session.execute("TRUNCATE atlas_testing.equivalent_content");
    }
    
    @Test
    public void testRemovesOldRows() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), Publisher.METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), Publisher.METABROADCAST);
        Content c3 = createAndWriteItem(Id.valueOf(31), Publisher.METABROADCAST);
        Content c4 = createAndWriteItem(Id.valueOf(41), Publisher.METABROADCAST);
        Content c5 = createAndWriteItem(Id.valueOf(51), Publisher.METABROADCAST);
        Content c6 = createAndWriteItem(Id.valueOf(61), Publisher.METABROADCAST);
        
        persistenceModule.equivalentContentStore().updateContent(c1.toRef());
        persistenceModule.equivalentContentStore().updateContent(c2.toRef());
        persistenceModule.equivalentContentStore().updateContent(c3.toRef());
        persistenceModule.equivalentContentStore().updateContent(c4.toRef());
        persistenceModule.equivalentContentStore().updateContent(c5.toRef());
        persistenceModule.equivalentContentStore().updateContent(c6.toRef());
        
        makeEquivalent(c2, c4);
        makeEquivalent(c3, c5);
        
        resolved(c2, c2, c4);
        resolved(c3, c3, c5);
        
        assertNoRowsWithSetId(c4.getId());
        assertNoRowsWithSetId(c5.getId());
        
        makeEquivalent(c1, c2, c3);
        
        resolved(c1, c1, c2, c3, c4, c5);
        
        assertNoRowsWithSetId(c2.getId());
        assertNoRowsWithSetId(c3.getId());
        assertNoRowsWithSetId(c4.getId());
        assertNoRowsWithSetId(c5.getId());
        
        makeEquivalent(c1, c2);
        
        resolved(c1, c1, c2, c4);
        resolved(c3, c3, c5);
        
        assertNoRowsWithIds(c1.getId(), c3.getId());
        assertNoRowsWithIds(c1.getId(), c5.getId());
        assertNoRowsWithSetId(c2.getId());
        assertNoRowsWithSetId(c4.getId());
        assertNoRowsWithSetId(c5.getId());
        
        makeEquivalent(c2);
        
        resolved(c1, c1, c2); 
        resolved(c4, c4);
        resolved(c3, c3, c5);
        
        assertNoRowsWithIds(c1.getId(), c4.getId());
        assertNoRowsWithSetId(c2.getId());
        assertNoRowsWithSetId(c5.getId());
    }

    private void assertNoRowsWithIds(Id setId, Id contentId) {
        Session session = cassandraService.getSession(keyspace);
        Statement rowsForIdQuery = select().all().from("equivalent_content")
                .where(eq("set_id", setId.longValue()))
                .and(eq("content_id", contentId.longValue()));
        ResultSet rows = session.execute(rowsForIdQuery);
        boolean exhausted = rows.isExhausted();
        assertTrue(String.format("Expected 0 rows for %s-%s, got %s", setId, contentId, rows.all().size()), exhausted);
    }

    private void assertNoRowsWithSetId(Id setId) {
        Session session = cassandraService.getSession(keyspace);
        Statement rowsForIdQuery = select().all().from("equivalent_content").where(eq("set_id", setId.longValue()));
        ResultSet rows = session.execute(rowsForIdQuery);
        boolean exhausted = rows.isExhausted();
        assertTrue(String.format("Expected 0 rows for %s, got %s", setId, rows.all().size()), exhausted);
    }

    private void resolved(Content c, Content... cs) throws Exception {
        ResolvedEquivalents<Content> resolved
            = get(persistenceModule.equivalentContentStore().resolveIds(ImmutableList.of(c.getId()), 
                    ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        ImmutableSet<Content> idContent = resolved.get(c.getId());
        assertEquals(ImmutableSet.copyOf(cs), idContent);
    }

    private <T> T get(ListenableFuture<T> resolveIds) throws Exception {
        return Futures.get(resolveIds, 10, TimeUnit.MINUTES, Exception.class);
    }
    

    private void makeEquivalent(Content c, Content... cs) throws WriteException {
        Set<ResourceRef> csRefs = ImmutableSet.<ResourceRef>copyOf(
                Iterables.transform(ImmutableSet.copyOf(cs), Content.toContentRef()));
       
        Optional<EquivalenceGraphUpdate> graphs
            = persistenceModule.contentEquivalenceGraphStore().updateEquivalences(c.toRef(), csRefs, 
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));
        
        persistenceModule.equivalentContentStore().updateEquivalences(graphs.get());
        
    }

    private Content createAndWriteItem(Id id, Publisher src) throws WriteException {
        Content content = new Item(id, src);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        WriteResult<Content> result = persistenceModule.contentStore().writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        return content;
    }

}
