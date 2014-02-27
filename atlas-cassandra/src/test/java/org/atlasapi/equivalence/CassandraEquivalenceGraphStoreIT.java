package org.atlasapi.equivalence;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.DatastaxCassandraService;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.ResolveException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageSender;
import org.joda.time.DateTime;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.time.DateTimeZones;

public class CassandraEquivalenceGraphStoreIT {

    private DatastaxCassandraService service
        = new DatastaxCassandraService(ImmutableList.of("localhost"));
    private CassandraEquivalenceGraphStore store;
    private Session session;
    
    private MessageSender messageSender = new MessageSender() {
        @Override
        public void sendMessage(Message message) throws IOException {
            //no-op;
        }
    };

    @BeforeClass
    public void setUp() {
        bbcItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        paItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        itvItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        c4Item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        fiveItem.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        service.startAsync().awaitRunning();
        session = service.getCluster().connect();
        session.execute("CREATE KEYSPACE atlas_testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");
        session = service.getSession("atlas_testing");
        session.execute(
            "CREATE TABLE equivalence_graph_index (resource_id bigint, graph_id bigint, PRIMARY KEY (resource_id));"
        );
        session.execute("CREATE TABLE equivalence_graph ("
            + "graph_id bigint, "
            + "graph blob, "
            + "PRIMARY KEY (graph_id)"
        + ");");
        store = new CassandraEquivalenceGraphStore(messageSender, session , ConsistencyLevel.ONE, ConsistencyLevel.ONE);
    }
    
    @AfterClass
    public void tearDown() {
        session.execute("DROP KEYSPACE atlas_testing");
    }
    
    @AfterMethod
    public void truncate() {
        session.execute("TRUNCATE equivalence_graph_index");
        session.execute("TRUNCATE equivalence_graph");
    }
    
    private final Item bbcItem = new Item(Id.valueOf(1), Publisher.BBC);
    private final Item paItem = new Item(Id.valueOf(2), Publisher.PA);
    private final Item itvItem = new Item(Id.valueOf(3), Publisher.ITV);
    private final Item c4Item = new Item(Id.valueOf(4), Publisher.C4);
    private final Item fiveItem = new Item(Id.valueOf(5), Publisher.FIVE);
    
    @Test
    public void testUpdatingEquivalences() throws Exception {

        Set<Publisher> sources = ImmutableSet.of(bbcItem.getPublisher(), paItem.getPublisher(), c4Item.getPublisher());
        store.updateEquivalences(bbcItem.toRef(), ImmutableSet.<ResourceRef>of(paItem.toRef(), c4Item.toRef()), sources);
        
        sources = ImmutableSet.of(itvItem.getPublisher(), fiveItem.getPublisher());
        store.updateEquivalences(itvItem.toRef(), ImmutableSet.<ResourceRef>of(fiveItem.toRef()), sources);
        
        sources = ImmutableSet.of(itvItem.getPublisher(), paItem.getPublisher());
        store.updateEquivalences(itvItem.toRef(), ImmutableSet.<ResourceRef>of(paItem.toRef()), sources);
        
        sources = ImmutableSet.of(itvItem.getPublisher(), paItem.getPublisher(), fiveItem.getPublisher());
        store.updateEquivalences(itvItem.toRef(), ImmutableSet.<ResourceRef>of(), sources);
        
        ListenableFuture<OptionalMap<Id,EquivalenceGraph>> resolveIds = store.resolveIds(ImmutableList.of(Id.valueOf(3), Id.valueOf(1)));
        OptionalMap<Id, EquivalenceGraph> graphs = Futures.get(resolveIds, ResolveException.class);
        
        EquivalenceGraph graph = graphs.get(Id.valueOf(3)).get();
        assertThat(Iterables.getOnlyElement(graph.getEquivalenceSet()), is(Id.valueOf(3)));
        
        graph = graphs.get(Id.valueOf(1)).get();
        assertThat(graph.getEquivalenceSet(), is(ImmutableSet.of(Id.valueOf(1),Id.valueOf(2),Id.valueOf(4))));
        
        resolveIds = store.resolveIds(ImmutableList.of(Id.valueOf(1),Id.valueOf(2)));
        graphs = Futures.get(resolveIds, ResolveException.class);
        
        EquivalenceGraph graph1 = graphs.get(Id.valueOf(1)).get();
        EquivalenceGraph graph2 = graphs.get(Id.valueOf(2)).get();
        assertEquals(graph1, graph2);
        assertTrue(graph1 == graph2);
    }
    
    @Test
    public void testCreatingEquivalences() throws Exception {

        ResourceRef subject = new BrandRef(Id.valueOf(1), Publisher.BBC);
        BrandRef equiv = new BrandRef(Id.valueOf(2), Publisher.PA);
        Set<ResourceRef> assertedAdjacents = ImmutableSet.<ResourceRef>of(equiv);
        Set<Publisher> sources = ImmutableSet.of(Publisher.BBC, Publisher.PA);
        store.updateEquivalences(subject, assertedAdjacents, sources);
        
        ListenableFuture<OptionalMap<Id,EquivalenceGraph>> resolveIds = store.resolveIds(ImmutableList.of(Id.valueOf(1)));
        
        OptionalMap<Id, EquivalenceGraph> graphs = Futures.get(resolveIds, ResolveException.class);
        EquivalenceGraph graph = graphs.get(Id.valueOf(1)).get();
        assertTrue(graph.getAdjacents(Id.valueOf(1)).getEfferent().contains(equiv));
        assertTrue(graph.getAdjacents(Id.valueOf(2)).getAfferent().contains(subject));
    }
    
}
