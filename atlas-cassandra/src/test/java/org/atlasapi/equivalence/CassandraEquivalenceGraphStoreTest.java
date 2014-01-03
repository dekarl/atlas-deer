package org.atlasapi.equivalence;

import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.atlasapi.DatastaxCassandraService;
import org.atlasapi.content.BrandRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.ResolveException;
import org.atlasapi.media.entity.Publisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;

@RunWith(JUnit4.class)
public class CassandraEquivalenceGraphStoreTest {

    private DatastaxCassandraService service
        = new DatastaxCassandraService(ImmutableList.of("localhost"));
    private CassandraEquivalenceGraphStore store;
    private Session session;

    @Before
    public void setUp() {
        service.startAsync().awaitRunning();
        session = service.getCluster().connect();
        session.execute("CREATE KEYSPACE atlas_testing WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");
        session = service.getSession("atlas_testing");
        session.execute(
            "CREATE TABLE equivalence_graph_index (resource_id bigint, graph_id bigint, PRIMARY KEY (resource_id));"
        );
        session.execute("CREATE TABLE equivalence_graph (graph_id bigint, resource_id bigint, "
            + "resource_ref blob, created timestamp, "
            + "updated timestamp, efferents set<blob>, afferents set<blob>,"
            + "PRIMARY KEY (graph_id, resource_id));");
        store = new CassandraEquivalenceGraphStore(session , ConsistencyLevel.ONE, ConsistencyLevel.ONE);
    }
    
    @After
    public void tearDown() {
        session.execute("DROP KEYSPACE atlas_testing");
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
        System.out.println(graph);
    }
    
}
