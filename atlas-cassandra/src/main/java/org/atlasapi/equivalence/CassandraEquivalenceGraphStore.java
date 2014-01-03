package org.atlasapi.equivalence;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceRefSerializer;
import org.atlasapi.entity.util.ResolveException;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.util.GroupLock;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

public final class CassandraEquivalenceGraphStore extends AbstractEquivalenceGraphStore {

    private static final String EQUIVALENCE_GRAPHS_TABLE = "equivalence_graph";
    private static final String EQUIVALENCE_GRAPH_INDEX_TABLE = "equivalence_graph_index";

    private static final String GRAPH_ID_KEY = "graph_id";
    private static final String RESOURCE_ID_KEY = "resource_id";
    private static final String RESOURCE_REF_KEY = "resource_ref";
    private static final String CREATED_KEY = "created";
    private static final String UPDATED_KEY = "updated";
    private static final String EFFERENTS_KEY = "efferents";
    private static final String AFFERENTS_KEY = "afferents";
    
    private static final GroupLock<Id> lock = GroupLock.natural();
    private static final Logger log = LoggerFactory.getLogger(CassandraEquivalenceGraphStore.class);
    
    private final ResourceRefSerializer serializer = new ResourceRefSerializer();
    
    private final Session session;
    private final ConsistencyLevel read;
    private final ConsistencyLevel write;

    private final PreparedStatement preparedIndexInsert;
    private final PreparedStatement preparedAdjacencyInsert;
    
    public CassandraEquivalenceGraphStore(Session session, ConsistencyLevel read, ConsistencyLevel write) {
        this.session = session;
        this.read = read;
        this.write = write;
        this.preparedIndexInsert = session.prepare(indexInsertQuery());
        this.preparedAdjacencyInsert = session.prepare(adjacencyInsertQuery());
    }

    private Insert indexInsertQuery() {
        return insertInto(EQUIVALENCE_GRAPH_INDEX_TABLE)
            .value(RESOURCE_ID_KEY, bindMarker())
            .value(GRAPH_ID_KEY, bindMarker());
    }

    private Insert adjacencyInsertQuery() {
        return insertInto(EQUIVALENCE_GRAPHS_TABLE)
            .value(GRAPH_ID_KEY, bindMarker())
            .value(RESOURCE_ID_KEY, bindMarker())
            .value(RESOURCE_REF_KEY, bindMarker())
            .value(CREATED_KEY, bindMarker())
            .value(UPDATED_KEY, bindMarker())
            .value(EFFERENTS_KEY, bindMarker())
            .value(AFFERENTS_KEY, bindMarker());
    }
    
    private final Function<ResultSet, Multimap<Long, Row>> toRowGraphIndex
        = new Function<ResultSet, Multimap<Long, Row>>() {
            @Override
            public Multimap<Long, Row> apply(ResultSet rows) {
                return Multimaps.index(rows, new Function<Row, Long>() {
                    @Override
                    public Long apply(Row input) {
                        return input.getLong(GRAPH_ID_KEY);
                    }
                });
            }
        };

    private final AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>> toGraphs
        = new AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>>() {
            @Override
            public ListenableFuture<OptionalMap<Id, EquivalenceGraph>> apply(Map<Id, Long> idIndex) {
                ResultSetFuture graphRows = resultOf(queryForGraphRows(idIndex));
                ListenableFuture<Multimap<Long, Row>> indexedGraphRows
                    = Futures.transform(graphRows, toRowGraphIndex);
                return Futures.transform(indexedGraphRows, toIdGraphIndex(idIndex));
            }
        };

    private Function<Multimap<Long, Row>, OptionalMap<Id, EquivalenceGraph>> toIdGraphIndex(final Map<Id, Long> idIndex) {
        return new Function<Multimap<Long, Row>, OptionalMap<Id, EquivalenceGraph>>() {
            @Override
            public OptionalMap<Id, EquivalenceGraph> apply(Multimap<Long, Row> rowGraphIndex){
                try {
                    ImmutableMap.Builder<Long, EquivalenceGraph> graphIndex = ImmutableMap.builder();
                    for (Entry<Long, Collection<Row>> graphRow : rowGraphIndex.asMap().entrySet()) {
                        graphIndex.put(graphRow.getKey(), transformToGraph(graphRow.getValue()));
                    }
                    return ImmutableOptionalMap.fromMap(Maps.transformValues(idIndex, Functions.forMap(graphIndex.build(), null)));
                } catch (ResolveException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
    
    private Statement queryForGraphRows(final Map<Id, Long> idIndex) {
        return select().all()
                .from(EQUIVALENCE_GRAPHS_TABLE)
                .where(in(GRAPH_ID_KEY, idIndex.values().toArray()))
                .orderBy(QueryBuilder.asc(RESOURCE_ID_KEY))
                .setConsistencyLevel(read);
    }

    private final Function<ResultSet, Map<Id, Long>> toGraphIdIndex
        = new Function<ResultSet, Map<Id, Long>>() {
            @Override
            public ImmutableMap<Id, Long> apply(ResultSet rows) {
                ImmutableMap.Builder<Id, Long> idIndex = ImmutableMap.builder();
                for (Row row : rows) {
                    Id resourceId = Id.valueOf(row.getLong(RESOURCE_ID_KEY));
                    long graphId = row.getLong(GRAPH_ID_KEY);
                    idIndex.put(resourceId, graphId);
                }
                return idIndex.build();
            }
        };
        
    @Override
    public ListenableFuture<OptionalMap<Id, EquivalenceGraph>> resolveIds(Iterable<Id> ids) {
        ListenableFuture<Map<Id, Long>> graphIdIndex = resolveToGraphIds(ids);
        return Futures.transform(graphIdIndex, toGraphs);
    }

    private ListenableFuture<Map<Id,Long>> resolveToGraphIds(Iterable<Id> ids) {
        return Futures.transform(resultOf(queryForGraphIds(ids)), toGraphIdIndex);
    }

    private ResultSetFuture resultOf(Statement query) {
        return session.executeAsync(query);
    }

    private Statement queryForGraphIds(Iterable<Id> ids) {
        Object[] lids = FluentIterable.from(ids).transform(Id.toLongValue()).toArray(Long.class);
        return QueryBuilder
            .select(RESOURCE_ID_KEY,GRAPH_ID_KEY)
            .from(EQUIVALENCE_GRAPH_INDEX_TABLE)
            .where(in(RESOURCE_ID_KEY, lids))
            .setConsistencyLevel(read);
    }

    private EquivalenceGraph transformToGraph(Collection<Row> rows) throws ResolveException {
        DateTime updated = null;
        ImmutableMap.Builder<Id, Adjacents> adjacencyList = ImmutableMap.builder();
        for (Row row : rows) {
            updated = new DateTime(row.getDate(UPDATED_KEY));
            Id id = Id.valueOf(row.getLong(RESOURCE_ID_KEY));
            Adjacents adjs = adjacentsFrom(row);
            adjacencyList.put(id, adjs);
        }
        return new EquivalenceGraph(adjacencyList.build(), updated);
    }

    private Adjacents adjacentsFrom(Row row) throws ResolveException {
        ResourceRef subject = deserializeToRef(row.getBytes(RESOURCE_REF_KEY));
        DateTime created = new DateTime(row.getDate(CREATED_KEY));
        Set<ResourceRef> efferent = fromByteBuffers(row.getSet(EFFERENTS_KEY, ByteBuffer.class));
        Set<ResourceRef> afferent = fromByteBuffers(row.getSet(AFFERENTS_KEY, ByteBuffer.class));
        return new Adjacents(subject, created, efferent, afferent);
    }

    private ResourceRef deserializeToRef(ByteBuffer blob) throws ResolveException {
        try {
            ByteString bytes = ByteString.copyFrom(blob);
            return serializer.deserialize(CommonProtos.Reference.parseFrom(bytes));
        } catch (InvalidProtocolBufferException ipbe) {
            throw new ResolveException(blob.toString(), ipbe);
        }
    }

    private Set<ResourceRef> fromByteBuffers(Set<ByteBuffer> blobs) throws ResolveException {
        ImmutableSet.Builder<ResourceRef> refs = ImmutableSet.builder();
        for (ByteBuffer blob : blobs) {
            refs.add(deserializeToRef(blob));
        }
        return refs.build();
    }

    @Override
    protected void doStore(ImmutableSet<EquivalenceGraph> graphs) {
        BatchStatement updateBatch = new BatchStatement();
        for (EquivalenceGraph graph : graphs) {
            Long graphId = lowestId(graph); 
            for (Entry<Id, Adjacents> adjacency : graph.entrySet()) {
                updateBatch.add(preparedIndexInsert.bind(adjacency.getKey().longValue(),graphId));
                updateBatch.add(graphAdjacencyUpdate(graphId, graph.getUpdated(), adjacency));
            }
        }
        session.execute(updateBatch.setConsistencyLevel(write));
    }
    
    private Long lowestId(EquivalenceGraph graph) {
        return Ordering.natural().min(graph.keySet()).longValue();
    }

    private BoundStatement graphAdjacencyUpdate(Long graphId, DateTime updated, Entry<Id, Adjacents> adjacency) {
        Adjacents adjacents = adjacency.getValue();
        ResourceRef ref = adjacents.getRef();
        return preparedAdjacencyInsert.bind(
            graphId, 
            ref.getId().longValue(), 
            ByteBuffer.wrap(serializer.serialize(ref).toByteArray()),
            adjacents.getCreated().toDate(),
            updated.toDate(),
            serialize(adjacents.getEfferent()),
            serialize(adjacents.getAfferent())
        );
    }

    private Set<ByteBuffer> serialize(ImmutableSet<ResourceRef> refs) {
        HashSet<ByteBuffer> serialized = Sets.newHashSetWithExpectedSize(refs.size());
        for (ResourceRef ref : refs) {
            serialized.add(ByteBuffer.wrap(serializer.serialize(ref).toByteArray()));
        }
        return serialized;
    }

    @Override
    protected GroupLock<Id> lock() {
        return lock;
    }

    @Override
    protected Logger log() {
        return log;
    }

}
