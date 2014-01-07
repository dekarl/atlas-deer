package org.atlasapi.equivalence;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.serialization.protobuf.EquivProtos;
import org.atlasapi.serialization.protobuf.EquivProtos.EquivGraph;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
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
import com.google.common.collect.Ordering;
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

    private static final String RESOURCE_ID_KEY = "resource_id";
    private static final String GRAPH_ID_KEY = "graph_id";
    private static final String GRAPH_KEY = "graph";
    
    private static final GroupLock<Id> lock = GroupLock.natural();
    private static final Logger log = LoggerFactory.getLogger(CassandraEquivalenceGraphStore.class);
    
    private final EquivalenceGraphSerializer serializer = new EquivalenceGraphSerializer();
    
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
            .value(GRAPH_KEY, bindMarker());
    }
    
    private final Function<ResultSet, Map<Long, EquivalenceGraph>> toGraph
        = new Function<ResultSet, Map<Long, EquivalenceGraph>>() {
            @Override
            public Map<Long, EquivalenceGraph> apply(ResultSet rows) {
                try {
                    ImmutableMap.Builder<Long, EquivalenceGraph> idGraph = ImmutableMap.builder();
                    for (Row row : rows) {
                        Long graphId = row.getLong(GRAPH_ID_KEY);
                        ByteString bytes = ByteString.copyFrom(row.getBytes(GRAPH_KEY));
                        EquivGraph buffer = EquivProtos.EquivGraph.parseFrom(bytes);
                        idGraph.put(graphId, serializer.deserialize(buffer));
                    }
                    return idGraph.build();
                } catch (InvalidProtocolBufferException ipbe) {
                    throw new RuntimeException(ipbe);
                }
            }

        };

    private final AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>> toGraphs
        = new AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>>() {
            @Override
            public ListenableFuture<OptionalMap<Id, EquivalenceGraph>> apply(Map<Id, Long> idIndex) {
                ResultSetFuture graphRows = resultOf(queryForGraphRows(idIndex));
                return Futures.transform(Futures.transform(graphRows, toGraph), toIdGraphIndex(idIndex));
            }
        };

    private Function<Map<Long, EquivalenceGraph>, OptionalMap<Id, EquivalenceGraph>> toIdGraphIndex(final Map<Id, Long> idIndex) {
        return new Function<Map<Long, EquivalenceGraph>, OptionalMap<Id, EquivalenceGraph>>() {
            @Override
            public OptionalMap<Id, EquivalenceGraph> apply(Map<Long, EquivalenceGraph> rowGraphIndex){
                return ImmutableOptionalMap.fromMap(Maps.transformValues(idIndex, Functions.forMap(rowGraphIndex, null)));
            }
        };
    }
    
    private Statement queryForGraphRows(final Map<Id, Long> idIndex) {
        return select().all()
                .from(EQUIVALENCE_GRAPHS_TABLE)
                .where(in(GRAPH_ID_KEY, idIndex.values().toArray()))
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

    @Override
    protected void doStore(ImmutableSet<EquivalenceGraph> graphs) {
        BatchStatement updateBatch = new BatchStatement();
        for (EquivalenceGraph graph : graphs) {
            Long graphId = lowestId(graph); 
            for (Entry<Id, Adjacents> adjacency : graph.entrySet()) {
                updateBatch.add(preparedIndexInsert.bind(adjacency.getKey().longValue(), graphId));
                ByteBuffer serializedGraph = ByteBuffer.wrap(serializer.serialize(graph).toByteArray());
                updateBatch.add(preparedAdjacencyInsert.bind(graphId, serializedGraph));
            }
        }
        session.execute(updateBatch.setConsistencyLevel(write));
    }

    private Long lowestId(EquivalenceGraph graph) {
        return Ordering.natural().min(graph.keySet()).longValue();
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
