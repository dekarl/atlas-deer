package org.atlasapi.content;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphSerializer;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.util.SecondaryIndex;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class CassandraEquivalentContentStore extends AbstractEquivalentContentStore {

    private static final String EQUIVALENT_CONTENT_TABLE = "equivalent_content";
    private static final String SET_ID_KEY = "set_id";
    private static final String GRAPH_KEY = "graph";
    private static final String CONTENT_KEY = "content";
    
    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;

    private final SecondaryIndex index;
    
    private final EquivalenceGraphSerializer graphSerializer
        = new EquivalenceGraphSerializer();
    private final ContentSerializer contentSerializer
        = new ContentSerializer();

    private final Function<ByteBuffer, Content> toContent = new Function<ByteBuffer, Content>() {
        @Override
        public Content apply(ByteBuffer input) {
            try {
                ByteString bytes = ByteString.copyFrom(input);
                ContentProtos.Content buffer = ContentProtos.Content.parseFrom(bytes);
                return contentSerializer.deserialize(buffer);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    };
    
    public CassandraEquivalentContentStore(ContentResolver contentResolver,
            EquivalenceGraphStore graphStore, Session session, ConsistencyLevel read,
            ConsistencyLevel write) {
        super(contentResolver, graphStore);
        this.session = session;
        this.readConsistency = read;
        this.writeConsistency = write;
        this.index = new SecondaryIndex(session, "equivalent_content_index", read);
    }

    @Override
    public ListenableFuture<ResolvedEquivalents<Content>> resolveIds(Iterable<Id> ids,
            Set<Publisher> selectedSources, Set<Annotation> activeAnnotations) {
        ListenableFuture<ImmutableMap<Long, Long>> setsToResolve = index.lookup(Iterables.transform(ids,
                Id.toLongValue()));
        return Futures.transform(setsToResolve, toEquivalentsSets(selectedSources));
    }

    private AsyncFunction<Map<Long, Long>, ResolvedEquivalents<Content>> toEquivalentsSets(
            final Set<Publisher> selectedSources) {
        return new AsyncFunction<Map<Long, Long>, ResolvedEquivalents<Content>>() {
            @Override
            public ListenableFuture<ResolvedEquivalents<Content>> apply(Map<Long, Long> index)
                    throws Exception {
                return Futures.transform(resultOf(selectSetsQuery(index.values())), 
                        toEquivalentsSets(index, selectedSources));
            }
        };
    }

    private Function<ResultSet, ResolvedEquivalents<Content>> toEquivalentsSets(
            final Map<Long, Long> index, final Set<Publisher> selectedSources) {
        return new Function<ResultSet, ResolvedEquivalents<Content>>() {
            @Override
            public ResolvedEquivalents<Content> apply(ResultSet setRows) {
                Multimap<Long, Content> sets = deserialize(setRows, selectedSources);
                ResolvedEquivalents.Builder<Content> results = ResolvedEquivalents.builder();
                for (Entry<Long, Long> id : index.entrySet()) {
                    results.putEquivalents(Id.valueOf(id.getKey()), sets.get(id.getValue()));
                }
                return results.build();
            }
        };
    }

    private Multimap<Long, Content> deserialize(ResultSet setRows, Set<Publisher> selectedSources) {
        ImmutableSetMultimap.Builder<Long, Content> sets = ImmutableSetMultimap.builder();
        for (Row row : setRows) {
            EquivalenceGraph graph = graphSerializer.deserialize(row.getBytes(GRAPH_KEY));
            Map<Long, ByteBuffer> contentMap = row.getMap(CONTENT_KEY, Long.class, ByteBuffer.class);
            sets.putAll(row.getLong(SET_ID_KEY), content(contentMap, graph, selectedSources));
        }
        return sets.build();
    }

    
    private Iterable<Content> content(Map<Long, ByteBuffer> contentMap,
            EquivalenceGraph graph, Set<Publisher> selectedSources) {
        return enabledIds(graph, selectedSources)
                .transform(Functions.forMap(contentMap))
                .transform(toContent);
    }

    private FluentIterable<Long> enabledIds(EquivalenceGraph graph, Set<Publisher> selectedSources) {
        return FluentIterable.from(graph.values())
                .filter(Sourceds.sourceFilter(selectedSources))
                .transform(Identifiables.toId())
                .transform(Id.toLongValue());
    }

    private ResultSetFuture resultOf(Query query) {
        return session.executeAsync(query.setConsistencyLevel(readConsistency));
    }
    
    private Query selectSetsQuery(Iterable<Long> keys) {
        return QueryBuilder.select().all()
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(in(SET_ID_KEY, ImmutableSet.copyOf(keys).toArray()));
    }

    @Override
    protected void updateEquivalences(
            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {
        ImmutableSet<Entry<EquivalenceGraph, Collection<Content>>> graphAndContentSet
            = graphsAndContent.asMap().entrySet();
        ArrayList<Statement> updates = Lists.newArrayList();
        for (Entry<EquivalenceGraph, Collection<Content>> graphAndContent : graphAndContentSet) {
            updates.add(insertIntoEquivalentContentTable(graphAndContent));
            updates.addAll(insertIntoIndex(graphAndContent.getKey()));
        }
        session.execute(batch(updates.toArray(new Statement[updates.size()]))
                .setConsistencyLevel(writeConsistency));
    }

    private Statement insertIntoEquivalentContentTable(
            Entry<EquivalenceGraph, Collection<Content>> graphAndContent) {
        EquivalenceGraph graph = graphAndContent.getKey();
        Collection<Content> content = graphAndContent.getValue();
        return insertInto(EQUIVALENT_CONTENT_TABLE)
                .value(SET_ID_KEY, setId(graph))
                .value(GRAPH_KEY, graphSerializer.serialize(graph))
                .value(CONTENT_KEY, serialize(content));
    }

    private Map<Long,ByteBuffer> serialize(Collection<Content> contents) {
        Map<Long, ByteBuffer> serialized = Maps.newHashMapWithExpectedSize(contents.size());
        for (Content content : contents) {
            serialized.put(content.getId().longValue(), serialize(content));
        }
        return serialized;
    }

    private ByteBuffer serialize(Content content) {
        ContentProtos.Content contentBuffer = contentSerializer.serialize(content);
        return ByteBuffer.wrap(contentBuffer.toByteArray());
    }

    private Collection<Statement> insertIntoIndex(EquivalenceGraph graph) {
        Long setId = setId(graph);
        Iterable<Long> setMembers = Iterables.transform(graph.keySet(),Id.toLongValue());
        return index.insertStatements(setMembers, setId);
    }

    private long setId(EquivalenceGraph graph) {
        return Ordering.natural().min(graph.keySet()).longValue();
    }

    @Override
    protected void updateInSet(EquivalenceGraph graph, Content content) {
        session.execute(update(EQUIVALENT_CONTENT_TABLE)
                .where(QueryBuilder.eq(SET_ID_KEY, setId(graph)))
                .with(QueryBuilder.put(CONTENT_KEY, content.getId().longValue(), serialize(content)))
                .setConsistencyLevel(writeConsistency));
    }

}
