package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;

public abstract class AbstractEquivalentContentStore implements EquivalentContentStore {

    private final ContentResolver contentResolver;
    private final EquivalenceGraphStore graphStore;

    public AbstractEquivalentContentStore(ContentResolver contentResolver, EquivalenceGraphStore graphStore) {
        this.contentResolver = checkNotNull(contentResolver);
        this.graphStore = checkNotNull(graphStore);
    }

    @Override
    public void updateEquivalences(Set<EquivalenceGraph> graphs) throws WriteException {
        ImmutableSetMultimap.Builder<EquivalenceGraph, Content> graphsAndContent
            = ImmutableSetMultimap.builder();
        Function<Id, Optional<Content>> toContent = Functions.forMap(contentFor(graphs));
        for (EquivalenceGraph graph : graphs) {
            Iterable<Optional<Content>> content = Collections2.transform(graph.keySet(), toContent);
            graphsAndContent.putAll(graph, Optional.presentInstances(content));
        }
        updateEquivalences(graphsAndContent.build());
    }

    protected abstract void updateEquivalences(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent);

    private OptionalMap<Id, Content> contentFor(Set<EquivalenceGraph> graphs) throws WriteException {
        Iterable<Id> ids = Iterables.concat(Iterables.transform(graphs,
            new Function<EquivalenceGraph, Set<Id>>() {
                @Override
                public Set<Id> apply(EquivalenceGraph input) {
                    return input.keySet();
                }
            }
        ));
        return resolveIds(ids);
    }

    private OptionalMap<Id, Content> resolveIds(Iterable<Id> ids) throws WriteException {
        return get(contentResolver.resolveIds(ids)).toMap();
    }

    private <T> T get(ListenableFuture<T> future) throws WriteException {
        return Futures.get(future, 1, TimeUnit.MINUTES, WriteException.class);
    }

    @Override
    public void updateContent(ResourceRef ref) throws WriteException {
        ImmutableList<Id> ids = ImmutableList.of(ref.getId());
        ListenableFuture<OptionalMap<Id, EquivalenceGraph>> graphs = graphStore.resolveIds(ids);
        OptionalMap<Id, Content> content = resolveIds(ids);
        EquivalenceGraph graph = get(graphs).get(ref.getId()).or(EquivalenceGraph.valueOf(ref));
        updateInGraph(graph, content.get(ref.getId()).get());
    }

    protected abstract void updateInGraph(EquivalenceGraph graph, Content content);

}
