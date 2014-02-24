package org.atlasapi.equivalence;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.entity.util.StoreException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.MessageSender;
import org.atlasapi.util.GroupLock;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.MoreSets;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

public abstract class AbstractEquivalenceGraphStore implements EquivalenceGraphStore {

    private static final int TIMEOUT = 1;
    private static final TimeUnit TIMEOUT_UNITS = TimeUnit.MINUTES;
    
    private static final int maxSetSize = 150;
    
    private final MessageSender messageSender;
    
    public AbstractEquivalenceGraphStore(MessageSender messageSender) {
        this.messageSender = checkNotNull(messageSender);
    }
    
    @Override
    public final Optional<ImmutableSet<EquivalenceGraph>> updateEquivalences(ResourceRef subject,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) throws WriteException {

        ImmutableSet<Id> newAdjacents
            = ImmutableSet.copyOf(Iterables.transform(assertedAdjacents, Identifiables.toId()));
        Set<Id> subjectAndAdjacents = MoreSets.add(newAdjacents, subject.getId());
        Set<Id> transitiveSetsIds = null;
        try {
            synchronized (lock()) {
                while((transitiveSetsIds = tryLockAllIds(subjectAndAdjacents)) == null) {
                    lock().unlock(subjectAndAdjacents);
                    lock().wait();
                }
            }

            Optional<ImmutableSet<EquivalenceGraph>> updated
                = updateGraphs(subject, ImmutableSet.<ResourceRef>copyOf(assertedAdjacents), sources);
            if (updated.isPresent()) {
                sendUpdateMessage(subject, updated);
            }
            return updated;
            
        } catch(OversizeTransitiveSetException otse) {
            log().info(String.format("Oversize set: %s + %s: %s", 
                subject, newAdjacents, otse.getMessage()));
            return Optional.absent();
        } catch(InterruptedException e) {
            log().error(String.format("%s: %s", subject, newAdjacents), e);
            return Optional.absent();
        } catch (StoreException e) {
            Throwables.propagateIfPossible(e, WriteException.class);
            throw new WriteException(e);
        } finally {
            synchronized (lock()) {
                lock().unlock(subjectAndAdjacents);
                if (transitiveSetsIds != null) {
                    lock().unlock(transitiveSetsIds);
                }
                lock().notifyAll();
            }
        }
        
    }

    private void sendUpdateMessage(ResourceRef subject, Optional<ImmutableSet<EquivalenceGraph>> updated)  {
        try {
            messageSender.sendMessage(new EquivalenceGraphUpdateMessage(
                UUID.randomUUID().toString(),
                Timestamp.of(DateTime.now(DateTimeZones.UTC)), 
                updated.get()
            ));
        } catch (IOException e) {
            log().warn("messaging failed for equivalence update of " + subject, e);
        }
    }
    
    protected abstract GroupLock<Id> lock();

    private Set<Id> tryLockAllIds(Set<Id> adjacentsIds) throws InterruptedException, StoreException {
        if (!lock().tryLock(adjacentsIds)) {
            return null;
        }
        Iterable<Id> transitiveIds = transitiveIdsToLock(adjacentsIds);
        Set<Id> allIds = ImmutableSet.copyOf(Iterables.concat(transitiveIds, adjacentsIds));
        if (allIds.size() > maxSetSize) {
            throw new OversizeTransitiveSetException(allIds.size());
        }
        Iterable<Id> idsToLock = Iterables.filter(allIds, not(in(adjacentsIds)));
        return lock().tryLock(ImmutableSet.copyOf(idsToLock)) ? allIds : null;
    }

    private Iterable<Id> transitiveIdsToLock(Set<Id> adjacentsIds) throws StoreException {
        return Iterables.concat(Iterables.transform(get(resolveIds(adjacentsIds)).values(),
            new Function<Optional<EquivalenceGraph>, Set<Id>>() {
                @Override
                public Set<Id> apply(Optional<EquivalenceGraph> input) {
                    return input.isPresent() ? input.get().getAdjacencyList().keySet() : ImmutableSet.<Id>of();
                }
            }
        ));
    }

    private Optional<ImmutableSet<EquivalenceGraph>> updateGraphs(ResourceRef subject, 
            ImmutableSet<ResourceRef> assertedAdjacents, Set<Publisher> sources) throws StoreException {
        
        EquivalenceGraph subjGraph = existingGraph(subject).or(EquivalenceGraph.valueOf(subject));
        Adjacents subAdjs = subjGraph.getAdjacents(subject);
        
        if(!changeInAdjacents(subAdjs, assertedAdjacents, sources)) {
            log().debug("{}: no change in neighbours: {}", subject, assertedAdjacents);
            return Optional.absent();
        }
        
        Map<Id, Adjacents> updatedAdjacents = updateAdjacencies(subject,
                subjGraph.getAdjacencyList().values(), assertedAdjacents, sources);
        
        return Optional.of(store(recomputeGraphs(updatedAdjacents)));
    }

    private ImmutableSet<EquivalenceGraph> recomputeGraphs(Map<Id, Adjacents> updatedAdjacents) {
        Function<Identifiable, Adjacents> toAdjs
            = Functions.compose(Functions.forMap(updatedAdjacents), Identifiables.toId());
        
        Map<Id, Set<Adjacents>> updated = Maps.newHashMap();
        for (Adjacents adj : updatedAdjacents.values()) {
            Set<Adjacents> transitiveSet = transitiveSet(updated, adj);
            transitiveSet.addAll(Collections2.transform(adj.getAdjacent(), toAdjs));
            for (ResourceRef r : adj.getAdjacent()) {
                updated.put(r.getId(), transitiveSet);
            }
        }
        ImmutableSet.Builder<EquivalenceGraph> updatedGraphs = ImmutableSet.builder();
        for (Set<Adjacents> set : ImmutableSet.copyOf(updated.values())) {
            updatedGraphs.add(EquivalenceGraph.valueOf(set));
        }
        return updatedGraphs.build();
    }

    private Set<Adjacents> transitiveSet(Map<Id, Set<Adjacents>> updated, Adjacents adj) {
        Set<Adjacents> transitiveSet = null;
        Iterator<ResourceRef> efferent = adj.getAdjacent().iterator();
        while(transitiveSet == null && efferent.hasNext()) {
            transitiveSet = updated.get(efferent.next().getId());
        }
        transitiveSet = Objects.firstNonNull(transitiveSet, Sets.<Adjacents>newHashSet());
        return transitiveSet;
    }

    private Map<Id, Adjacents> updateAdjacencies(ResourceRef subject,
            Iterable<Adjacents> subjAdjacencies, ImmutableSet<ResourceRef> assertedAdjacents,
            Set<Publisher> sources) throws StoreException {
        ImmutableMap.Builder<Id, Adjacents> updated = ImmutableMap.builder();
        ImmutableSet<Adjacents> allAdjacents = currentTransitiveAdjacents(assertedAdjacents)
                .addAll(subjAdjacencies).build();
        for (Adjacents adj : allAdjacents) {
            updated.put(adj.getId(), updateAdjacents(adj, subject, assertedAdjacents, sources));
        }
        return updated.build();
    }

    private Adjacents updateAdjacents(Adjacents adj, ResourceRef subject, 
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        Adjacents result = adj;
        if (subject.equals(adj.getRef())) {
            result = updateSubjectAdjacents(adj, assertedAdjacents, sources);
        } else if (sources.contains(adj.getPublisher())) {
            if (assertedAdjacents.contains(adj.getRef())) {
                result = adj.copyWithAfferent(subject);
            } else if (adj.hasAfferentAdjacent(subject)) {
                result = adj.copyWithoutAfferent(subject);
            }
        }
        return result;
    }

    private Adjacents updateSubjectAdjacents(Adjacents subj,
            Set<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        ImmutableSet.Builder<ResourceRef> updatedEfferents = ImmutableSet.<ResourceRef>builder()
            .add(subj.getRef())
            .addAll(assertedAdjacents)
            .addAll(Sets.filter(subj.getEfferent(), Predicates.not(Sourceds.sourceFilter(sources))));
        return subj.copyWithEfferents(updatedEfferents.build());
    }

    private ImmutableSet.Builder<Adjacents> currentTransitiveAdjacents(Set<ResourceRef> adjacents)
            throws StoreException {
        Iterable<Id> adjacentsIds = Iterables.transform(adjacents, Identifiables.toId());
        OptionalMap<Id,EquivalenceGraph> resolved = get(resolveIds(adjacentsIds));
        ImmutableSet.Builder<Adjacents> result = ImmutableSet.<Adjacents>builder();
        for (ResourceRef ref : adjacents) {
            Optional<EquivalenceGraph> g = resolved.get(ref.getId());
            if (g.isPresent()) {
                result.addAll(g.get().getAdjacencyList().values());
            } else {
                result.add(Adjacents.valueOf(ref));
            }
        }
        return result;
    }
    
    private boolean changeInAdjacents(Adjacents subjAdjs,
            ImmutableSet<ResourceRef> assertedAdjacents, Set<Publisher> sources) {
        Set<ResourceRef> currentNeighbours
            = Sets.filter(subjAdjs.getEfferent(), Sourceds.sourceFilter(sources));
        Set<ResourceRef> subjectAndAsserted = MoreSets.add(assertedAdjacents, subjAdjs.getRef());
        boolean change = !currentNeighbours.equals(subjectAndAsserted);
        if (change) {
            log().debug("Equivalence change: {} -> {}", currentNeighbours, subjectAndAsserted);
        }
        return change;
    }
    
    private Optional<EquivalenceGraph> existingGraph(ResourceRef subject) throws StoreException {
        return get(resolveIds(ImmutableSet.of(subject.getId()))).get(subject.getId());
    }

    private <F> F get(ListenableFuture<F> resolved) throws WriteException {
        return Futures.get(resolved, TIMEOUT, TIMEOUT_UNITS, WriteException.class);
    }

    private final ImmutableSet<EquivalenceGraph> store(ImmutableSet<EquivalenceGraph> graphs) {
        doStore(graphs);
        return graphs;
    }
    
    protected abstract void doStore(ImmutableSet<EquivalenceGraph> graphs);
    
    protected abstract Logger log();
    
    private static class OversizeTransitiveSetException extends RuntimeException {
        
        private int size;

        public OversizeTransitiveSetException(int size)  {
            this.size = size;
        }

        @Override
        public String getMessage() {
            return String.valueOf(size);
        }
        
    }
    
}
