package org.atlasapi.output;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.entity.Sourced;
import org.atlasapi.entity.Sourceds;
import org.atlasapi.equiv.ApplicationEquivalentsMerger;
import org.atlasapi.equiv.Equivalent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

public class StrategyBackedEquivalentsMerger<E extends Equivalent<E>>
        implements ApplicationEquivalentsMerger<E> {

    private final EquivalentsMergeStrategy<E> strategy;

    public StrategyBackedEquivalentsMerger(EquivalentsMergeStrategy<E> strategy) {
        this.strategy = checkNotNull(strategy);
    }

    @Override
    public <T extends E> List<T> merge(Iterable<T> equivalents, ApplicationSources sources) {
        if (!sources.isPrecedenceEnabled()) {
            return ImmutableList.copyOf(equivalents);
        }
        Ordering<Sourced> equivsOrdering = applicationEquivalentsOrdering(sources);
        ImmutableList<T> sortedEquivalents = equivsOrdering.immutableSortedCopy(equivalents);
        if (trivialMerge(sortedEquivalents)) {
            return sortedEquivalents;
        }
        T chosen = sortedEquivalents.get(0);
        ImmutableList<T> notChosen = sortedEquivalents.subList(1, sortedEquivalents.size());
        return ImmutableList.of(strategy.merge(chosen, notChosen, sources));
    }

    private boolean trivialMerge(ImmutableList<?> sortedEquivalents) {
        return sortedEquivalents.isEmpty() || sortedEquivalents.size() == 1;
    }

    private Ordering<Sourced> applicationEquivalentsOrdering(ApplicationSources sources) {
        return sources.publisherPrecedenceOrdering().onResultOf(Sourceds.toPublisher());
    }

}
