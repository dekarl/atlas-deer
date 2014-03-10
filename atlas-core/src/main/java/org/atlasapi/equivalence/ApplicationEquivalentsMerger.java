package org.atlasapi.equivalence;

import java.util.List;

import org.atlasapi.application.ApplicationSources;

public interface ApplicationEquivalentsMerger<E extends Equivalable<E>> {

    <T extends E> List<T> merge(Iterable<T> equivalents, ApplicationSources sources);
    
}
