package org.atlasapi.equivalence;

import java.util.Set;

import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;

@Deprecated
public interface Equivalable<E extends Equivalable<E>> extends Identifiable, Sourced {

    Set<EquivalenceRef> getEquivalentTo();
    
    //I don't like this method.
    E copyWithEquivalentTo(Iterable<EquivalenceRef> equivalents);
    
}
