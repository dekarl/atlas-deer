package org.atlasapi.equiv;

import java.util.Set;

import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;

public interface Equivalent<E extends Equivalent<E>> extends Identifiable, Sourced {

    Set<EquivalenceRef> getEquivalentTo();
    
    //I don't like this method.
    E copyWithEquivalentTo(Iterable<EquivalenceRef> equivalents);
    
}
