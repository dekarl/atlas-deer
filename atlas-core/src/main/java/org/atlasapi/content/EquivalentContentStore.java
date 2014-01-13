package org.atlasapi.content;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalentsResolver;

//TODO Can this be EquivalentStore<T> in the future?
//TODO Given there's already EquivalentsResolver have EquivalentsWriter too as super-interfaces?
public interface EquivalentContentStore extends EquivalentsResolver<Content> {

    void updateEquivalences(Set<EquivalenceGraph> newGraphs) throws WriteException;
    
    void updateContent(ResourceRef ref) throws WriteException;
    
}
