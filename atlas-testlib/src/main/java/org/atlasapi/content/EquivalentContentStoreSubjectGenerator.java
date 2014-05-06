package org.atlasapi.content;

import org.atlasapi.equivalence.EquivalenceGraphStore;

public interface EquivalentContentStoreSubjectGenerator {

    EquivalentContentStore getEquivalentContentStore();

    EquivalenceGraphStore getEquivalenceGraphStore();

    ContentStore getContentStore();

}
