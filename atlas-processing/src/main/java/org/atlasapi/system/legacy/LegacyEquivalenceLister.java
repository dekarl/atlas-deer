package org.atlasapi.system.legacy;

import org.atlasapi.entity.ResourceLister;
import org.atlasapi.equiv.EquivalenceRecord;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import com.google.common.collect.FluentIterable;

public class LegacyEquivalenceLister implements ResourceLister<EquivalenceRecord> {

    private final MongoLookupEntryStore lookupStore;
	private final LegacyEquivalenceTransformer transformer;

    public LegacyEquivalenceLister(MongoLookupEntryStore lookupStore) {
        this.lookupStore = lookupStore;
        this.transformer = new LegacyEquivalenceTransformer();
    }

    @Override
    public FluentIterable<EquivalenceRecord> list() {
        return FluentIterable.from(lookupStore.all()).transform(transformer);
    }

}
