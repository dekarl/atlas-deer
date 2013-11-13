package org.atlasapi.system.legacy;

import org.atlasapi.entity.ResourceLister;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;

import com.google.common.collect.FluentIterable;

public class LookupEntryLister implements ResourceLister<LookupEntry> {

    private MongoLookupEntryStore lookupStore;

    public LookupEntryLister(MongoLookupEntryStore lookupStore) {
        this.lookupStore = lookupStore;
    }

    @Override
    public FluentIterable<LookupEntry> list() {
        return lookupStore.all();
    }

}
