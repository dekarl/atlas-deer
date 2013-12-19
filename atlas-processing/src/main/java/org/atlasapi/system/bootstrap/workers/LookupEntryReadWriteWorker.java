package org.atlasapi.system.bootstrap.workers;

import org.atlasapi.equivalence.EquivalenceRecordStore;
import org.atlasapi.messaging.BaseWorker;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.system.bootstrap.EquivalenceBootstrapListener;
import org.atlasapi.system.legacy.LegacyEquivalenceTransformer;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


public class LookupEntryReadWriteWorker extends BaseWorker<ResourceUpdatedMessage> {

    private final EquivalenceBootstrapListener changeListener;
    private final LookupEntryStore lookupStore;
    private final LegacyEquivalenceTransformer transformer = new LegacyEquivalenceTransformer();

    public LookupEntryReadWriteWorker(LookupEntryStore lookupStore,
            EquivalenceRecordStore equivalenceRecordStore) {
        this.lookupStore = lookupStore;
        this.changeListener = new EquivalenceBootstrapListener(1, equivalenceRecordStore);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        long id = message.getUpdatedResource().getId().longValue();
        Iterable<LookupEntry> entries = lookupStore.entriesForIds(ImmutableSet.of(id));
        changeListener.onChange(Iterables.transform(entries, transformer));
    }
    
}
