package org.atlasapi.system;

import org.atlasapi.equiv.EquivalenceRecordStore;
import org.atlasapi.media.common.Id;
import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.workers.AbstractWorker;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.system.bootstrap.LookupEntryChangeListener;

import com.google.common.collect.ImmutableSet;


public class LookupEntryReadWriter extends AbstractWorker {

    private final LookupEntryChangeListener changeListener;
    private final LookupEntryStore lookupStore;

    public LookupEntryReadWriter(LookupEntryStore lookupStore,
            EquivalenceRecordStore equivalenceRecordStore) {
        this.lookupStore = lookupStore;
        this.changeListener = new LookupEntryChangeListener(1, lookupStore, equivalenceRecordStore);
    }

    @Override
    public void process(EntityUpdatedMessage message) {
        Iterable<LookupEntry> entries = lookupStore.entriesForIds(ImmutableSet.of(Id.valueOf(message.getEntityId())));
        changeListener.onChange(entries);
    }
    
}
