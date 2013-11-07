package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.equiv.EquivalenceRecord;
import org.atlasapi.equiv.EquivalenceRecordStore;
import org.atlasapi.equiv.EquivalenceRef;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class LookupEntryChangeListener extends AbstractMultiThreadedChangeListener<LookupEntry> {

    private final LookupEntryStore lookupStore;
    private final EquivalenceRecordStore equivStore;
    
    private final LoadingCache<LookupRef, Id> idCache = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<LookupRef, Id>(){
                @Override
                public Id load(LookupRef key) throws Exception {
                    Iterable<LookupEntry> entries = lookupStore.entriesForCanonicalUris(
                            ImmutableList.of(key.id()));
                    LookupEntry entry = Iterables.getOnlyElement(entries, null);
                    return checkNotNull(entry, "no entry for " + key.id()).id();
                }
            });

    public LookupEntryChangeListener(int concurrencyLevel, LookupEntryStore store,
            EquivalenceRecordStore equivStore) {
        super(concurrencyLevel);
        this.lookupStore = store;
        this.equivStore = equivStore;
    }

    @Override
    protected void onChange(LookupEntry change) {
        idCache.put(change.lookupRef(), change.id());
        equivStore.writeRecords(ImmutableList.of(translate(change)));
    }

    private EquivalenceRecord translate(LookupEntry change) {
        return new EquivalenceRecord(
            new EquivalenceRef(change.id(), change.lookupRef().publisher()),
            translate(change.directEquivalents()),
            translate(change.explicitEquivalents()),
            translate(change.equivalents()),
            change.created(),
            change.updated()
        );
    }

    private Iterable<EquivalenceRef> translate(Set<LookupRef> refs) {
        return Collections2.transform(refs, new Function<LookupRef, EquivalenceRef>(){
            @Override
            public EquivalenceRef apply(LookupRef input) {
                return translate(input);
            }
        });
    }
    
    private EquivalenceRef translate(LookupRef input) {
        return new EquivalenceRef(idCache.getUnchecked(input), input.publisher());
    }

}
