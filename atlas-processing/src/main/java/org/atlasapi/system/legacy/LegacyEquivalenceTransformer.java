package org.atlasapi.system.legacy;

import java.util.Set;

import org.atlasapi.entity.Id;
import org.atlasapi.equiv.EquivalenceRecord;
import org.atlasapi.equiv.EquivalenceRef;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class LegacyEquivalenceTransformer extends
        BaseLegacyResourceTransformer<LookupEntry, EquivalenceRecord> {

    @Override
    public EquivalenceRecord apply(LookupEntry input) {
        return translate(input);
    }

    private EquivalenceRecord translate(LookupEntry change) {
        return new EquivalenceRecord(
                new EquivalenceRef(Id.valueOf(change.id()), change.lookupRef().publisher()),
                translate(change.directEquivalents()),
                translate(change.explicitEquivalents()),
                translate(change.equivalents()),
                change.created(),
                change.updated());
    }

    private Iterable<EquivalenceRef> translate(Set<LookupRef> refs) {
        return Collections2.transform(refs, new Function<LookupRef, EquivalenceRef>() {

            @Override
            public EquivalenceRef apply(LookupRef input) {
                return new EquivalenceRef(Id.valueOf(input.id()), input.publisher());
            }
        });
    }

}
