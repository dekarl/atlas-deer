package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.Sourceds;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equiv.EquivalenceRecordStore;
import org.atlasapi.equiv.EquivalenceRecordWriter;
import org.atlasapi.equiv.EquivalenceRef;
import org.atlasapi.equiv.TransitiveEquivalenceRecordWriter;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class EquivalenceWritingContentStore extends ForwardingContentStore {
    
    private final ContentStore delegate;
    private final EquivalenceRecordWriter recordWriter;

    public EquivalenceWritingContentStore(ContentStore delegate, EquivalenceRecordStore recordStore) {
        this.delegate = checkNotNull(delegate);
        this.recordWriter = TransitiveEquivalenceRecordWriter.explicit(recordStore);
    }
    
    @Override
    protected ContentStore delegate() {
        return delegate;
    }

    @Override
    public <C extends Content> WriteResult<C> writeContent(C content) throws WriteException {
        WriteResult<C> writeResult = super.writeContent(content);
        writeEquivalences(content);
        return writeResult;
    }
    
    private void writeEquivalences(Content content) {
        if (!content.getEquivalentTo().isEmpty()) {
            ImmutableSet<Publisher> sources = sources(content);
            recordWriter.writeRecord(EquivalenceRef.valueOf(content), 
                    content.getEquivalentTo(), sources);
        }
    }
    
    private ImmutableSet<Publisher> sources(Content content) {
        return ImmutableSet.<Publisher>builder()
                .add(content.getPublisher())
                .addAll(Iterables.transform(content.getEquivalentTo(), Sourceds.toPublisher()))
                .build();
    }
    
}
