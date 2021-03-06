package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphStore;

import com.metabroadcast.common.queue.Worker;


public class ContentEquivalenceUpdatingWorker implements Worker<EquivalenceAssertionMessage> {

    private final EquivalenceGraphStore graphStore;

    public ContentEquivalenceUpdatingWorker(EquivalenceGraphStore graphStore) {
        this.graphStore = checkNotNull(graphStore);
    }
    
    @Override
    public void process(EquivalenceAssertionMessage message) {
        try {
            graphStore.updateEquivalences(message.getSubject(), message.getAssertedAdjacents(), 
                    message.getPublishers());
        } catch (WriteException e) {
            throw new RuntimeException(e);
        }
    }

}
