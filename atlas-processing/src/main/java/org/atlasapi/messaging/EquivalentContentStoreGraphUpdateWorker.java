package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.metabroadcast.common.queue.Worker;

public class EquivalentContentStoreGraphUpdateWorker implements Worker<EquivalenceGraphUpdateMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final EquivalentContentStore equivalentContentStore;

    public EquivalentContentStoreGraphUpdateWorker(EquivalentContentStore equivalentContentStore) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) {
        try {
            equivalentContentStore.updateEquivalences(message.getGraphUpdate());
        } catch (WriteException e) {
            log.error("update failed for " + message.getGraphUpdate(), e);
        }
    }

}
