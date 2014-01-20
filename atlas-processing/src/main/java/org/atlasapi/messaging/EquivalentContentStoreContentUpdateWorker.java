package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.util.WriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EquivalentContentStoreContentUpdateWorker extends BaseWorker<ResourceUpdatedMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final EquivalentContentStore equivalentContentStore;

    public EquivalentContentStoreContentUpdateWorker(EquivalentContentStore equivalentContentStore) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
    }

    @Override
    public void process(ResourceUpdatedMessage message) {
        try {
            equivalentContentStore.updateContent(message.getUpdatedResource());
        } catch (WriteException e) {
            log.error("update failed for content " + message.getUpdatedResource(), e);
        }
    }

}
