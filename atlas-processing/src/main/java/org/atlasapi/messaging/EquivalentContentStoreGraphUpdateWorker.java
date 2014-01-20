package org.atlasapi.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.EquivalentContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;


public class EquivalentContentStoreGraphUpdateWorker extends BaseWorker<EquivalenceGraphUpdateMessage> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final EquivalentContentStore equivalentContentStore;

    public EquivalentContentStoreGraphUpdateWorker(EquivalentContentStore equivalentContentStore) {
        this.equivalentContentStore = checkNotNull(equivalentContentStore);
    }

    @Override
    public void process(EquivalenceGraphUpdateMessage message) {
        try {
            equivalentContentStore.updateEquivalences(message.getUpdatedGraphs());
        } catch (WriteException e) {
            Iterable<Id> ids = Iterables.transform(message.getUpdatedGraphs(), Identifiables.toId());
            log.error("update failed for graphs " + ids, e);
        }
    }

}
