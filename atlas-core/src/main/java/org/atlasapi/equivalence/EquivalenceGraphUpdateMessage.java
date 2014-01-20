package org.atlasapi.equivalence;

import java.util.Set;

import org.atlasapi.messaging.AbstractMessage;

import com.metabroadcast.common.time.Timestamp;


public class EquivalenceGraphUpdateMessage extends AbstractMessage {

    private final Set<EquivalenceGraph> updatedGraphs;

    public EquivalenceGraphUpdateMessage(String messageId, Timestamp timestamp, Set<EquivalenceGraph> updatedGraphs) {
        super(messageId, timestamp);
        this.updatedGraphs = updatedGraphs;
    }
    
    public Set<EquivalenceGraph> getUpdatedGraphs() {
        return updatedGraphs;
    }

}
