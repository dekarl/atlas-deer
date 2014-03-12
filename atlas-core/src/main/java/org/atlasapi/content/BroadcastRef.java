package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.joda.time.Interval;

public final class BroadcastRef {

    private final String sourceId;
    private final Id channelId;
    private final Interval transmissionInterval;

    public BroadcastRef(String sourceId, Id channelId, Interval transmissionInterval) {
        this.sourceId = sourceId;
        this.channelId = channelId;
        this.transmissionInterval = transmissionInterval;
    }

    public String getSourceId() {
        return sourceId;
    }
    
    public Interval getTransmissionInterval() {
        return transmissionInterval;
    }
    
    public Id getChannelId() {
        return channelId;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof BroadcastRef) {
            BroadcastRef other = (BroadcastRef) that;
            return sourceId.equals(other.sourceId)
                && channelId.equals(other.channelId)
                && transmissionInterval.equals(other.transmissionInterval);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return sourceId.hashCode();
    }
    
}
