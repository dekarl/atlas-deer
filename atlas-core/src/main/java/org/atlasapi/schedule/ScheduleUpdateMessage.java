package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.messaging.Message;

import com.metabroadcast.common.time.Timestamp;


public class ScheduleUpdateMessage implements Message {

    private final String mid;
    private final Timestamp ts;
    private final ScheduleRef updateRef;

    public ScheduleUpdateMessage(String mid, Timestamp ts, ScheduleRef updateRef) {
        this.mid = checkNotNull(mid);
        this.ts = checkNotNull(ts);
        this.updateRef = checkNotNull(updateRef);
    }
    
    @Override
    public String getMessageId() {
        return mid;
    }

    @Override
    public Timestamp getTimestamp() {
        return ts;
    }

    public ScheduleRef getScheduleUpdateRef() {
        return updateRef;
    }
    
}
