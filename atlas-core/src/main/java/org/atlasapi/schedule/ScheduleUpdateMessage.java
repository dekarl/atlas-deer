package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.messaging.AbstractMessage;

import com.metabroadcast.common.time.Timestamp;

public class ScheduleUpdateMessage extends AbstractMessage {

    private final ScheduleUpdate update;

    public ScheduleUpdateMessage(String mid, Timestamp ts, ScheduleUpdate update) {
        super(mid, ts);
        this.update = checkNotNull(update);
    }
    
    public ScheduleUpdate getScheduleUpdate() {
        return update;
    }
    
}
