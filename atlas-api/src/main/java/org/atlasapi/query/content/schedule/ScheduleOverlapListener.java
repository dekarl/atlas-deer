package org.atlasapi.query.content.schedule;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;

public interface ScheduleOverlapListener {
    
    void itemRemovedFromSchedule(Item item, Broadcast broadcast);
    
}
