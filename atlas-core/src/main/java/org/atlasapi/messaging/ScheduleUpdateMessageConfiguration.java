package org.atlasapi.messaging;

import org.atlasapi.schedule.ScheduleUpdate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;


public class ScheduleUpdateMessageConfiguration {

    @JsonCreator
    public ScheduleUpdateMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("update") ScheduleUpdate scheduleUpdate) {
        
    }
    
}
