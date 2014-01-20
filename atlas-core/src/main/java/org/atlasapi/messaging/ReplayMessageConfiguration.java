package org.atlasapi.messaging;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;

import org.atlasapi.messaging.Message;

/**
 */
public abstract class ReplayMessageConfiguration {

    @JsonCreator
    ReplayMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("original") Message original) {
    }
    
}
