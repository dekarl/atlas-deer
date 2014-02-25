package org.atlasapi.messaging;

import org.atlasapi.equivalence.EquivalenceGraphUpdate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;


public class EquivalenceGraphUpdateMessageConfiguration {

    @JsonCreator
    public EquivalenceGraphUpdateMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("update") EquivalenceGraphUpdate update) {
    }
    
}
