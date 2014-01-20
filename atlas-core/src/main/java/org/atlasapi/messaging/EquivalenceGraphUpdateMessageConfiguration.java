package org.atlasapi.messaging;

import java.util.Set;

import org.atlasapi.equivalence.EquivalenceGraph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;


public class EquivalenceGraphUpdateMessageConfiguration {

    @JsonCreator
    public EquivalenceGraphUpdateMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("updatedGraphs") Set<EquivalenceGraph> updatedGraphs) {
    }
    
}
