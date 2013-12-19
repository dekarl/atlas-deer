package org.atlasapi.messaging;

import org.atlasapi.entity.ResourceRef;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;

public abstract class ResourceUpdatedMessageConfiguration {

    @JsonCreator
    ResourceUpdatedMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("updatedResource") ResourceRef updatedResource) {
        
    }
    
}
