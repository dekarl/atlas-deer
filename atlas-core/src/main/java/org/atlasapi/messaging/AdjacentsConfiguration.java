package org.atlasapi.messaging;

import java.util.Set;

import org.atlasapi.entity.ResourceRef;
import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class AdjacentsConfiguration {

    @JsonCreator
    public AdjacentsConfiguration(
            @JsonProperty("subject") ResourceRef subject, 
            @JsonProperty("created") DateTime created,
            @JsonProperty("efferent") Set<ResourceRef> efferent, 
            @JsonProperty("afferent") Set<ResourceRef> afferent) {
        
    }
    
}
