package org.atlasapi.messaging;

import java.util.Map;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class EquivalenceGraphConfiguration {

    @JsonCreator
    public EquivalenceGraphConfiguration(
            @JsonProperty("adjacencyList") Map<Id, Adjacents> adjacencyList, 
            @JsonProperty("updated") DateTime updated) {
        
    }
    
}
