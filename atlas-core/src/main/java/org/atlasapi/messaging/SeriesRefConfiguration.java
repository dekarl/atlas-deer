package org.atlasapi.messaging;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class SeriesRefConfiguration {
    @JsonCreator
    SeriesRefConfiguration(
            @JsonProperty("id") Id id, 
            @JsonProperty("source") Publisher source, 
            @JsonProperty("title") String title, 
            @JsonProperty("seriesNumber") Integer seriesNumber, 
            @JsonProperty("updated") DateTime updated) {
    }
}
