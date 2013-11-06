package org.atlasapi.content;

import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Summary;


public class ContainerSummarySerializer {

    public ContentProtos.Summary serialize(Item.ContainerSummary containerSummary) {
        ContentProtos.Summary.Builder summary = ContentProtos.Summary.newBuilder();
        if (containerSummary.getType() != null) {
            summary.setType(containerSummary.getType());
        }
        if (containerSummary.getTitle() != null) {
            summary.setTitle(containerSummary.getTitle());
        }
        if (containerSummary.getDescription() != null) {
            summary.setDescription(containerSummary.getDescription());
        }
        if (containerSummary.getSeriesNumber() != null) {
            summary.setPosition(containerSummary.getSeriesNumber());
        }
        return summary.build();
    }
    
    public Item.ContainerSummary deserialize(Summary summary) {
        return new Item.ContainerSummary(
            summary.hasType() ? summary.getType() : null,
            summary.hasTitle() ? summary.getTitle() : null,
            summary.hasDescription() ? summary.getDescription() : null,
            summary.hasPosition() ? summary.getPosition() : null
        );
    }
    
}
