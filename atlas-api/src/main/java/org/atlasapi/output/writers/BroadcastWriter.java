package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.Broadcast;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public final class BroadcastWriter implements EntityListWriter<Broadcast> {

    private static final String ELEMENT_NAME = "broadcast";
    private final String listName;

    public BroadcastWriter(String listName) {
        this.listName = listName;
    }

    @Override
    public void write(Broadcast entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("id", entity.getSourceId());
        writer.writeField("transmission_time", entity.getTransmissionTime());
        writer.writeField("transmission_end_time", entity.getTransmissionEndTime());
        writer.writeField("broadcast_duration", entity.getBroadcastDuration());
        writer.writeField("broadcast_on", entity.getBroadcastOn());
        writer.writeField("schedule_date", entity.getScheduleDate());
        writer.writeField("repeat", entity.getRepeat());
        writer.writeField("subtitled", entity.getSubtitled());
        writer.writeField("signed",entity.getSigned());
        writer.writeField("audio_described",entity.getAudioDescribed());
        writer.writeField("high_definition",entity.getHighDefinition());
        writer.writeField("widescreen",entity.getWidescreen());
        writer.writeField("surround",entity.getSurround());
        writer.writeField("live",entity.getLive());
        writer.writeField("premiere",entity.getPremiere());
        writer.writeField("new_series",entity.getNewSeries());
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(Broadcast entity) {
        return ELEMENT_NAME;
    }
}