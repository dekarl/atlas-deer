package org.atlasapi.query.v4.schedule;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.schedule.ChannelSchedule;

import com.google.common.collect.Lists;


public class ScheduleListWriter implements EntityListWriter<ChannelSchedule> {

    private final EntityWriter<Channel> channelWriter;
    private final EntityListWriter<Content> contentWriter;

    public ScheduleListWriter(EntityWriter<Channel> channelWriter, EntityListWriter<Content> contentWriter) {
        this.channelWriter = channelWriter;
        this.contentWriter = contentWriter;
    }
    
    @Override
    public void write(ChannelSchedule entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeObject(channelWriter, entity.getChannel(), ctxt);
        writer.writeList(contentWriter, Lists.transform(entity.getEntries(),ItemAndBroadcast.toItem()), ctxt);
    }

    @Override
    public String fieldName(ChannelSchedule entity) {
        return "schedule";
    }

    @Override
    public String listName() {
        return "schedules";
    }

}
