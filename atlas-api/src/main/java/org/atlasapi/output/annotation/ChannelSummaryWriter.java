package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;


public class ChannelSummaryWriter extends OutputAnnotation<Channel> {

    public ChannelSummaryWriter() {
        super();
    }

    @Override
    public void write(Channel entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("title", entity.title());
        writer.writeField("image", null);
    }

}
