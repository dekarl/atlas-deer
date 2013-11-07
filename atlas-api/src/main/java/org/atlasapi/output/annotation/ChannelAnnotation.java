package org.atlasapi.output.annotation;

import static org.atlasapi.output.writers.SourceWriter.sourceListWriter;
import static org.atlasapi.output.writers.SourceWriter.sourceWriter;

import java.io.IOException;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;


public class ChannelAnnotation extends OutputAnnotation<Channel> {

    private static final EntityListWriter<Publisher> AVAILABLE_FROM_WRITER = sourceListWriter("available_from");
    private static final EntityWriter<Publisher> BROADCASTER_WRITER = sourceWriter("broadcaster");

    public ChannelAnnotation() {
        super();
    }

    @Override
    public void write(Channel entity, FieldWriter format, OutputContext ctxt) throws IOException {
        format.writeList(AVAILABLE_FROM_WRITER, entity.availableFrom(), ctxt);
        format.writeObject(AVAILABLE_FROM_WRITER, entity.source(), ctxt);
        format.writeField("media_type", entity.mediaType());
        format.writeObject(BROADCASTER_WRITER, entity.broadcaster(), ctxt);
    }

}
