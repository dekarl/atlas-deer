package org.atlasapi.output.annotation;


import java.io.IOException;

import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.ChildRefWriter;

import com.metabroadcast.common.ids.NumberToShortStringCodec;


public class SubItemAnnotation extends OutputAnnotation<Content> {

    private final ChildRefWriter childRefWriter;

    public SubItemAnnotation(NumberToShortStringCodec idCodec) {
        childRefWriter = new ChildRefWriter(idCodec, "content");
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Container) {
            Container container = (Container) entity;
            writer.writeList(childRefWriter, container.getChildRefs(), ctxt);
        }
    }

}
