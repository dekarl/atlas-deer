package org.atlasapi.output.annotation;

import static org.atlasapi.content.Broadcast.ACTIVELY_PUBLISHED;

import java.io.IOException;

import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class BroadcastsAnnotation extends OutputAnnotation<Content> {
    
    private final BroadcastWriter broadcastWriter;
    
    public BroadcastsAnnotation(NumberToShortStringCodec codec) {
        broadcastWriter = new BroadcastWriter("broadcasts", codec);
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            writeBroadcasts(writer, (Item) entity, ctxt);
        }
    }

    private void writeBroadcasts(FieldWriter writer, Item item, OutputContext ctxt) throws IOException {
        writer.writeList(broadcastWriter, Iterables.filter(item.getBroadcasts(), ACTIVELY_PUBLISHED), ctxt);
    }

}
