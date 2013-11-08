package org.atlasapi.output.annotation;

import java.io.IOException;
import java.util.Set;

import org.atlasapi.content.Content;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.Version;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.writers.BroadcastWriter;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableSet.Builder;
import com.metabroadcast.common.time.Clock;

public class NextBroadcastAnnotation extends OutputAnnotation<Content> {

    private final BroadcastWriter broadcastWriter;
    private final Clock clock;

    public NextBroadcastAnnotation(Clock clock) {
        super();
        this.clock = clock;
        this.broadcastWriter = new BroadcastWriter("next_broadcasts");
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        if (entity instanceof Item) {
            writeBroadcasts(writer, (Item) entity, ctxt);
        }
    }

    private void writeBroadcasts(FieldWriter writer, Item item, OutputContext ctxt) throws IOException {
        Set<Version> versions = item.getVersions();
        Iterable<Broadcast> broadcasts = Iterables.concat(Iterables.transform(versions, Version.TO_BROADCASTS));
        writer.writeList(broadcastWriter, nextBroadcast(Iterables.filter(broadcasts, Broadcast.ACTIVELY_PUBLISHED)), ctxt);
    }

    private Iterable<Broadcast> nextBroadcast(Iterable<Broadcast> broadcasts) {
        DateTime now = clock.now();
        DateTime earliest = null;
        Builder<Broadcast> filteredBroadcasts = ImmutableSet.builder();
        for (Broadcast broadcast : broadcasts) {
            DateTime transmissionTime = broadcast.getTransmissionTime();
            if (transmissionTime.isAfter(now) && (earliest == null || transmissionTime.isBefore(earliest))) {
                earliest = transmissionTime;
                filteredBroadcasts = ImmutableSet.<Broadcast>builder().add(broadcast);
            } else if (transmissionTime.isEqual(earliest)) {
                filteredBroadcasts.add(broadcast);
            }
        }
        return filteredBroadcasts.build();
    }
}
