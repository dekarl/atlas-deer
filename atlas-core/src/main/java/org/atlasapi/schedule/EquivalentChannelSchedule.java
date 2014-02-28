package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.media.channel.Channel;
import org.joda.time.Interval;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSetMultimap;

public class EquivalentChannelSchedule {

    private final Channel channel;
    private final Interval interval;
    private final ImmutableSetMultimap<Broadcast, Item> entries;

    public EquivalentChannelSchedule(Channel channel, Interval interval, ImmutableSetMultimap<Broadcast, Item> entries) {
        this.channel = checkNotNull(channel);
        this.interval = checkNotNull(interval);
        this.entries = ImmutableSetMultimap.copyOf(entries);
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public Interval getInterval() {
        return interval;
    }

    public ImmutableSetMultimap<Broadcast, Item> getEntries() {
        return entries;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalentChannelSchedule) {
            EquivalentChannelSchedule other = (EquivalentChannelSchedule) that;
            return channel.equals(other.channel)
                && interval.equals(other.interval)
                && entries.equals(other.entries);
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return channel.hashCode();
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .add("channel", channel)
                .add("interval", interval)
                .add("entries", entries)
                .toString();
    }
    
}
