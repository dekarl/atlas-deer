package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.joda.time.Interval;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.time.IntervalOrdering;

public final class ScheduleRef {

    public static final Builder forChannel(Id channel, Interval interval) {
        return new Builder(checkNotNull(channel), checkNotNull(interval));
    }

    public static final class Builder {

        private final Id channel;
        private final Interval interval;
        private ImmutableSet.Builder<ScheduleRefEntry> entries;

        private Builder(Id channel, Interval interval) {
            this.channel = channel;
            this.interval = interval;
            this.entries = ImmutableSet.builder();
        }
        
        public Builder addEntry(ScheduleRefEntry entry) {
            this.entries.add(entry);
            return this;
        }
        
        public Builder addEntries(Iterable<ScheduleRefEntry> entries) {
            this.entries.addAll(entries);
            return this;
        }
        
        public ScheduleRef build() {
            return new ScheduleRef(channel, interval, Ordering.natural().immutableSortedCopy(entries.build()));
        }
    }

    private final Id channel;
    private final Interval interval;
    private final ImmutableList<ScheduleRefEntry> entries;

    private ScheduleRef(Id channel, Interval interval, ImmutableList<ScheduleRefEntry> entries) {
        this.channel = channel;
        this.interval = interval;
        this.entries = entries;
    }
    
    public Id getChannel() {
        return channel;
    }
    
    public Interval getInterval() {
        return interval;
    }
    
    public ImmutableList<ScheduleRefEntry> getScheduleEntries() {
        return entries;
    }
    
    public boolean isEmpty() {
        return entries.isEmpty();
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ScheduleRef) {
            ScheduleRef other = (ScheduleRef) that;
            return channel.equals(channel)
                && interval.equals(interval)
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
        return Objects.toStringHelper(this)
                .add("channel", channel)
                .add("entries", entries)
                .toString();
    }
    
    public static final class ScheduleRefEntry implements Comparable<ScheduleRefEntry> {
        
        private final Id item;
        private final Id channel;
        private final Interval broadcastInterval;
        private final Optional<String> broadcastId;

        public ScheduleRefEntry(Id item, Id channel, Interval broadcastInterval, @Nullable String broadcastId) {
            this.item = checkNotNull(item);
            this.channel = checkNotNull(channel);
            this.broadcastInterval = checkNotNull(broadcastInterval);
            this.broadcastId = Optional.fromNullable(broadcastId);
        }

        @Override
        public int compareTo(ScheduleRefEntry o) {
            return ComparisonChain.start().compare(channel, o.channel)
                    .compare(broadcastInterval, o.broadcastInterval, IntervalOrdering.byStartShortestFirst())
                    .result();
        }

        public Id getItem() {
            return item;
        }

        public Id getChannel() {
            return channel;
        }

        public Interval getBroadcastInterval() {
            return broadcastInterval;
        }

        public Optional<String> getBroadcastId() {
            return broadcastId;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) {
                return true;
            }
            if (that instanceof ScheduleRefEntry) {
                ScheduleRefEntry other = (ScheduleRefEntry) that;
                return item.equals(other.item)
                    && channel.equals(other.channel)
                    && broadcastInterval.equals(other.broadcastInterval);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(item, channel, broadcastInterval);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .omitNullValues()
                    .add("item", item)
                    .add("channel", channel)
                    .add("start", broadcastInterval)
                    .add("id", broadcastId)
                    .toString();
        }

    }
}
