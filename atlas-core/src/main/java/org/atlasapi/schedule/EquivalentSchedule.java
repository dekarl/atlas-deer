package org.atlasapi.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.joda.time.Interval;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;


public class EquivalentSchedule {

    private final Interval interval;
    private final List<EquivalentChannelSchedule> channelSchedules;
    
    public EquivalentSchedule(List<EquivalentChannelSchedule> channelSchedules, Interval interval) {
        this.channelSchedules = ImmutableList.copyOf(channelSchedules);
        this.interval = checkNotNull(interval);
    }
    
    public Interval interval() {
        return interval;
    }

    public List<EquivalentChannelSchedule> channelSchedules() {
        return this.channelSchedules;
    }

    @Override
    public int hashCode() {
        return channelSchedules.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof EquivalentSchedule) {
            EquivalentSchedule other = (EquivalentSchedule) that;
            return channelSchedules.equals(other.channelSchedules);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .addValue(channelSchedules)
            .toString();
    }
    
}
