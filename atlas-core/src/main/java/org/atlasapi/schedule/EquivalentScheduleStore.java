package org.atlasapi.schedule;

import java.util.Set;

import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;

import com.google.common.util.concurrent.ListenableFuture;


public interface EquivalentScheduleStore {

    ListenableFuture<EquivalentSchedule> resolveSchedules(Iterable<Channel> channels,
            Interval interval, Publisher source, Set<Publisher> selectedSources);
    
    void updateSchedule(ScheduleRef refs);
    
    void updateEquivalences(EquivalenceGraphUpdate update);
    
}
