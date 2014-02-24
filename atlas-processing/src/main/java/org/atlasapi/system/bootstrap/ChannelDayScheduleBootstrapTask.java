package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.input.ReadException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleHierarchy;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.elasticsearch.common.base.Objects;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;

/**
 * <p>Copies a schedule from a {@link ScheduleResolver} to a {@link ScheduleWriter}
 * for a given {@link Channel}/{@link LocalDate}/{@link Publisher} combo.</p>
 * 
 * <p>Items in the schedule are resolved via a {@link ContentResolver} to ensure
 * all broadcasts are found.</p>
 */
public class ChannelDayScheduleBootstrapTask implements Callable<UpdateProgress> {

    private static final Logger log =
        LoggerFactory.getLogger(ChannelDayScheduleBootstrapTask.class);

    private final ScheduleResolver scheduleResolver;
    private final ScheduleWriter scheduleWriter;
    private final ContentResolver contentResolver;
    
    private final Channel channel;
    private final LocalDate day;
    private final Publisher source;

    public ChannelDayScheduleBootstrapTask(ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter, ContentResolver contentResolver,
            Channel channel, LocalDate day, Publisher source) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentResolver = checkNotNull(contentResolver);
        this.channel = checkNotNull(channel);
        this.day = checkNotNull(day);
        this.source = checkNotNull(source);
    }

    @Override
    public UpdateProgress call() throws Exception {
        ListenableFuture<Schedule> resolved =
            scheduleResolver.resolve(ImmutableSet.of(channel), interval(day), source);
        Schedule schedule = Futures.get(resolved, 1, TimeUnit.MINUTES, ReadException.class);
        /* it's reasonable for there not to be a channel for a given source/channel combination
         * but there should be precisely one if any. 
         */
        if (!schedule.channelSchedules().isEmpty()) {
            return writeItemsIn(Iterables.getOnlyElement(schedule.channelSchedules()));
        } else {
            return UpdateProgress.START;
        }
    }

    private UpdateProgress writeItemsIn(ChannelSchedule channelSchedule) throws ReadException {
        checkState(channelSchedule.getChannel().equals(channel),
                "got schedule for %s not %s", channelSchedule.getChannel(), channel);
        Map<Id, Optional<Content>> scheduleItems = resolveItems(channelSchedule);
        UpdateProgress progress = UpdateProgress.START;
        for (ItemAndBroadcast iab : channelSchedule.getEntries()) {
            Optional<Content> item = scheduleItems.get(iab.getItem().getId());
            progress = progress.reduce(tryWriteItem(item, iab.getBroadcast(), channel));
        }
        return progress;
    }

    private UpdateProgress tryWriteItem(Optional<Content> item, Broadcast broadcast, Channel channel) {
        if (!item.isPresent()) {
            log.warn("content not resolved for broadcast " + broadcast);
            return UpdateProgress.FAILURE;
        }
        ItemAndBroadcast iab = new ItemAndBroadcast((Item) item.get(), broadcast);
        try {
            ScheduleHierarchy sh = ScheduleHierarchy.itemOnly(iab);
            scheduleWriter.writeSchedule(ImmutableList.of(sh), channel, intervalOf(broadcast));
            return UpdateProgress.SUCCESS;
        } catch (WriteException we) {
            log.warn("Failed to write " + iab, we);
            return UpdateProgress.FAILURE;
        }
    }

    private Interval intervalOf(Broadcast broadcast) {
        return new Interval(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime());
    }

    private OptionalMap<Id, Content> resolveItems(ChannelSchedule channelSchedule)
            throws ReadException {
        List<ItemAndBroadcast> entries = channelSchedule.getEntries();
        Set<Id> entryIds = Sets.newHashSetWithExpectedSize(entries.size());
        for (ItemAndBroadcast itemAndBroadcast : entries) {
            entryIds.add(itemAndBroadcast.getItem().getId());
        }
        ListenableFuture<Resolved<Content>> resolved = contentResolver.resolveIds(entryIds);
        return Futures.get(resolved, 1, TimeUnit.MINUTES, ReadException.class).toMap();
    }

    private Interval interval(LocalDate day) {
        return new Interval(day.toDateTimeAtStartOfDay(DateTimeZones.UTC),
                day.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC));
    }

    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
            .add("src", source)
            .add("channel", channel)
            .add("day", day)
            .toString();
    }
}
