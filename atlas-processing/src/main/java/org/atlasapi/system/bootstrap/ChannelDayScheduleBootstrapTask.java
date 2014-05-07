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
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.util.ResolveException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.StoreException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleHierarchy;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.elasticsearch.common.base.Objects;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    private final ContentStore contentStore;
    
    private final Channel channel;
    private final LocalDate day;
    private final Publisher source;

    public ChannelDayScheduleBootstrapTask(ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter, ContentStore contentStore,
            Channel channel, LocalDate day, Publisher source) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentStore = checkNotNull(contentStore);
        this.channel = checkNotNull(channel);
        this.day = checkNotNull(day);
        this.source = checkNotNull(source);
    }

    @Override
    public UpdateProgress call() throws Exception {
        ListenableFuture<Schedule> resolved =
            scheduleResolver.resolve(ImmutableSet.of(channel), interval(day), source);
        Schedule schedule = Futures.get(resolved, 1, TimeUnit.MINUTES, ResolveException.class);
        /* it's reasonable for there not to be a channel for a given source/channel combination
         * but there should be precisely one if any. 
         */
        if (!schedule.channelSchedules().isEmpty()) {
            return writeItemsIn(Iterables.getOnlyElement(schedule.channelSchedules()));
        } else {
            return UpdateProgress.START;
        }
    }

    private UpdateProgress writeItemsIn(ChannelSchedule channelSchedule) throws StoreException {
        checkState(channelSchedule.getChannel().equals(channel),
                "got schedule for %s not %s", channelSchedule.getChannel(), channel);
        Map<Id, Optional<Content>> scheduleItems = resolveItems(channelSchedule);
        ensureContainers(Optional.presentInstances(scheduleItems.values()));
        UpdateProgress progress = UpdateProgress.START;
        //used to handle zero duration broadcasts, assumes no adjacent zero duration broadcasts
        ItemAndBroadcast prev = null;
        for (ItemAndBroadcast iab : channelSchedule.getEntries()) {
            if (iab.getBroadcast().getBroadcastDuration().getMillis() == 0) {
                prev = iab;
            } else {
                ImmutableList<ItemAndBroadcast> iabs = prev == null ? ImmutableList.of(iab)
                                                                    : ImmutableList.of(iab, prev);
                progress = progress.reduce(tryWrite(scheduleItems, iabs, channel));
                prev = null;
            }
        }
        return progress;
    }

    private UpdateProgress tryWrite(Map<Id, Optional<Content>> scheduleItems, 
            ImmutableList<ItemAndBroadcast> iabs, Channel channel) throws WriteException {
        
        List<Id> ids = Lists.transform(iabs, Functions.compose(Identifiables.toId(), ItemAndBroadcast.toItem()));
        Map<Id, Optional<Content>> items = Maps.filterKeys(scheduleItems, Predicates.in(ids));
        
        DateTime earliestStart = null;
        DateTime latestEnd = null;
        
        ImmutableList.Builder<ScheduleHierarchy> schedule = ImmutableList.builder();
        for (ItemAndBroadcast iab : iabs) {
            Broadcast broadcast = iab.getBroadcast();
            Optional<Content> possItem = items.get(iab.getItem().getId());
            if (!possItem.isPresent()) {
                log.warn("content not resolved for broadcast " + broadcast);
                return new UpdateProgress(0, ids.size());
            }
            DateTime bcastStart = broadcast.getTransmissionTime();
            DateTime bcastEnd = broadcast.getTransmissionEndTime();
            earliestStart = earliestStart == null || earliestStart.isAfter(bcastStart) ? bcastStart
                                                                                       : earliestStart;
            latestEnd = latestEnd == null || latestEnd.isBefore(bcastEnd) ? bcastEnd
                                                                          : latestEnd;
            schedule.add(ScheduleHierarchy.itemOnly(iab));
        }
        try {
            scheduleWriter.writeSchedule(schedule.build(), channel, new Interval(earliestStart, latestEnd));
            return new UpdateProgress(ids.size(), 0);
        } catch (WriteException we) {
            log.warn("Failed to write " + iabs, we);
            return new UpdateProgress(0, ids.size());
        }
    }

    private void ensureContainers(Iterable<Content> items) throws StoreException {
        ImmutableSet<Id> ids = containerIds(items);
        ListenableFuture<Resolved<Content>> resolved = contentStore.resolveIds(ids);
        Resolved<Content> containers = Futures.get(resolved, 1, TimeUnit.MINUTES, ResolveException.class);
        OptionalMap<Id, Content> containerIndex = containers.toMap();
        for (Id id : ids) {
            Optional<Content> possibleContainer = containerIndex.get(id);
            if (possibleContainer.isPresent()) {
                contentStore.writeContent(possibleContainer.get());
            }
        }
    }

    private ImmutableSet<Id> containerIds(Iterable<Content> items) {
        ImmutableSet.Builder<Id> containerIds = ImmutableSet.builder(); 
        for (Item item : Iterables.filter(items, Item.class)) {
            if (item.getContainerRef() != null) {
                containerIds.add(item.getContainerRef().getId());
            }
            if (item instanceof Episode && ((Episode)item).getSeriesRef() != null) {
                containerIds.add(((Episode)item).getSeriesRef().getId());
            }
        }
        return containerIds.build();
    }

    private OptionalMap<Id, Content> resolveItems(ChannelSchedule channelSchedule)
            throws ResolveException {
        List<ItemAndBroadcast> entries = channelSchedule.getEntries();
        Set<Id> entryIds = Sets.newHashSetWithExpectedSize(entries.size());
        for (ItemAndBroadcast itemAndBroadcast : entries) {
            entryIds.add(itemAndBroadcast.getItem().getId());
        }
        ListenableFuture<Resolved<Content>> resolved = contentStore.resolveIds(entryIds);
        return Futures.get(resolved, 1, TimeUnit.MINUTES, ResolveException.class).toMap();
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
