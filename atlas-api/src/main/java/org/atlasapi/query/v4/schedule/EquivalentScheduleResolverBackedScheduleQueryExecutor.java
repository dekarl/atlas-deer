package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.List;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.ApplicationEquivalentsMerger;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.EquivalentChannelSchedule;
import org.atlasapi.schedule.EquivalentSchedule;
import org.atlasapi.schedule.EquivalentScheduleEntry;
import org.atlasapi.schedule.EquivalentScheduleResolver;
import org.atlasapi.schedule.Schedule;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.Maybe;


public class EquivalentScheduleResolverBackedScheduleQueryExecutor implements ScheduleQueryExecutor {

    private static final long QUERY_TIMEOUT = 60000;

    private ChannelResolver channelResolver;
    private EquivalentScheduleResolver scheduleResolver;
    private ApplicationEquivalentsMerger<Content> equivalentsMerger;

    public EquivalentScheduleResolverBackedScheduleQueryExecutor(ChannelResolver channelResolver,
            EquivalentScheduleResolver scheduleResolver, ApplicationEquivalentsMerger<Content> equivalentsMerger) {
        this.channelResolver = checkNotNull(channelResolver);
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.equivalentsMerger = checkNotNull(equivalentsMerger);
    }

    @Override
    public QueryResult<ChannelSchedule> execute(ScheduleQuery query)
            throws QueryExecutionException {
        
        List<Channel> channels = resolveChannels(query);
        
        ImmutableSet<Publisher> selectedSources = selectedSources(query);
        ListenableFuture<EquivalentSchedule> schedule = scheduleResolver.resolveSchedules(channels, query.getInterval(), query.getSource(), selectedSources);
        
        if (query.isMultiChannel()) {
            return QueryResult.listResult(channelSchedules(schedule, query), query.getContext());
        }
        return QueryResult.singleResult(Iterables.getOnlyElement(channelSchedules(schedule, query)), query.getContext());
    }

    private ImmutableSet<Publisher> selectedSources(ScheduleQuery query) {
        if (query.getContext().getApplicationSources().isPrecedenceEnabled()) {
            return query.getContext().getApplicationSources().getEnabledReadSources();
        }
        return ImmutableSet.of(query.getSource());
    }

    private ImmutableList<Channel> resolveChannels(ScheduleQuery query) throws NotFoundException {
        ImmutableList<Channel> channels;
        if (query.isMultiChannel()) {
            List<Long> ids = Lists.transform(query.getChannelIds().asList(),Id.toLongValue());
            channels = ImmutableList.copyOf(channelResolver.forIds(ids));
        } else {
            Maybe<Channel> possibleChannel = channelResolver.fromId(query.getChannelId().longValue());
            if (!possibleChannel.hasValue()) {
                throw new NotFoundException(query.getChannelId());
            }
            channels = ImmutableList.of(possibleChannel.requireValue());
        }
        return channels;
    }

    private List<ChannelSchedule> channelSchedules(ListenableFuture<EquivalentSchedule> schedule, ScheduleQuery query)
            throws ScheduleQueryExecutionException {
        
        return Futures.get(Futures.transform(schedule, toSchedule(query.getContext())),
                QUERY_TIMEOUT, MILLISECONDS, ScheduleQueryExecutionException.class).channelSchedules(); 
    }

    private Function<EquivalentSchedule, Schedule> toSchedule(final QueryContext context) {
        return new Function<EquivalentSchedule, Schedule>() {
            @Override
            public Schedule apply(EquivalentSchedule input) {
                if (context.getApplicationSources().isPrecedenceEnabled()) {
                    return mergeItemsInSchedule(input, context.getApplicationSources());
                }
                return selectBroadcastItems(input);
            }

        };
    }

    private Schedule mergeItemsInSchedule(EquivalentSchedule schedule, ApplicationSources applicationSources) {
        ImmutableList.Builder<ChannelSchedule> channelSchedules = ImmutableList.builder();
        for (EquivalentChannelSchedule ecs : schedule.channelSchedules()) {
            channelSchedules.add(new ChannelSchedule(ecs.getChannel(), ecs.getInterval(), mergeItems(ecs.getEntries(), applicationSources)));
        }
        return new Schedule(channelSchedules.build(), schedule.interval());
    }

    private Iterable<ItemAndBroadcast> mergeItems(ImmutableList<EquivalentScheduleEntry> entries,
            ApplicationSources applicationSources) {
        ImmutableList.Builder<ItemAndBroadcast> iabs = ImmutableList.builder();
        for (EquivalentScheduleEntry entry : entries) {
            List<Item> mergedItems = equivalentsMerger.merge(entry.getItems().getResources(), applicationSources);
            iabs.add(new ItemAndBroadcast(Iterables.getOnlyElement(mergedItems), entry.getBroadcast()));
        }
        return iabs.build();
    }
    

    private Schedule selectBroadcastItems(EquivalentSchedule schedule) {
        ImmutableList.Builder<ChannelSchedule> channelSchedules = ImmutableList.builder();
        for (EquivalentChannelSchedule ecs : schedule.channelSchedules()) {
            channelSchedules.add(new ChannelSchedule(ecs.getChannel(), ecs.getInterval(), selectBroadcastItems(ecs.getEntries())));
        }
        return new Schedule(channelSchedules.build(), schedule.interval());
    }

    private Iterable<ItemAndBroadcast> selectBroadcastItems(List<EquivalentScheduleEntry> entries) {
        ImmutableList.Builder<ItemAndBroadcast> iabs = ImmutableList.builder();
        for (EquivalentScheduleEntry entry : entries) {
            iabs.add(new ItemAndBroadcast(selectBroadcastItem(entry), entry.getBroadcast()));
        }
        return iabs.build();
    }

    private Item selectBroadcastItem(EquivalentScheduleEntry entry) {
        for (Item item : entry.getItems().getResources()) {
            if (item.getBroadcasts().contains(entry.getBroadcast())) {
                return item;
            }
        }
        throw new IllegalStateException("couldn't find broadcast item in " + entry);
    }

}
