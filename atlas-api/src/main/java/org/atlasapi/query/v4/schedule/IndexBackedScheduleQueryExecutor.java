package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.Set;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Version;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.ScheduleIndex;
import org.atlasapi.schedule.ScheduleRef;
import org.atlasapi.schedule.ScheduleRef.ScheduleRefEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.collect.OptionalMap;

public class IndexBackedScheduleQueryExecutor implements ScheduleQueryExecutor {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private static final long QUERY_TIMEOUT = 60000;

    private static Function<ScheduleRefEntry, Id> toId = new Function<ScheduleRefEntry, Id>() {

        @Override
        public Id apply(ScheduleRefEntry input) {
            return input.getItemId();
        }
        
    };

    private final ContentResolver contentResolver;
    private final ScheduleIndex index;
    private final ChannelResolver channelResolver;

    public IndexBackedScheduleQueryExecutor(ChannelResolver channelResolver, ScheduleIndex index, ContentResolver contentResolver) {
        this.channelResolver = checkNotNull(channelResolver);
        this.index = checkNotNull(index);
        this.contentResolver = checkNotNull(contentResolver);
    }

    @Override
    public QueryResult<ChannelSchedule> execute(final ScheduleQuery query) throws QueryExecutionException {
        
        Maybe<Channel> possibleChannel = channelResolver.fromId(query.getChannelId().longValue());
        if (!possibleChannel.hasValue()) {
            throw new NotFoundException(query.getChannelId());
        }
        
        Channel channel = possibleChannel.requireValue();
        ChannelSchedule schedule = Futures.get(resolveToSchedule(queryIndex(query, channel), query, channel),
            QUERY_TIMEOUT, MILLISECONDS, ScheduleQueryExecutionException.class);

        return QueryResult.singleResult(schedule, query.getContext());
    }

    private ListenableFuture<ChannelSchedule> resolveToSchedule(ListenableFuture<ScheduleRef> queryIndex,
            final ScheduleQuery query, final Channel channel) {
        return Futures.transform(queryIndex, new AsyncFunction<ScheduleRef, ChannelSchedule>() {
            @Override
            public ListenableFuture<ChannelSchedule> apply(ScheduleRef input) throws Exception {
                return transformToChannelSchedule(query, channel, input);
            }
        });
    }

    private ListenableFuture<ScheduleRef> queryIndex(ScheduleQuery query, Channel channel) throws NotFoundException {
        return index.resolveSchedule(query.getSource(), channel, query.getInterval());
    }

    private ListenableFuture<ChannelSchedule> transformToChannelSchedule(final ScheduleQuery query, final Channel channel, final ScheduleRef scheduleRef) {
        if (scheduleRef.isEmpty()) {
            return Futures.immediateFuture(new ChannelSchedule(channel, query.getInterval(), ImmutableList.<ItemAndBroadcast>of()));
        }
        
        return Futures.transform(contentResolver.resolveIds(
            Iterables.transform(scheduleRef.getScheduleEntries(), toId)
        ), new Function<Resolved<Content>, ChannelSchedule>() {
            @Override
            public ChannelSchedule apply(Resolved<Content> input) {
                return new ChannelSchedule(channel, query.getInterval(),
                        contentList(query, scheduleRef, input.toMap()));
            }
        });
    }

    private Iterable<ItemAndBroadcast> contentList(ScheduleQuery query, ScheduleRef scheduleRef, OptionalMap<Id, Content> resolvedContent) {
        Builder<ItemAndBroadcast> contentList = ImmutableList.builder();
        for (ScheduleRefEntry entry : scheduleRef.getScheduleEntries()){
            Optional<Content> resolved = resolvedContent.get(entry.getItemId());
            if (!resolved.isPresent()) {
                // item in index but not store, throw exception?
                continue; 
            }
            Item item = (Item) resolved.get();
            if (item != null) {
                /* copy the item here because it may appear in the same schedule
                 * twice but will only appear in resolvedContent once.
                 */
                item = item.copy();
                ItemAndBroadcast itemAndBroadcast = trimBroadcasts(entry, item);
                if (itemAndBroadcast != null) {
                    contentList.add(itemAndBroadcast);
                }
            }
        }
        return contentList.build();
    }

    /* This handles when resolved content is either merged on un-merged. We 
     * always want the requested source's content so we can find the broadcast.
     */
    private Item findRequestSourceItem(ScheduleQuery query, Collection<Content> resolved) {
        if (resolved != null) {
            for (Identified identified : resolved) {
                if (identified instanceof Item) {
                    Item item = (Item) identified;
                    if (query.getSource().equals(item.getPublisher())) {
                        return item;
                    }
                }
            }
        }
        return null;
    }

    private ItemAndBroadcast trimBroadcasts(ScheduleRefEntry entry, Item item) {
        ItemAndBroadcast itemAndBroadcast = null;
        for (Version version : item.getVersions()) {
            Set<Broadcast> allBroadcasts = version.getBroadcasts();
            version.setBroadcasts(Sets.<Broadcast>newHashSet());
            if (itemAndBroadcast != null) {
                continue;
            }
            for (Broadcast broadcast : allBroadcasts) {
                if (relevantBroadcast(broadcast, entry)) {
                    version.setBroadcasts(Sets.newHashSet(broadcast));
                    itemAndBroadcast = new ItemAndBroadcast(item, broadcast);
                }
            }
        }
        return itemAndBroadcast;
    }

    private boolean relevantBroadcast(Broadcast broadcast, ScheduleRefEntry entry) {
        return broadcast.isActivelyPublished()
            && matchingChannel(broadcast, entry)
            && (matchingSourceId(broadcast, entry) || matchingStartTime(broadcast, entry));
    }

    private boolean matchingStartTime(Broadcast broadcast, ScheduleRefEntry entry) {
        return broadcast.getTransmissionTime().equals(entry.getBroadcastTime());
    }

    private boolean matchingSourceId(Broadcast broadcast, ScheduleRefEntry entry) {
        return broadcast.getSourceId() != null && broadcast.getSourceId().equals(entry.getBroadcastId().orNull());
    }

    private boolean matchingChannel(Broadcast broadcast, ScheduleRefEntry entry) {
        return broadcast.getBroadcastOn().equals(entry.getChannelId());
    }
}
