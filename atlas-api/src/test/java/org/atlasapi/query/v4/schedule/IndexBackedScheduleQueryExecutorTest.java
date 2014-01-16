package org.atlasapi.query.v4.schedule;

import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Version;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.ScheduleIndex;
import org.atlasapi.schedule.ScheduleRef;
import org.atlasapi.schedule.ScheduleRef.ScheduleRefEntry;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.mockito.Mock;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;

@Listeners(MockitoTestNGListener.class)
public class IndexBackedScheduleQueryExecutorTest {

    @Mock private ChannelResolver channelResolver;
    @Mock private ScheduleIndex index;
    @Mock private ContentResolver contentResolver;
    private IndexBackedScheduleQueryExecutor queryExecutor;
    
    @BeforeMethod
    public void setup() {
        queryExecutor = new IndexBackedScheduleQueryExecutor(channelResolver, index, contentResolver);
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testDoesntResolveContentOnEmptyScheduleRef() throws Exception {
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, QueryContext.standard(), Id.valueOf(channel.getId()));

        when(channelResolver.fromId(channel.getId()))
            .thenReturn(Maybe.just(channel));
        when(index.resolveSchedule(METABROADCAST, channel, interval))
            .thenReturn(Futures.immediateFuture(ScheduleRef.forChannel(channel.getCanonicalUri()).build()));
        
        ChannelSchedule channelSchedule = queryExecutor.execute(query).getOnlyResource();

        verify(contentResolver, never()).resolveIds(argThat(any(Iterable.class)));
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getEntries().isEmpty(), is(true));
    }

    @Test
    public void testHandlesSimpleScheduleQuery() throws Exception {
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, QueryContext.standard(), Id.valueOf(channel.getId()));
        
        Item item = itemWithBroadcast("item", channel.getCanonicalUri(), dateTime(25), dateTime(75), "bid");
        addBroadcast(item, channel.getCanonicalUri(), dateTime(125), dateTime(175), "bid2");
        
        when(channelResolver.fromId(channel.getId()))
            .thenReturn(Maybe.just(channel));
        when(index.resolveSchedule(METABROADCAST, channel, interval))
            .thenReturn(Futures.immediateFuture(
                ScheduleRef.forChannel(channel.getCanonicalUri())
                    .addEntry(new ScheduleRefEntry(item.getId().longValue(), channel.getCanonicalUri(), dateTime(25), dateTime(75), "bid"))
                    .build()
            ));
        
        when(contentResolver.resolveIds(argThat(hasItems(item.getId()))))
            .thenReturn(queryResult(ImmutableList.<Content>of(item)));
        
        ChannelSchedule channelSchedule = queryExecutor.execute(query).getOnlyResource();

        verify(contentResolver).resolveIds(argThat(hasItems(item.getId())));
        
        assertThat(channelSchedule.getChannel(), is(channel));
        
        Item scheduleItem = Iterables.getOnlyElement(channelSchedule.getEntries()).getItem();
        assertThat(scheduleItem.getCanonicalUri(), is(item.getCanonicalUri()));
        Version scheduleVersion = Iterables.getOnlyElement(scheduleItem.getVersions());
        Broadcast scheduleBroadcast = Iterables.getOnlyElement(scheduleVersion.getBroadcasts());
        assertThat(scheduleBroadcast.getSourceId(), is("bid"));
    }
    
    @Test
    public void testRepeatedScheduleItemAppearsTwice() throws Exception {
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        channel.setCanonicalUri("one");
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ScheduleQuery query = ScheduleQuery.single(METABROADCAST, interval, QueryContext.standard(), Id.valueOf(channel.getId()));
        
        Item item = itemWithBroadcast("item", channel.getCanonicalUri(), dateTime(25), dateTime(75), "bid");
        addBroadcast(item, channel.getCanonicalUri(), dateTime(125), dateTime(175), "bid2");
        
        when(channelResolver.fromId(channel.getId()))
            .thenReturn(Maybe.just(channel));
        when(index.resolveSchedule(METABROADCAST, channel, interval))
            .thenReturn(Futures.immediateFuture(
                ScheduleRef.forChannel(channel.getCanonicalUri())
                    .addEntry(new ScheduleRefEntry(item.getId().longValue(), channel.getCanonicalUri(), dateTime(25), dateTime(75), "bid"))
                    .addEntry(new ScheduleRefEntry(item.getId().longValue(), channel.getCanonicalUri(), dateTime(125), dateTime(175), "bid2"))
                    .build()
            ));
        
        when(contentResolver.resolveIds(argThat(hasItems(item.getId()))))
            .thenReturn(queryResult(ImmutableList.<Content>of(item)));
    
        ChannelSchedule channelSchedule = queryExecutor.execute(query).getOnlyResource();
    
        verify(contentResolver).resolveIds(argThat(hasItems(item.getId())));
        
        assertThat(channelSchedule.getChannel(), is(channel));
        
        List<Item> items = Lists.transform(channelSchedule.getEntries(),ItemAndBroadcast.toItem());
        assertThat(items.size(), is(2));
        
        Item firstItem = items.get(0);
        assertThat(firstItem.getCanonicalUri(), is(item.getCanonicalUri()));
        Version scheduleVersion = Iterables.getOnlyElement(firstItem.getVersions());
        Broadcast scheduleBroadcast = Iterables.getOnlyElement(scheduleVersion.getBroadcasts());
        assertThat(scheduleBroadcast.getSourceId(), is("bid"));

        Item secondItem = items.get(1);
        assertThat(secondItem.getCanonicalUri(), is(item.getCanonicalUri()));
        scheduleVersion = Iterables.getOnlyElement(secondItem.getVersions());
        scheduleBroadcast = Iterables.getOnlyElement(scheduleVersion.getBroadcasts());
        assertThat(scheduleBroadcast.getSourceId(), is("bid2"));
        
    }

    private void addBroadcast(Item item, String channelUri, DateTime start, DateTime end, String bid) {
        Broadcast broadcast = new Broadcast(channelUri, start, end);
        broadcast.withId(bid);
        
        Version version = Iterables.getFirst(item.getVersions(), new Version());
        version.addBroadcast(broadcast);
    }

    private ListenableFuture<Resolved<Content>> queryResult(List<Content> content) {
        return Futures.immediateFuture(Resolved.valueOf(content));
    }

    private Item itemWithBroadcast(String itemUri, String channelUri, DateTime start, DateTime end, String bid) {
        
        Broadcast broadcast = new Broadcast(channelUri, start, end);
        broadcast.withId(bid);
        
        Version version = new Version();
        version.addBroadcast(broadcast);
        
        Item item = new Item(itemUri, itemUri, Publisher.METABROADCAST);
        item.setId(Id.valueOf(Long.valueOf(itemUri.hashCode())));
        item.addVersion(version);
        
        return item;
    }
    
    private DateTime dateTime(int ts) {
        return new DateTime(ts, DateTimeZones.UTC);
    }
}
