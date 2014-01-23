package org.atlasapi.schedule;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertFalse;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.CassandraContentStore;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Version;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.MessageSender;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.mockito.Mock;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.SequenceGenerator;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

@Listeners(MockitoTestNGListener.class)
public class CassandraScheduleStoreIT {

    private static final String SCHEDULE_CF_NAME = "schedule";
    private static final String CONTENT_CF_NAME = "content";
    private static final String CONTENT_ALIASES_CF_NAME = "content_aliases";

    private static final AstyanaxContext<Keyspace> context =
            CassandraHelper.testCassandraContext();

    //hasher is mock till we have a non-Mongo based one.
    @Mock private ContentHasher hasher;
    @Mock private MessageSender sender; 
    
    private final Clock clock = new TimeMachine();
    
    private CassandraContentStore contentStore;
    private CassandraScheduleStore store;
    
    private final Publisher source = Publisher.METABROADCAST;
    private final Channel channel = Channel.builder().build();

    @BeforeClass
    public static void setup() throws ConnectionException {
        context.start();
        CassandraHelper.createKeyspace(context);
        CassandraHelper.createColumnFamily(context,
                SCHEDULE_CF_NAME,
                StringSerializer.get(),
                StringSerializer.get());
        CassandraHelper.createColumnFamily(context, CONTENT_CF_NAME, LongSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, CONTENT_ALIASES_CF_NAME, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    }

    @AfterClass
    public static void tearDown() throws ConnectionException {
        context.getClient().dropKeyspace();
    }
    
    @BeforeMethod
    public void setUp() {
        channel.setCanonicalUri("channel");
        channel.setId(1234L);
        contentStore = CassandraContentStore
                .builder(context, CONTENT_CF_NAME, hasher, sender, new SequenceGenerator())
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .build();
        store = CassandraScheduleStore
                .builder(context, SCHEDULE_CF_NAME, contentStore)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .build();
    }

    @AfterMethod
    public void clearCf() throws ConnectionException {
        context.getClient().truncateColumnFamily(SCHEDULE_CF_NAME);
        context.getClient().truncateColumnFamily(CONTENT_CF_NAME);
        context.getClient().truncateColumnFamily(CONTENT_ALIASES_CF_NAME);
    }

    @Test
    public void testWritingAndResolvingANewSchedule() throws Exception {
        
        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,23,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(itemAndBroadcast(null, "one", source, channel, start, middle)), 
            ScheduleHierarchy.itemOnly(itemAndBroadcast(null, "two", source, channel, middle, end))
        );
        
        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        
        assertThat(results.size(), is(2));
        
        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,05,31,22,30,0,0,DateTimeZones.UTC) 
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(2L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(1L));
    }

    @Test
    public void testReWritingASchedule() throws Exception {

        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,23,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        ItemAndBroadcast episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode2 = itemAndBroadcast(null, "two", source, channel, middle, end);
        
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode2)
        );
        
        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));

        DateTime newMiddle = new DateTime(2013,05,31,23,30,0,0,DateTimeZones.LONDON);
        
        //items 1 and 2 change 
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("differentOne")
            .thenReturn("oneDifferent")
            .thenReturn("differentTwo")
            .thenReturn("twoDifferent");
        
        episode1 = itemAndBroadcast(null, "one", source, channel, start, newMiddle);
        ItemAndBroadcast episode3 = itemAndBroadcast(null, "three", source, channel, newMiddle, end);
        hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode3)
        );
        
        results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));
        AssertJUnit.assertTrue(Iterables.all(results, WriteResult.<Content>writtenFilter()));
        
        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,05,31,22,40,0,0,DateTimeZones.UTC) 
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(2L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(3L));
        
        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))));
        Item two = (Item) resolved.getResources().first().get();
        assertFalse(Iterables.getOnlyElement(Iterables.getOnlyElement(two.getVersions()).getBroadcasts()).isActivelyPublished());
    }
    
    @Test
    public void testReWritingScheduleUpdatesOverlappingBroadcastsInDifferentSegment() throws Exception {

        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,20,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        ItemAndBroadcast episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode2 = itemAndBroadcast(null, "two", source, channel, middle, end);
        
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode2)
        );
        
        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));

        DateTime newEnd = new DateTime(2013,05,31,23,30,0,0,DateTimeZones.LONDON);
        
        //items 1 and 2 change 
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("differentOne")
            .thenReturn("oneDifferent")
            .thenReturn("differentTwo")
            .thenReturn("twoDifferent");
        
        episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode3 = itemAndBroadcast(null, "three", source, channel, middle, newEnd);
        hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode3)
        );
        
        results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));
        AssertJUnit.assertTrue(Iterables.all(results, WriteResult.<Content>writtenFilter()));
        
        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,05,31,22,40,0,0,DateTimeZones.UTC) 
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(2L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(3L));
        
        requestedInterval = new Interval(
            new DateTime(2013,05,31,23,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,06,01,22,40,0,0,DateTimeZones.UTC) 
        );
        schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(0));
        
        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))));
        Item two = (Item) resolved.getResources().first().get();
        assertFalse(Iterables.getOnlyElement(Iterables.getOnlyElement(two.getVersions()).getBroadcasts()).isActivelyPublished());
    }
    
    @Test
    public void testWritingAnItemWithManyBroadcastsOnlyHasTheRelevantBroadcastInTheResolvedSchedule() throws Exception {
        
        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,20,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        Item item1 = item(1, source, "item");
        Broadcast broadcast1 = broadcast("one", channel, start, middle);
        Broadcast broadcast2 = broadcast("two", channel, middle, end);
        Broadcast broadcast3 = broadcast("three", channel, new DateTime(DateTimeZones.LONDON), new DateTime(DateTimeZones.LONDON));
        
        Version version1 = new Version();
        version1.setCanonicalUri("one");
        version1.addBroadcast(broadcast1);
        version1.addBroadcast(broadcast3);
        item1.addVersion(version1);
        
        Version version2 = new Version();
        version2.setCanonicalUri("two");
        version2.addBroadcast(broadcast2);
        item1.addVersion(version2);
        
        ItemAndBroadcast iab1 = new ItemAndBroadcast(item1, broadcast1);
        ItemAndBroadcast iab2 = new ItemAndBroadcast(item1.copy(), broadcast2);
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(iab1), 
            ScheduleHierarchy.itemOnly(iab2)
        );
        Interval writtenInterval = new Interval(start, end);
        
        when(hasher.hash(argThat(any(Content.class)))).thenReturn("one", "two", "three");
        
        List<WriteResult<? extends Content>> results
            = store.writeSchedule(hiers, channel, writtenInterval);

        verify(hasher, never()).hash(argThat(is(any(Content.class))));
        assertThat(results.size(), is(1));

        Schedule schedule = future(store.resolve(ImmutableList.of(channel), writtenInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        assertThat(channelSchedule.getEntries().size(), is(2));
        ItemAndBroadcast fst = channelSchedule.getEntries().get(0);
        ItemAndBroadcast snd = channelSchedule.getEntries().get(1);
        
        Item resolved1 = fst.getItem();
        ImmutableMap<String, Version> versionIndex = Maps.uniqueIndex(resolved1.getVersions(), Identified.TO_URI);
        assertThat(Iterables.getOnlyElement(versionIndex.get("one").getBroadcasts()), is(fst.getBroadcast()));
        assertThat(versionIndex.get("two").getBroadcasts(), is(empty()));

        Item resolved2 = snd.getItem();
        versionIndex = Maps.uniqueIndex(resolved2.getVersions(), Identified.TO_URI);
        assertThat(versionIndex.get("one").getBroadcasts(), is(empty()));
        assertThat(Iterables.getOnlyElement(versionIndex.get("two").getBroadcasts()), is(snd.getBroadcast()));
        
        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))));
        Item item = (Item) resolved.toMap().get(Id.valueOf(1)).get();
        assertThat(item.getVersions().size(), is(2));
        versionIndex = Maps.uniqueIndex(item.getVersions(), Identified.TO_URI);
        assertThat(versionIndex.get("one").getBroadcasts(), hasItems(broadcast1, broadcast3));
        assertThat(versionIndex.get("two").getBroadcasts(), hasItem(broadcast2));
    }

    @Test(enabled = false)
    // TODO Known issue: if the update interval doesn't cover a now stale broadcast
    // in a subsequent segment then that broadcast won't be updated in the
    // subsequent segment, it will be updated in the store though.
    public void testReWritingScheduleUpdatesOverlappingBroadcastsInDifferentSegmentEvenWhenUpdateIntervalDoesntCoverSegment() throws Exception {
        
        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,20,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        ItemAndBroadcast episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode2 = itemAndBroadcast(null, "two", source, channel, middle, end);
        
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode2)
        );
        
        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));
        
        DateTime newEnd = new DateTime(2013,05,31,23,30,0,0,DateTimeZones.LONDON);
        
        //items 1 and 2 change 
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("differentOne")
            .thenReturn("oneDifferent")
            .thenReturn("differentTwo")
            .thenReturn("twoDifferent");
        
        episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode3 = itemAndBroadcast(null, "three", source, channel, middle, newEnd);
        hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode3)
        );
        
        results = store.writeSchedule(hiers, channel, new Interval(start, newEnd));
        assertThat(results.size(), is(2));
        AssertJUnit.assertTrue(Iterables.all(results, WriteResult.<Content>writtenFilter()));
        
        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,05,31,22,40,0,0,DateTimeZones.UTC) 
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(1L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(3L));
        
        requestedInterval = new Interval(
            new DateTime(2013,05,31,23,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,06,01,22,40,0,0,DateTimeZones.UTC) 
        );
        schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(0));
        
        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(2))));
        Item two = (Item) resolved.getResources().first().get();
        assertFalse(Iterables.getOnlyElement(Iterables.getOnlyElement(two.getVersions()).getBroadcasts()).isActivelyPublished());
    }

    private <T> T future(ListenableFuture<T> resolve) throws Exception {
        return Futures.get(resolve, 1, TimeUnit.SECONDS, Exception.class);
    }

    private ItemAndBroadcast itemAndBroadcast(Integer id, String alias, Publisher source, Channel channel, DateTime start, DateTime end) {
        Item item = item(id, source, alias);
        Version version = new Version();
        item.addVersion(version);
        Broadcast broadcast = broadcast(alias, channel, start, end);
        version.addBroadcast(broadcast);
        return new ItemAndBroadcast(item, broadcast);
    }

    private Broadcast broadcast(String broadacstId, Channel channel, DateTime start, DateTime end) {
        Broadcast b = new Broadcast(channel.getCanonicalUri(), start, end);
        b.withId(broadacstId);
        return b;
    }

    private Item item(Integer id, Publisher source, String alias) {
        Item item = new Item();
        if (id != null) {
            item.setId(id);
        }
        item.addAlias(new Alias("uri", alias));
        item.setPublisher(source);
        return item;
    }
    
}
