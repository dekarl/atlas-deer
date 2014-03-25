package org.atlasapi.schedule;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.TimeUnit;

import org.atlasapi.PersistenceModule;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;

public abstract class EquivalentScheduleStoreTestSuite {
    
    private EquivalentScheduleStore store;
    private ContentStore contentStore;
    private ScheduleStore scheduleStore;
    private EquivalenceGraphStore graphStore;

    abstract PersistenceModule persistenceModule() throws Exception;
    
    @BeforeClass
    public void setup() throws Exception {
        PersistenceModule module = persistenceModule();
        this.store = module.equivalentScheduleStore();
        this.contentStore = module.contentStore();
        this.scheduleStore = module.scheduleStore();
        this.graphStore = module.contentEquivalenceGraphStore();
    }
    
    @Test
    public void testWritingNewSchedule() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        Interval interval = new Interval(new DateTime(2014,03,21,16,00,00,000, DateTimeZones.UTC), 
                new DateTime(2014,03,21,17,00,00,000, DateTimeZones.UTC));
        
        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);
        
        contentStore.writeContent(item);
        
        ScheduleRef scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
                .addEntry(item.getId(), broadcast.toRef())
            .build();
        
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of()));
        
        EquivalentSchedule resolved
            = get(store.resolveSchedules(ImmutableList.of(channel), interval, Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST)));
        
        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        
        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item));
    }

    @Test
    public void testUpdatingASchedule() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        Interval interval = new Interval(new DateTime(2014,03,21,16,00,00,000, DateTimeZones.UTC), 
                new DateTime(2014,03,21,17,00,00,000, DateTimeZones.UTC));
        
        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast1 = new Broadcast(channel, interval).withId("sid1");
        item1.addBroadcast(broadcast1);

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        Broadcast broadcast2 = new Broadcast(channel, interval).withId("sid2");
        item2.addBroadcast(broadcast2);
        
        contentStore.writeContent(item1);
        contentStore.writeContent(item2);
        
        ScheduleRef scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .build();
        
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of()));

        scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of(
            broadcast1.toRef()
        )));
        
        EquivalentSchedule resolved
            = get(store.resolveSchedules(ImmutableList.of(channel), interval, Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST)));
        
        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        
        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item2));
        assertThat(Iterables.getOnlyElement(schedule.getEntries()).getBroadcast(), is(broadcast2));
    }

    @Test
    public void testWritingRepeatedItem() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        Interval interval = new Interval(new DateTime(2014,03,21,16,00,00,000, DateTimeZones.UTC), 
                new DateTime(2014,03,21,18,00,00,000, DateTimeZones.UTC));
        
        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast1 = new Broadcast(channel, new Interval(interval.getStart(), interval.getStart().plusHours(1))).withId("sid1");
        Broadcast broadcast2 = new Broadcast(channel, new Interval(interval.getEnd().minusHours(1), interval.getEnd())).withId("sid2");
        item.addBroadcast(broadcast1);
        item.addBroadcast(broadcast2);
        
        contentStore.writeContent(item);
        
        ScheduleRef scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
                .addEntry(item.getId(), broadcast1.toRef())
                .addEntry(item.getId(), broadcast2.toRef())
                .build();
        
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of()));
        
        EquivalentSchedule resolved
            = get(store.resolveSchedules(ImmutableList.of(channel), interval, Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST)));
        
        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        
        ImmutableList<EquivalentScheduleEntry> entries = schedule.getEntries();
        
        Item first = Iterables.getOnlyElement(entries.get(0).getItems().getResources());
        assertThat(Iterables.getOnlyElement(first.getBroadcasts()), is(broadcast1));
        Item second = Iterables.getOnlyElement(entries.get(1).getItems().getResources());
        assertThat(Iterables.getOnlyElement(second.getBroadcasts()), is(broadcast2));
    }

    @Test
    public void testWritingNewScheduleWithEquivalentItems() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        Interval interval = new Interval(new DateTime(2014,03,21,16,00,00,000, DateTimeZones.UTC), 
                new DateTime(2014,03,21,17,00,00,000, DateTimeZones.UTC));
        
        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);
        
        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        
        graphStore.updateEquivalences(item.toRef(), ImmutableSet.<ResourceRef>of(equiv.toRef()), ImmutableSet.of(Publisher.METABROADCAST));
        
        contentStore.writeContent(item);
        contentStore.writeContent(equiv);
        
        ScheduleRef scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();
        
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of()));
        
        EquivalentSchedule resolved
        = get(store.resolveSchedules(ImmutableList.of(channel), interval, Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)));
        
        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        
        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(broadcastItems.getResources().size(), is(2));
        assertThat(broadcastItems.getResources(), hasItems(item, equiv));
    }

    @Test
    public void testResolvingScheduleFiltersItemsAccordingToSelectedSources() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        Interval interval = new Interval(new DateTime(2014,03,21,16,00,00,000, DateTimeZones.UTC), 
                new DateTime(2014,03,21,17,00,00,000, DateTimeZones.UTC));
        
        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);
        
        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        
        graphStore.updateEquivalences(item.toRef(), ImmutableSet.<ResourceRef>of(equiv.toRef()), ImmutableSet.of(Publisher.METABROADCAST));
        
        contentStore.writeContent(item);
        contentStore.writeContent(equiv);
        
        ScheduleRef scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();
        
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of()));
        
        EquivalentSchedule resolved
        = get(store.resolveSchedules(ImmutableList.of(channel), interval, Publisher.METABROADCAST, ImmutableSet.of(Publisher.BBC)));
        
        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        
        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(equiv));
    }

    @Test
    public void testWritingNewScheduleWithEquivalentItemsChoosesBestEquivalent() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        Interval interval = new Interval(new DateTime(2014,03,21,16,00,00,000, DateTimeZones.UTC), 
                new DateTime(2014,03,21,17,00,00,000, DateTimeZones.UTC));
        
        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);
        
        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast.withId("sid1"));
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        Item otherEquiv = new Item(Id.valueOf(3), Publisher.BBC);
        otherEquiv.addBroadcast(new Broadcast(channel, interval.getEnd(), interval.getEnd().plusHours(1)).withId("sid2"));
        otherEquiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        
        graphStore.updateEquivalences(item.toRef(), ImmutableSet.<ResourceRef>of(equiv.toRef()), ImmutableSet.of(Publisher.METABROADCAST));
        graphStore.updateEquivalences(otherEquiv.toRef(), ImmutableSet.<ResourceRef>of(item.toRef()), ImmutableSet.of(Publisher.METABROADCAST));
        
        contentStore.writeContent(item);
        contentStore.writeContent(equiv);
        contentStore.writeContent(otherEquiv);
        
        ScheduleRef scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
            .addEntry(item.getId(), broadcast.toRef())
            .build();
        
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of()));
        
        EquivalentSchedule resolved
            = get(store.resolveSchedules(ImmutableList.of(channel), interval, Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)));
        
        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        
        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(broadcastItems.getResources().size(), is(2));
        assertThat(broadcastItems.getResources(), hasItems(item, equiv));
    }

    @Test
    public void testUpdatingEquivalences() throws Exception {
        
        Channel channel = Channel.builder().build();
        channel.setId(1L);
        Interval interval = new Interval(new DateTime(2014,03,21,16,00,00,000, DateTimeZones.UTC), 
                new DateTime(2014,03,21,17,00,00,000, DateTimeZones.UTC));
        
        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);
        
        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast.withId("sid1"));
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        
        Item otherEquiv = new Item(Id.valueOf(3), Publisher.BBC);
        otherEquiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        
        graphStore.updateEquivalences(otherEquiv.toRef(), ImmutableSet.<ResourceRef>of(item.toRef()), ImmutableSet.of(Publisher.METABROADCAST));
        
        contentStore.writeContent(item);
        contentStore.writeContent(equiv);
        contentStore.writeContent(otherEquiv);
        
        ScheduleRef scheduleRef = ScheduleRef.forChannel(Id.valueOf(channel.getId()), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();
        
        store.updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, scheduleRef, ImmutableSet.<BroadcastRef>of()));
        
        EquivalenceGraphUpdate equivUpdate = graphStore.updateEquivalences(item.toRef(), ImmutableSet.<ResourceRef>of(equiv.toRef()), ImmutableSet.of(Publisher.METABROADCAST)).get();
        
        store.updateEquivalences(equivUpdate);
        
        EquivalentSchedule resolved
            = get(store.resolveSchedules(ImmutableList.of(channel), interval, Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)));
        
        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        
        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(broadcastItems.getResources().size(), is(2));
        assertThat(broadcastItems.getResources(), hasItems(item, equiv));
    }
    
    private <T> T get(ListenableFuture<T> future) throws Exception {
        return Futures.get(future, 10, TimeUnit.SECONDS, Exception.class);
    }
    
}
