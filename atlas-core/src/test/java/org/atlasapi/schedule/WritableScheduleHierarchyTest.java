package org.atlasapi.schedule;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Identified;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Series;
import org.atlasapi.content.Version;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.content.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class WritableScheduleHierarchyTest {
    
    @Mock private ContentStore store;
    private final Channel channel = Channel.builder().build();
    
    @Before
    public void setup() throws WriteException {
        when(store.writeContent(Mockito.argThat(any(Content.class))))
            .then(new Answer<WriteResult<Content>>() {
                @Override
                public WriteResult<Content> answer(InvocationOnMock invocation) throws Throwable {
                    Content written = (Content) invocation.getArguments()[0];
                    Content copy = (Content) written.copy();
                    if (copy.getId() == null) {
                        copy.setId(copy.hashCode());
                    }
                    return WriteResult.written(copy).withPrevious(written).build();
                }
            });
    }
    
    @Test
    public void testWritingANewItemAndBroadcast() throws WriteException {

        Broadcast b1 = broadcast("one", channel);
        ScheduleHierarchy niab
            = ScheduleHierarchy.itemOnly(andBroadcast(item(null, "one", Publisher.METABROADCAST), b1));
        
        List<WriteResult<? extends Content>> results
            = WritableScheduleHierarchy.from(ImmutableList.of(niab)).writeTo(store);
        
        verify(store).writeContent(niab.getItemAndBroadcast().getItem());
        
        Content written = Iterables.getOnlyElement(results).getResource();
        assertEquals(written.getId().longValue(), (long)written.hashCode());
        
    }

    @Test
    public void testWritingAnItemThatAppearsTwiceWithoutIdedVersions() throws WriteException {
        
        Broadcast b1 = broadcast("one", channel);
        Broadcast b2 = broadcast("two", channel);
        
        ScheduleHierarchy iab1
            = ScheduleHierarchy.itemOnly(andBroadcast(item(1, "one", Publisher.METABROADCAST), b1));
        ScheduleHierarchy iab2
            = ScheduleHierarchy.itemOnly(andBroadcast(item(1, "one", Publisher.METABROADCAST), b2));
        
        List<WriteResult<? extends Content>> results
            = WritableScheduleHierarchy.from(ImmutableList.of(iab1, iab2)).writeTo(store);
        
        ArgumentCaptor<Content> contentCaptor = ArgumentCaptor.forClass(Content.class);
        verify(store, times(1)).writeContent(contentCaptor.capture());
        
        Item written = (Item) contentCaptor.getValue();
        
        assertEquals(Iterables.getOnlyElement(results).getResource().getId(), written.getId());
        assertThat(Iterables.getOnlyElement(written.getVersions()).getBroadcasts(), hasItems(b1, b2));
    }
    
    @Test
    public void testWritingAnItemThatAppearsTwiceWithSameIdedVersions() throws WriteException {
        
        Item i1 = item(1, "one", Publisher.METABROADCAST);
        Broadcast b1 = broadcast("one", channel);
        Version v1 = new Version();
        v1.setCanonicalUri("version");
        v1.addBroadcast(b1);
        i1.addVersion(v1);
        
        Item i2 = item(1, "one", Publisher.METABROADCAST);
        Broadcast b2 = broadcast("two", channel);
        Version v2 = new Version();
        v2.setCanonicalUri("version");
        v2.addBroadcast(b2);
        i2.addVersion(v2);
        
        ScheduleHierarchy iab1 = ScheduleHierarchy.itemOnly(new ItemAndBroadcast(i1, b1));
        ScheduleHierarchy iab2 = ScheduleHierarchy.itemOnly(new ItemAndBroadcast(i2, b2));
        
        List<WriteResult<? extends Content>> results
            = WritableScheduleHierarchy.from(ImmutableList.of(iab1, iab2)).writeTo(store);
        
        ArgumentCaptor<Content> contentCaptor = ArgumentCaptor.forClass(Content.class);
        verify(store, times(1)).writeContent(contentCaptor.capture());
        
        Item written = (Item) contentCaptor.getValue();
        
        assertEquals(Iterables.getOnlyElement(results).getResource().getId(), written.getId());
        assertThat(Iterables.getOnlyElement(written.getVersions()).getBroadcasts(), hasItems(b1, b2));
    }

    @Test
    public void testWritingAnItemThatAppearsTwiceWithDifferentIdedVersions() throws WriteException {
        
        Item i1 = item(1, "one", Publisher.METABROADCAST);
        Broadcast b1 = broadcast("one", channel);
        Version v1 = new Version();
        v1.setCanonicalUri("v1");
        v1.addBroadcast(b1);
        i1.addVersion(v1);
        
        Item i2 = item(1, "one", Publisher.METABROADCAST);
        Broadcast b2 = broadcast("two", channel);
        Version v2 = new Version();
        v2.setCanonicalUri("v2");
        v2.addBroadcast(b2);
        i2.addVersion(v2);
        
        ScheduleHierarchy iab1 = ScheduleHierarchy.itemOnly(new ItemAndBroadcast(i1, b1));
        ScheduleHierarchy iab2 = ScheduleHierarchy.itemOnly(new ItemAndBroadcast(i2, b2));
        
        List<WriteResult<? extends Content>> results
            = WritableScheduleHierarchy.from(ImmutableList.of(iab1, iab2)).writeTo(store);
        
        ArgumentCaptor<Content> contentCaptor = ArgumentCaptor.forClass(Content.class);
        verify(store, times(1)).writeContent(contentCaptor.capture());
        
        Item written = (Item) contentCaptor.getValue();
        
        assertEquals(Iterables.getOnlyElement(results).getResource().getId(), written.getId());
        Map<String,Version> versions = Maps.uniqueIndex(written.getVersions(), Identified.TO_URI);
        assertThat(versions.get("v1").getBroadcasts(), hasItems(b1));
        assertThat(versions.get("v2").getBroadcasts(), hasItems(b2));
    }
    
    @Test
    public void testWritingABrandAppearingTwiceInASchedule() throws WriteException {

        Brand brand = new Brand(Id.valueOf(1), Publisher.METABROADCAST);
        ItemAndBroadcast iab1 = andBroadcast(item(2, "two", Publisher.METABROADCAST), broadcast("two", channel));
        ItemAndBroadcast iab2 = andBroadcast(item(3, "three", Publisher.METABROADCAST), broadcast("three", channel));
        
        ScheduleHierarchy h1 = ScheduleHierarchy.brandAndItem(brand, iab1);
        ScheduleHierarchy h2 = ScheduleHierarchy.brandAndItem(brand, iab2);
        
        WritableScheduleHierarchy.from(ImmutableList.of(h1, h2)).writeTo(store);
        
        ArgumentCaptor<Content> contentCaptor = ArgumentCaptor.forClass(Content.class);
        
        verify(store, times(3)).writeContent(contentCaptor.capture());
        
        assertThat((Brand)contentCaptor.getAllValues().get(0), is(brand));
        for (Content content : contentCaptor.getAllValues().subList(1, 2)) {
            assertThat(((Item)content).getContainer(), is(ParentRef.parentRefFrom(brand)));
        }
        
    }

    @Test
    public void testWritingASeriesAppearingTwiceInASchedule() throws WriteException {
        
        Brand brand = new Brand(Id.valueOf(1), Publisher.METABROADCAST);
        Series series = new Series(Id.valueOf(2), Publisher.METABROADCAST);
        ItemAndBroadcast iab1 = andBroadcast(episode(3, "three", Publisher.METABROADCAST), broadcast("three", channel));
        ItemAndBroadcast iab2 = andBroadcast(episode(4, "four", Publisher.METABROADCAST), broadcast("four", channel));
        
        ScheduleHierarchy h1 = ScheduleHierarchy.brandSeriesAndItem(brand, series, iab1);
        ScheduleHierarchy h2 = ScheduleHierarchy.brandSeriesAndItem(brand, series, iab2);
        
        WritableScheduleHierarchy.from(ImmutableList.of(h1, h2)).writeTo(store);
        
        ArgumentCaptor<Content> contentCaptor = ArgumentCaptor.forClass(Content.class);
        
        verify(store, times(4)).writeContent(contentCaptor.capture());
        
        assertThat((Brand)contentCaptor.getAllValues().get(0), is(brand));
        Series writtenSeries = (Series) contentCaptor.getAllValues().get(1);
        assertThat(writtenSeries, is(series));
        assertThat(writtenSeries.getParent(), is(ParentRef.parentRefFrom(brand)));
        for (Content content : contentCaptor.getAllValues().subList(2, 3)) {
            assertThat(((Item)content).getContainer(), is(ParentRef.parentRefFrom(brand)));
            assertThat(((Episode)content).getSeriesRef(), is(ParentRef.parentRefFrom(series)));
        }
        
    }
    
    private ItemAndBroadcast andBroadcast(Item item, Broadcast broadcast) {
        Version version = new Version();
        item.addVersion(version);
        version.addBroadcast(broadcast);
        return new ItemAndBroadcast(item, broadcast);
    }
    
    private Broadcast broadcast(String broadacstId, Channel channel) {
        DateTime start = new DateTime(DateTimeZones.UTC);
        DateTime end = new DateTime(DateTimeZones.UTC);
        Broadcast b = new Broadcast(channel.getCanonicalUri(), start, end);
        b.withId(broadacstId);
        return b;
    }

    private Item item(Integer id, String alias, Publisher source) {
        Item item = new Item();
        if (id != null) {
            item.setId(id);
        }
        item.addAlias(new Alias("uri", alias));
        item.setPublisher(source);
        return item;
    }

    private Item episode(Integer id, String alias, Publisher source) {
        Item item = new Episode();
        if (id != null) {
            item.setId(id);
        }
        item.addAlias(new Alias("uri", alias));
        item.setPublisher(source);
        return item;
    }
    
}
