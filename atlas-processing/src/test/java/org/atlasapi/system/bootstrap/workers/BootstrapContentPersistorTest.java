package org.atlasapi.system.bootstrap.workers;

import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.when;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Version;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleHierarchy;
import org.atlasapi.schedule.ScheduleWriter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.time.DateTimeZones;

@Listeners(MockitoTestNGListener.class)
public class BootstrapContentPersistorTest {

    @Mock private ContentStore contentStore;
    @Mock private ScheduleWriter scheduleWriter;
    @Mock private ChannelResolver channelResolver;
    
    private BootstrapContentPersistor persistor;
    
    @BeforeClass
    public void setup() {
        persistor = new BootstrapContentPersistor(contentStore, scheduleWriter, channelResolver);
    }
    
    @Test(expectedExceptions=MissingResourceException.class)
    public void testThrowsUnwrappedWriteExceptionFromContentStore() throws WriteException {
        
        Content content = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        
        when(contentStore.writeContent(content)).thenThrow(new MissingResourceException(Id.valueOf(2)));
        
        persistor.writeContent(content);
        
    }

    @Test(expectedExceptions=MissingResourceException.class)
    public void testThrowsUnwrappedWriteExceptionFromScheduleWriter() throws WriteException {
        
        Channel channel = Channel.builder().withUri("channel").build();
        channel.setId(1L);
        
        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Version version = new Version();
        item.addVersion(version);
        
        Item resolved = item.copy();

        Interval interval = new Interval(new DateTime(DateTimeZones.UTC),new DateTime(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel.getUri(), interval);
        broadcast.withId("sourceId");
        version.addBroadcast(broadcast);
        
        ItemAndBroadcast iab = new ItemAndBroadcast(item, broadcast);
        when(scheduleWriter.writeSchedule(ImmutableList.of(ScheduleHierarchy.itemOnly(iab)), channel, interval))
            .thenThrow(new MissingResourceException(Id.valueOf(2)));
        when(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))))
            .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableList.<Content>of(resolved))));
        
        persistor.writeContent(item);
        
    }
}
