package org.atlasapi.system.bootstrap.workers;

import static org.mockito.Mockito.when;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.MissingResourceException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleWriter;
import org.mockito.Mock;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

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
}
