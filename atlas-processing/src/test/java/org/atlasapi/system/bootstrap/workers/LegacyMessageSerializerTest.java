package org.atlasapi.system.bootstrap.workers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.atlasapi.entity.ResourceType;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.v3.Message;
import org.atlasapi.serialization.json.JsonFactory;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.io.ByteSource;


public class LegacyMessageSerializerTest {

    LegacyMessageSerializer serializer = new LegacyMessageSerializer();
    
    @Test
    public void testDeSerializesLegacyMessage() throws Exception {
        
        Message msg = new EntityUpdatedMessage("1", new DateTime().getMillis(), "1", "item", "bbc.co.uk");
        
        ByteSource serialized = ByteSource.wrap(JsonFactory.makeJsonMapper().writeValueAsBytes(msg));
        
        org.atlasapi.messaging.ResourceUpdatedMessage deserialized
            = (org.atlasapi.messaging.ResourceUpdatedMessage) serializer.<org.atlasapi.messaging.ResourceUpdatedMessage>deserialize(serialized);
        
        assertTrue(deserialized instanceof org.atlasapi.messaging.ResourceUpdatedMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp().millis(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource().getId().toString(), is(msg.getEntityId()));
        assertThat(deserialized.getUpdatedResource().getPublisher().toString(), is(msg.getEntitySource()));
        assertThat(deserialized.getUpdatedResource().getResourceType(), is(ResourceType.CONTENT));
        
    }

}
