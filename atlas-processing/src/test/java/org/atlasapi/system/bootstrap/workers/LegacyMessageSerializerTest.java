package org.atlasapi.system.bootstrap.workers;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
        
        Message msg = new EntityUpdatedMessage("1", new DateTime().getMillis(), "id", "type", "src");
        
        ByteSource serialized = ByteSource.wrap(JsonFactory.makeJsonMapper().writeValueAsBytes(msg));
        
        org.atlasapi.messaging.Message deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof org.atlasapi.messaging.EntityUpdatedMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getEntityId(), is(msg.getEntityId()));
        assertThat(deserialized.getEntityType(), is(msg.getEntityType()));
        assertThat(deserialized.getEntitySource(), is(msg.getEntitySource()));
        
    }

}
