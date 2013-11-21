package org.atlasapi.messaging;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.io.ByteSource;


public class JacksonMessageSerializerTest {

    private final JacksonMessageSerializer serializer = new JacksonMessageSerializer();
    
    @Test
    public void testDeSerialization() throws Exception {
        Message msg = new EntityUpdatedMessage("1", new DateTime().getMillis(), "id", "type", "src");
        
        ByteSource serialized = serializer.serialize(msg);
        
        Message deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof EntityUpdatedMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getEntityId(), is(msg.getEntityId()));
        assertThat(deserialized.getEntityType(), is(msg.getEntityType()));
        assertThat(deserialized.getEntitySource(), is(msg.getEntitySource()));
        
    }

}
