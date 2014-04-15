package org.atlasapi.system.bootstrap.workers;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.serialization.json.JsonFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.Timestamp;

public class ContentEquivalenceAssertionLegacyMessageSerializerTest {

    private final MessageSerializer<EquivalenceAssertionMessage> serializer
        = new ContentEquivalenceAssertionLegacyMessageSerializer();
    
    @Test
    public void testDeSerializesContentEquivalenceAssertionMessage() throws Exception {
        ImmutableList<AdjacentRef> adjs = ImmutableList.of(new AdjacentRef(123L,"item",Publisher.PA));
        ContentEquivalenceAssertionMessage msg = new ContentEquivalenceAssertionMessage(
            "1", Timestamp.of(1L), "1", "item", "bbc.co.uk", adjs, Publisher.all());
        
        byte[] serialized = JsonFactory.makeJsonMapper().writeValueAsBytes(msg);
        
        EquivalenceAssertionMessage deserialized
            = serializer.deserialize(serialized);
        
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getSubject().getId().toString(), is(msg.getEntityId()));
        assertThat(deserialized.getSubject().getPublisher().toString(), is(msg.getEntitySource()));
        assertThat(deserialized.getSubject().getResourceType(), is(ResourceType.CONTENT));
        assertThat(deserialized.getPublishers(), is(msg.getSources()));
        assertThat(Iterables.getOnlyElement(deserialized.getAssertedAdjacents()).getId().longValue(), 
                is(Iterables.getOnlyElement(adjs).getId()));
        assertThat(Iterables.getOnlyElement(deserialized.getAssertedAdjacents()).getPublisher(), 
                is(Iterables.getOnlyElement(adjs).getSource()));
        assertThat(Iterables.getOnlyElement(deserialized.getAssertedAdjacents()), instanceOf(ItemRef.class));
        
    }
}
