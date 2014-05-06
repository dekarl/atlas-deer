package org.atlasapi.content;

import static org.junit.Assert.assertThat;
import org.junit.Test;
import static org.hamcrest.Matchers.is;


public class TopicRefSerializerTest {
    
    private final TopicRefSerializer serializer = new TopicRefSerializer();

    @Test
    public void testDeSerializeTopicRef() {
        TopicRef topicRef = new TopicRef(1234L, null, null, null);
        serializeAndCheck(topicRef);
        topicRef.setSupervised(true);
        serializeAndCheck(topicRef);
        topicRef.setRelationship(TopicRef.Relationship.TRANSCRIPTION);
        serializeAndCheck(topicRef);
        topicRef.setOffset(1243);
        serializeAndCheck(topicRef);
        topicRef.setWeighting(1.0f);
        serializeAndCheck(topicRef);
    }

    private void serializeAndCheck(TopicRef topicRef) {
        TopicRef deserialized = serializer.deserialize(serializer.serialize(topicRef));
        assertThat(deserialized.getTopic(), is(topicRef.getTopic()));
        assertThat(deserialized.isSupervised(), is(topicRef.isSupervised()));
        assertThat(deserialized.getRelationship(), is(topicRef.getRelationship()));
        assertThat(deserialized.getWeighting(), is(topicRef.getWeighting()));
        assertThat(deserialized.getOffset(), is(topicRef.getOffset()));
    }

}
