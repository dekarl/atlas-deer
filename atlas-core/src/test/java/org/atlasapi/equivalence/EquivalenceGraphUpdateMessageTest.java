package org.atlasapi.equivalence;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.BrandRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.MessageException;
import org.atlasapi.messaging.MessageSerializer;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.metabroadcast.common.time.Timestamp;

public class EquivalenceGraphUpdateMessageTest {

    MessageSerializer serializer = new JacksonMessageSerializer();

    @Test
    public void testDeSerializationWithoutCreatedDeleted() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
            EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
            ImmutableSet.<EquivalenceGraph> of(),
            ImmutableSet.<Id> of()
        ));
    }

    @Test
    public void testDeSerializationWithoutDeleted() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
            EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
            ImmutableSet.of(EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(2), Publisher.BBC))),
            ImmutableSet.<Id> of()
        ));
    }
    
    @Test
    public void testDeSerializationWithoutCreated() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
            EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
            ImmutableSet.<EquivalenceGraph> of(),
            ImmutableSet.<Id> of(Id.valueOf(1))
        ));
    }

    @Test
    public void testDeSerialization() throws Exception {
        testSerializingMessageWith(new EquivalenceGraphUpdate(
            EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(1), Publisher.BBC)),
            ImmutableSet.of(EquivalenceGraph.valueOf(new BrandRef(Id.valueOf(2), Publisher.BBC))),
            ImmutableSet.<Id> of(Id.valueOf(1))
        ));
    }

    private void testSerializingMessageWith(EquivalenceGraphUpdate update) throws MessageException {
        EquivalenceGraphUpdateMessage egum =
            new EquivalenceGraphUpdateMessage("message", Timestamp.of(0), update);

        ByteSource serialized = serializer.serialize(egum);

        EquivalenceGraphUpdateMessage deserialized = serializer.deserialize(serialized);

        assertThat(deserialized, is(egum));
        assertThat(deserialized.getGraphUpdate(), is(egum.getGraphUpdate()));
    }
}
