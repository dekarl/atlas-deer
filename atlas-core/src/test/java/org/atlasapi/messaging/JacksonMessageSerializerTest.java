package org.atlasapi.messaging;

import static org.junit.Assert.assertThat;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.Test;

import static org.hamcrest.Matchers.is;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.Topic;
import org.joda.time.DateTime;

import com.google.common.io.ByteSource;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

public class JacksonMessageSerializerTest {

    private final JacksonMessageSerializer serializer = new JacksonMessageSerializer();
    
    @Test
    public void testDeSerializationOfItemUpdateMessage() throws Exception {
        Item item = new Item(Id.valueOf(1), Publisher.BBC);
        item.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        ResourceUpdatedMessage msg = new ResourceUpdatedMessage("1", Timestamp.of(1234), item.toRef());
        
        ByteSource serialized = serializer.serialize(msg);
        
        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof ResourceUpdatedMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));
        
    }

    @Test
    public void testDeSerializationOfEpisodeUpdateMessage() throws Exception {
        Episode episode = new Episode(Id.valueOf(1), Publisher.BBC);
        episode.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        ResourceUpdatedMessage msg = new ResourceUpdatedMessage("1", Timestamp.of(1234), episode.toRef());
        
        ByteSource serialized = serializer.serialize(msg);
        
        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof ResourceUpdatedMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));
        
    }

    @Test
    public void testDeSerializationOfBrandUpdateMessage() throws Exception {
        Brand brand = new Brand(Id.valueOf(1), Publisher.BBC);
        brand.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        ResourceUpdatedMessage msg = new ResourceUpdatedMessage("1", Timestamp.of(1234), brand.toRef());
        
        ByteSource serialized = serializer.serialize(msg);
        
        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof ResourceUpdatedMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));
        
    }

    @Test
    public void testDeSerializationOfTopicUpdateMessage() throws Exception {
        Topic topic = new Topic(Id.valueOf(1));
        topic.setPublisher(Publisher.BBC);
        topic.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        ResourceUpdatedMessage msg = new ResourceUpdatedMessage("1", Timestamp.of(1234), topic.toRef());
        
        ByteSource serialized = serializer.serialize(msg);
        
        ResourceUpdatedMessage deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof ResourceUpdatedMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        assertThat(deserialized.getUpdatedResource(), is(msg.getUpdatedResource()));
        
    }

    @Test
    public void testDeSerializationOfBeginReplayMessage() throws Exception {
        
        BeginReplayMessage msg = new BeginReplayMessage("1", Timestamp.of(1234));
        
        ByteSource serialized = serializer.serialize(msg);
        
        BeginReplayMessage deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof BeginReplayMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        
    }

    @Test
    public void testDeSerializationOfEndReplayMessage() throws Exception {
        
        EndReplayMessage msg = new EndReplayMessage("1", Timestamp.of(1234));
        
        ByteSource serialized = serializer.serialize(msg);
        
        EndReplayMessage deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof EndReplayMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        
    }

    @Test
    public void testDeSerializationOfReplayMessage() throws Exception {
        
        Series series = new Series(Id.valueOf(1), Publisher.BBC).withSeriesNumber(1);
        series.setTitle("Series");
        series.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        ResourceRef updated = series.toRef();
        
        ResourceUpdatedMessage original = new ResourceUpdatedMessage("0", Timestamp.of(123), updated );
        ReplayMessage<ResourceUpdatedMessage> msg = new ReplayMessage<>("1", Timestamp.of(1234), original);
        
        ByteSource serialized = serializer.serialize(msg);
        
        ReplayMessage<ResourceUpdatedMessage> deserialized = serializer.deserialize(serialized);
        
        assertTrue(deserialized instanceof ReplayMessage);
        assertThat(deserialized.getMessageId(), is(msg.getMessageId()));
        assertThat(deserialized.getTimestamp(), is(msg.getTimestamp()));
        
    }

}
