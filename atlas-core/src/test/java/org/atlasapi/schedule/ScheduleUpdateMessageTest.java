package org.atlasapi.schedule;

import static org.junit.Assert.assertEquals;

import org.atlasapi.content.BroadcastRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.JacksonMessageSerializer;
import org.atlasapi.messaging.MessageException;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

public class ScheduleUpdateMessageTest {

    @Test
    public void testDeSerializeMessage() throws MessageException {
        
        JacksonMessageSerializer serializer = new JacksonMessageSerializer();
        
        ScheduleUpdate update = new ScheduleUpdate(Publisher.METABROADCAST,
            ScheduleRef.forChannel(Id.valueOf(1), new Interval(1, 2))
                .addEntry(Id.valueOf(1), new BroadcastRef("a", Id.valueOf(1), new Interval(1,2)))
            .build(), 
            ImmutableSet.of(new BroadcastRef("b", Id.valueOf(2), new Interval(1,2)))
        );
        ScheduleUpdateMessage msg
            = new ScheduleUpdateMessage("1", Timestamp.of(DateTime.now(DateTimeZones.UTC)), update);
        
        ByteSource serialized = serializer.serialize(msg);
        
        ScheduleUpdateMessage deserialized = serializer.deserialize(serialized);
        
        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getScheduleUpdate().getSource(), deserialized.getScheduleUpdate().getSource());
        assertEquals(msg.getScheduleUpdate().getSchedule(), deserialized.getScheduleUpdate().getSchedule());
        assertEquals(msg.getScheduleUpdate().getStaleBroadcasts(), deserialized.getScheduleUpdate().getStaleBroadcasts());
        
    }

    @Test
    public void testDeSerializeMessageWithoutStaleBroadcasts() throws MessageException {
        
        JacksonMessageSerializer serializer = new JacksonMessageSerializer();
        
        ScheduleUpdate update = new ScheduleUpdate(Publisher.METABROADCAST,
            ScheduleRef.forChannel(Id.valueOf(1), new Interval(1, 2))
                .addEntry(Id.valueOf(1), new BroadcastRef("a", Id.valueOf(1), new Interval(1,2)))
            .build(), 
            ImmutableSet.<BroadcastRef>of()
        );
        ScheduleUpdateMessage msg
        = new ScheduleUpdateMessage("1", Timestamp.of(DateTime.now(DateTimeZones.UTC)), update);
        
        ByteSource serialized = serializer.serialize(msg);
        
        ScheduleUpdateMessage deserialized = serializer.deserialize(serialized);
        
        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getScheduleUpdate().getSource(), deserialized.getScheduleUpdate().getSource());
        assertEquals(msg.getScheduleUpdate().getSchedule(), deserialized.getScheduleUpdate().getSchedule());
        assertEquals(msg.getScheduleUpdate().getStaleBroadcasts(), deserialized.getScheduleUpdate().getStaleBroadcasts());
        
    }
}
