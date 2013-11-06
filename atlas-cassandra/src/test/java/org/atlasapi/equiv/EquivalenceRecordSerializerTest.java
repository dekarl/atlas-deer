package org.atlasapi.equiv;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.Content;
import org.atlasapi.content.Episode;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Serializer;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class EquivalenceRecordSerializerTest {

    private Serializer<EquivalenceRecord, byte[]> serializer
            = new EquivalenceRecordSerializer();

    @Test
    public void testDeSerializesEquivalenceRecord() {
        EquivalenceRecord record = record(1234L, Publisher.METABROADCAST)
            .copyWithGeneratedAdjacent(ImmutableList.of(ref(1235, Publisher.BBC)))
            .copyWithExplicitAdjacent(ImmutableList.of(ref(1236, Publisher.PA)))
            .copyWithEquivalents(ImmutableList.of(ref(1237, Publisher.C4)));
        
        EquivalenceRecord deserialized = serializer.deserialize(serializer.serialize(record));
        
        assertThat(deserialized.getId(), is(deserialized.getId()));
        assertThat(deserialized.getPublisher(), is(deserialized.getPublisher()));
        assertThat(deserialized.getCreated(), is(deserialized.getCreated()));
        assertThat(deserialized.getUpdated(), is(deserialized.getUpdated()));
        assertThat(deserialized.getGeneratedAdjacents(), is(deserialized.getGeneratedAdjacents()));
        assertThat(deserialized.getExplicitAdjacents(), is(deserialized.getExplicitAdjacents()));
        assertThat(deserialized.getEquivalents(), is(deserialized.getEquivalents()));
        
    }

    private EquivalenceRecord record(long id, Publisher source) {
        Content content = new Episode();
        content.setId(id);
        content.setPublisher(source);
        
        EquivalenceRecord record = EquivalenceRecord.valueOf(content);
        return record;
    }

    private EquivalenceRef ref(int id, Publisher source) {
        return new EquivalenceRef(Id.valueOf(id), source);
    }

}
