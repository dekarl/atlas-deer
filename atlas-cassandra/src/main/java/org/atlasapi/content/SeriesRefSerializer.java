package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ProtoBufUtils;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.source.Sources;

import com.google.common.primitives.Ints;

public class SeriesRefSerializer {

    private Publisher deflt;

    public SeriesRefSerializer(Publisher publisher) {
        this.deflt = publisher;
    }

    public CommonProtos.Reference.Builder serialize(SeriesRef series) {
        CommonProtos.Reference.Builder ref = CommonProtos.Reference.newBuilder();
        ref.setId(series.getId().longValue());
        ref.setSource(series.getPublisher().key());
        ref.setSort(series.getTitle());
        ref.setUpdated(ProtoBufUtils.serializeDateTime(series.getUpdated()));
        return ref;
    }

    public SeriesRef deserialize(Reference ref) {
        return new SeriesRef(
            Id.valueOf(ref.getId()),
            Sources.fromPossibleKey(ref.getSource()).or(deflt),
            ref.getSort(),
            Ints.saturatedCast(ref.getPosition()),
            ProtoBufUtils.deserializeDateTime(ref.getUpdated()));
    }

}
