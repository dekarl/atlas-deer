package org.atlasapi.content;

import org.atlasapi.entity.ProtoBufUtils;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.DateTimeZones;

public class ReleaseDateSerializer {

    public ContentProtos.ReleaseDate serialize(ReleaseDate releaseDate) {
        ContentProtos.ReleaseDate.Builder date = ContentProtos.ReleaseDate.newBuilder();
        date.setDate(ProtoBufUtils.serializeDateTime(releaseDate.date().toDateTimeAtStartOfDay(DateTimeZones.UTC)));
        date.setCountry(releaseDate.country().code());
        date.setType(releaseDate.type().toString());
        return date.build();
    }

    public ReleaseDate deserialize(ContentProtos.ReleaseDate date) {
        return new ReleaseDate(
            ProtoBufUtils.deserializeDateTime(date.getDate()).toLocalDate(),
            Countries.fromCode(date.getCountry()),
            ReleaseDate.ReleaseType.valueOf(date.getType()));
    }
    
}
