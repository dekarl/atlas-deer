package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ProtoBufUtils;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.source.Sources;
import org.joda.time.DateTime;

public class ChildRefSerializer {

    private final Publisher deflt;

    public ChildRefSerializer(Publisher deflt) {
        this.deflt = deflt;
    }
    
    public CommonProtos.Reference.Builder serialize(ItemRef itemRef) {
        CommonProtos.Reference.Builder ref = CommonProtos.Reference.newBuilder();
        ref.setId(itemRef.getId().longValue());
        ref.setSource(itemRef.getPublisher().key());
        ref.setSort(itemRef.getSortKey());
        ref.setUpdated(ProtoBufUtils.serializeDateTime(itemRef.getUpdated()));
        ref.setType(itemRef.getContentType().toString());
        return ref;
    }

    public ItemRef deserialize(final Reference ref) {
        final Id id = Id.valueOf(ref.getId());
        final Publisher src = Sources.fromPossibleKey(ref.getSource()).or(deflt);
        final String sortKey = ref.getSort();
        final DateTime updated = ProtoBufUtils.deserializeDateTime(ref.getUpdated());
        ContentType type = ContentType.fromKey(ref.getType()).get();
        
        return type.accept(new ContentType.Visitor<ItemRef>() {
            
            @Override
            public ItemRef visitItem(ContentType contentType) {
                return new ItemRef(id, src, sortKey, updated);
            }
            
            @Override
            public ItemRef visitClip(ContentType contentType) {
                return new ClipRef(id, src, sortKey, updated);
            }
            
            @Override
            public ItemRef visitEpisode(ContentType contentType) {
                return new EpisodeRef(id, src, sortKey, updated);
            }
            
            @Override
            public ItemRef visitFilm(ContentType contentType) {
                return new FilmRef(id, src, sortKey, updated);
            }
            
            @Override
            public ItemRef visitSong(ContentType contentType) {
                return new SongRef(id, src, sortKey, updated);
            }

            @Override
            public ItemRef visitBrand(ContentType contentType) {
                throw new IllegalArgumentException("Can't create ItemRef for type " + contentType);
            }

            @Override
            public ItemRef visitSeries(ContentType contentType) {
                throw new IllegalArgumentException("Can't create ItemRef for type " + contentType);
            }
            
        });
    }

}
