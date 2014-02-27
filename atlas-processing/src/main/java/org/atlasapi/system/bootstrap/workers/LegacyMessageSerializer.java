package org.atlasapi.system.bootstrap.workers;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.ClipRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.FilmRef;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.SongRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalenceAssertionMessage;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageException;
import org.atlasapi.messaging.MessageSerializer;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.worker.v3.Worker;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.topic.TopicRef;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;


public class LegacyMessageSerializer implements MessageSerializer {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper()
            .registerModule(new org.atlasapi.messaging.worker.v3.AbstractWorker.MessagingModule());
    
    @Override
    public ByteSource serialize(Message msg) throws MessageException {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <M extends Message> M deserialize(ByteSource bytes) throws MessageException {
        try {
            org.atlasapi.messaging.v3.Message legacy = mapper.readValue(bytes.read(), org.atlasapi.messaging.v3.Message.class);
            final AtomicReference<Message> message = new AtomicReference<>();
            legacy.dispatchTo(new Worker(){

                @Override
                public void process(org.atlasapi.messaging.v3.EntityUpdatedMessage leg) {
                    message.set(new ResourceUpdatedMessage(
                        leg.getMessageId(), 
                        Timestamp.of(leg.getTimestamp()), 
                        resourceRef(leg)
                    ));
                }

                private ResourceRef resourceRef(EntityUpdatedMessage leg) {
                    final Long lid = Long.valueOf(leg.getEntityId());
                    final Publisher src = Publisher.fromKey(leg.getEntitySource()).requireValue();
                    final DateTime updated = new DateTime(leg.getTimestamp(), DateTimeZones.UTC);
                    String type = leg.getEntityType();
                    Optional<ContentType> possContentType = ContentType.fromKey(type);
                    if (possContentType.isPresent()) {
                        return toResourceRef(lid, src, type, updated);
                    } else {
                        Publisher source = Publisher.fromKey(leg.getEntitySource()).requireValue();
                        return new TopicRef(Id.valueOf(lid), source);
                    }
                }

                @Override
                public void process(org.atlasapi.messaging.v3.BeginReplayMessage leg) {
                    
                }

                @Override
                public void process(org.atlasapi.messaging.v3.EndReplayMessage leg) {
                    
                }

                @Override
                public void process(org.atlasapi.messaging.v3.ReplayMessage leg) {
                    
                }
                
                @Override
                public void process(org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage leg) {
                    String mid = leg.getMessageId();
                    Timestamp ts = Timestamp.of(leg.getTimestamp());
                    ResourceRef subj = getSubject(leg);
                    Set<ResourceRef> adjacents = toResourceRef(leg);
                    Set<Publisher> srcs = leg.getSources();
                    message.set(new EquivalenceAssertionMessage(mid, ts, subj, adjacents, srcs));
                }

                private Set<ResourceRef> toResourceRef(ContentEquivalenceAssertionMessage leg) {
                    if (leg.getAdjacent() == null || leg.getAdjacent().isEmpty()) {
                        return ImmutableSet.of();
                    }
                    DateTime madeUpUpdatedTime = new DateTime(leg.getTimestamp(), DateTimeZones.UTC);
                    ImmutableSet.Builder<ResourceRef> resourceRefs = ImmutableSet.builder();
                    for (AdjacentRef adjacentRef : leg.getAdjacent()) {
                        resourceRefs.add(toResourceRef(adjacentRef.getId(), adjacentRef.getSource(), 
                                adjacentRef.getType(), madeUpUpdatedTime));
                    }
                    return resourceRefs.build();
                }

                private ResourceRef getSubject(ContentEquivalenceAssertionMessage leg) {
                    DateTime madeUpUpdatedTime = new DateTime(leg.getTimestamp(), DateTimeZones.UTC);
                    Long lid = Long.valueOf(leg.getEntityId());
                    final Publisher src = Publisher.fromKey(leg.getEntitySource()).requireValue();
                    return toResourceRef(lid, src, leg.getEntityType(), madeUpUpdatedTime);
                }

                private ResourceRef toResourceRef(final Long lid, final Publisher src,
                        String type, final DateTime updated) {
                    final Id rid = Id.valueOf(lid);
                    ContentType contentType = ContentType.fromKey(type).get();
                    return contentType.accept(new ContentType.Visitor<ResourceRef>(){

                        @Override
                        public BrandRef visitBrand(ContentType contentType) {
                            return new BrandRef(rid, src);
                        }

                        @Override
                        public ClipRef visitClip(ContentType contentType) {
                            return new ClipRef(rid, src, "11", updated);
                        }

                        @Override
                        public SongRef visitSong(ContentType contentType) {
                            return new SongRef(rid, src, "11", updated);
                        }

                        @Override
                        public FilmRef visitFilm(ContentType contentType) {
                            return new FilmRef(rid, src, "11", updated);
                        }

                        @Override
                        public EpisodeRef visitEpisode(ContentType contentType) {
                            return new EpisodeRef(rid, src, "11", updated);
                        }

                        @Override
                        public ItemRef visitItem(ContentType contentType) {
                            return new ItemRef(rid, src, "11", updated);
                        }

                        @Override
                        public SeriesRef visitSeries(ContentType contentType) {
                            return new SeriesRef(rid, src);
                        }
                    });
                }
                
            });
            return (M) message.get();
        } catch (Exception e) {
            throw new MessageException(e);
        }
    }

}
