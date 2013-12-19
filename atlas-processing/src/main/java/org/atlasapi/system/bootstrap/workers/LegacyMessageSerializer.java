package org.atlasapi.system.bootstrap.workers;

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
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageException;
import org.atlasapi.messaging.MessageSerializer;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.worker.v3.Worker;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.topic.TopicRef;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
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
                    final Id id = Id.valueOf(leg.getEntityId());
                    final Publisher source = Publisher.fromKey(leg.getEntitySource()).requireValue();
                    final DateTime updated = new DateTime(leg.getTimestamp(), DateTimeZones.UTC);
                    String entityType = leg.getEntityType();
                    Optional<ContentType> possContentType = ContentType.fromKey(entityType);
                    if (possContentType.isPresent()) {
                        return possContentType.get().accept(new ContentType.Visitor<ResourceRef>() {

                            @Override
                            public ResourceRef visitBrand(ContentType contentType) {
                                return new BrandRef(id, source);
                            }

                            @Override
                            public ResourceRef visitClip(ContentType contentType) {
                                return new ClipRef(id, source, "", updated);
                            }

                            @Override
                            public ResourceRef visitSong(ContentType contentType) {
                                return new SongRef(id, source, "", updated);
                            }

                            @Override
                            public ResourceRef visitFilm(ContentType contentType) {
                                return new FilmRef(id, source, "", updated);
                            }

                            @Override
                            public ResourceRef visitEpisode(ContentType contentType) {
                                return new EpisodeRef(id, source, "", updated);
                            }

                            @Override
                            public ResourceRef visitItem(ContentType contentType) {
                                return new ItemRef(id, source, "", updated);
                            }

                            @Override
                            public ResourceRef visitSeries(ContentType contentType) {
                                return new SeriesRef(id, source);
                            }
                        });
                    } else {
                        return new TopicRef(id, source);
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
                public void process(ContentEquivalenceAssertionMessage equivalenceAssertionMessage) {
                    
                }
            });
            return (M) message.get();
        } catch (Exception e) {
            throw new MessageException(e);
        }
    }

}
