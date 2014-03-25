package org.atlasapi.messaging;

import java.io.IOException;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.ClipRef;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.FilmRef;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.SongRef;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.schedule.ScheduleRef;
import org.atlasapi.schedule.ScheduleUpdate;
import org.atlasapi.schedule.ScheduleUpdateMessage;
import org.atlasapi.topic.TopicRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.io.ByteSource;
import com.metabroadcast.common.time.Timestamp;

//TODO should be able to register additional modules at creation time so as to decentralize configuration classes.
public class JacksonMessageSerializer implements MessageSerializer {
    
    private ObjectMapper mapper;

    public class MessagingModule extends SimpleModule {

        public MessagingModule() {
            super("Messaging Module", new com.fasterxml.jackson.core.Version(0, 0, 1, null, null, null));
        }

        @Override
        public void setupModule(Module.SetupContext context) {
            super.setupModule(context);
            context.setMixInAnnotations(ResourceUpdatedMessage.class, ResourceUpdatedMessageConfiguration.class);
            context.setMixInAnnotations(BeginReplayMessage.class, BasicMessageConfiguration.class);
            context.setMixInAnnotations(EndReplayMessage.class, BasicMessageConfiguration.class);
            context.setMixInAnnotations(ReplayMessage.class, ReplayMessageConfiguration.class);
            context.setMixInAnnotations(EquivalenceGraphUpdateMessage.class, EquivalenceGraphUpdateMessageConfiguration.class);
            context.setMixInAnnotations(EquivalenceGraph.class, EquivalenceGraphConfiguration.class);
            context.setMixInAnnotations(EquivalenceGraph.Adjacents.class, AdjacentsConfiguration.class);
            context.setMixInAnnotations(EquivalenceGraphUpdate.class, EquivalenceGraphUpdateConfiguration.class);
            context.setMixInAnnotations(EquivalenceGraphUpdate.Builder.class, EquivalenceGraphUpdateConfiguration.Builder.class);
            context.setMixInAnnotations(ScheduleUpdateMessage.class, ScheduleUpdateMessageConfiguration.class);
            context.setMixInAnnotations(ScheduleUpdate.class, ScheduleUpdateConfiguration.class);
            context.setMixInAnnotations(ScheduleUpdate.Builder.class, ScheduleUpdateConfiguration.Builder.class);
        }
        
    }
    
    public class AtlasModelModule extends SimpleModule {
        
        public AtlasModelModule() {
            super("Model Module", new com.fasterxml.jackson.core.Version(0, 0, 1, null, null, null));
        }
        
        @Override
        public void setupModule(Module.SetupContext context) {
            super.setupModule(context);
            context.setMixInAnnotations(Id.class, IdConfiguration.class);
            context.setMixInAnnotations(BrandRef.class, ResourceRefConfiguration.class);
            context.setMixInAnnotations(TopicRef.class, ResourceRefConfiguration.class);
            context.setMixInAnnotations(ItemRef.class, ItemRefConfiguration.class);
            context.setMixInAnnotations(EpisodeRef.class, ItemRefConfiguration.class);
            context.setMixInAnnotations(SongRef.class, ItemRefConfiguration.class);
            context.setMixInAnnotations(FilmRef.class, ItemRefConfiguration.class);
            context.setMixInAnnotations(ClipRef.class, ItemRefConfiguration.class);
            context.setMixInAnnotations(SeriesRef.class, SeriesRefConfiguration.class);
            context.setMixInAnnotations(BroadcastRef.class, BroadcastRefConfiguration.class);
            context.setMixInAnnotations(ScheduleRef.class, ScheduleRefConfiguration.class);
            context.setMixInAnnotations(ScheduleRef.Entry.class, ScheduleRefConfiguration.Entry.class);
            context.setMixInAnnotations(Timestamp.class, TimestampConfiguration.class);
            SimpleDeserializers desers = new SimpleDeserializers();
            context.addDeserializers(desers);
        }
    }

    private static class GenericModule extends SimpleModule {

        public GenericModule() {
            super("Generic Module", new com.fasterxml.jackson.core.Version(0, 0, 1, null, null, null));
        }

        @Override
        public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.setMixInAnnotations(Object.class, ObjectConfiguration.class);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    private static interface ObjectConfiguration {
    }

    public JacksonMessageSerializer() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, SerializationFeature.WRITE_NULL_MAP_VALUES);
        mapper.registerModule(new GenericModule());
        mapper.registerModule(new JodaModule());
        mapper.registerModule(new GuavaModule());
        mapper.registerModule(new AtlasModelModule());
        mapper.registerModule(new MessagingModule());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setVisibility(PropertyAccessor.CREATOR, Visibility.ANY);
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        mapper.setVisibility(PropertyAccessor.GETTER, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.IS_GETTER, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.SETTER, Visibility.NONE);
        this.mapper = mapper;
    }
    
    @Override
    public ByteSource serialize(Message msg) throws MessageException {
        try {
            return ByteSource.wrap(mapper.writeValueAsBytes(msg));
        } catch (IOException e) {
            throw new MessageException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> M deserialize(ByteSource bytes) throws MessageException {
        try {
            return (M) mapper.readValue(bytes.openStream(), Message.class);
        } catch (IOException e) {
            throw new MessageException(e);
        }
    }
    
}
