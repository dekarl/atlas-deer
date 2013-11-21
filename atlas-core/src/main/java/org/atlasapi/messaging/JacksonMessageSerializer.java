package org.atlasapi.messaging;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.io.ByteSource;


public class JacksonMessageSerializer implements MessageSerializer {
    
    private ObjectMapper mapper;

    public class MessagingModule extends SimpleModule {

        public MessagingModule() {
            super("Messaging Module", new com.fasterxml.jackson.core.Version(0, 0, 1, null, null, null));
        }

        @Override
        public void setupModule(Module.SetupContext context) {
            super.setupModule(context);
            context.setMixInAnnotations(EntityUpdatedMessage.class, AbstractMessageConfiguration.class);
            context.setMixInAnnotations(BeginReplayMessage.class, AbstractMessageConfiguration.class);
            context.setMixInAnnotations(ReplayMessage.class, ReplayMessageConfiguration.class);
            context.setMixInAnnotations(EndReplayMessage.class, AbstractMessageConfiguration.class);
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
    public Message deserialize(ByteSource bytes) throws MessageException {
        try {
            return mapper.readValue(bytes.openStream(), Message.class);
        } catch (IOException e) {
            throw new MessageException(e);
        }
    }
    
}
