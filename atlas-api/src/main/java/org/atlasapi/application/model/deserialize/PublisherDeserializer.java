package org.atlasapi.application.model.deserialize;

import java.lang.reflect.Type;

import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Optional;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;


public class PublisherDeserializer implements JsonDeserializer<Publisher> {

    @Override
    public Publisher deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        String key;
        if (json.isJsonObject()) {
            key = json.getAsJsonObject().get("key").getAsJsonPrimitive().getAsString();
        } else {
            key = json.getAsJsonPrimitive().getAsString();
        }
        Optional<Publisher> publisher = Optional.fromNullable(Publisher.fromKey(key).valueOrNull());
        return publisher.get();
    }
}
