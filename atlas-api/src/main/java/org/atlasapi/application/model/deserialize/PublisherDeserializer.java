package org.atlasapi.application.model.deserialize;

import java.lang.reflect.Type;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.source.Sources;

import com.google.common.base.Optional;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class PublisherDeserializer implements JsonDeserializer<Publisher>,
        JsonSerializer<Publisher> {

    @Override
    public Publisher deserialize(JsonElement json, Type typeOfT,
            JsonDeserializationContext context) throws JsonParseException {
        String key;
        if (json.isJsonObject()) {
            key = json.getAsJsonObject().get("key").getAsJsonPrimitive().getAsString();
        } else {
            key = json.getAsJsonPrimitive().getAsString();
        }
        Optional<Publisher> publisher = Sources.fromPossibleKey(key);
        return publisher.get();
    }

    @Override
    public JsonElement serialize(Publisher src, Type typeOfSrc,
            JsonSerializationContext context) {
        JsonObject publisher = new JsonObject();
        publisher.addProperty("key", src.key());
        publisher.addProperty("country", src.country().code());
        publisher.addProperty("title", src.title());
        return publisher;
    }
}
