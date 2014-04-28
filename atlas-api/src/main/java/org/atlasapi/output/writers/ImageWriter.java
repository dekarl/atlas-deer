package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.content.Image;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;


public class ImageWriter implements EntityListWriter<Image> {

    @Override
    public void write(Image entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("uri", entity.getCanonicalUri());
        writer.writeField("mime_type", entity.getMimeType());
        writer.writeField("type", entity.getType().getName());
        writer.writeField("color", entity.getColor().getName());
        writer.writeField("theme", entity.getTheme().getName());
        writer.writeField("aspect_ratio", entity.getAspectRatio().getName());
        writer.writeField("availability_start", entity.getAvailabilityStart());
        writer.writeField("availability_end", entity.getAvailabilityEnd());
        writer.writeField("width", entity.getWidth());
        writer.writeField("height", entity.getHeight());
    }

    @Override
    public String fieldName(Image entity) {
        return "image";
    }

    @Override
    public String listName() {
        return "images";
    }

}
