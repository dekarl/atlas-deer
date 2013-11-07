package org.atlasapi.output.writers;

import java.io.IOException;

import org.atlasapi.media.entity.RelatedLink;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public final class RelatedLinkWriter implements EntityListWriter<RelatedLink> {

    @Override
    public void write(RelatedLink rl, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("url", rl.getUrl());
        writer.writeField("type", rl.getType().toString().toLowerCase());
        writer.writeField("source_id", rl.getSourceId());
        writer.writeField("short_name", rl.getShortName());
        writer.writeField("title", rl.getTitle());
        writer.writeField("description", rl.getDescription());
        writer.writeField("image", rl.getImage());
        writer.writeField("thumbnail", rl.getThumbnail());
    }

    @Override
    public String listName() {
        return "related_links";
    }

    @Override
    public String fieldName(RelatedLink entity) {
        return "related_link";
    }
}