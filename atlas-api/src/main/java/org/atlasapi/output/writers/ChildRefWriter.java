package org.atlasapi.output.writers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public final class ChildRefWriter implements EntityListWriter<ChildRef> {

    private final String listName;
    private final NumberToShortStringCodec idCodec;

    public ChildRefWriter(NumberToShortStringCodec idCodec, String listName) {
        this.idCodec = checkNotNull(idCodec);
        this.listName = checkNotNull(listName);
    }

    @Override
    public void write(ChildRef entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeField("type", entity.getType());
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(ChildRef entity) {
        return "content";
    }
}