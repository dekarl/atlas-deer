package org.atlasapi.output.writers;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.content.ItemRef;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public final class ItemRefWriter implements EntityListWriter<ItemRef> {

    private final String listName;
    private final NumberToShortStringCodec idCodec;

    public ItemRefWriter(NumberToShortStringCodec idCodec, String listName) {
        this.idCodec = checkNotNull(idCodec);
        this.listName = checkNotNull(listName);
    }

    @Override
    public void write(ItemRef entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("id", idCodec.encode(entity.getId().toBigInteger()));
        writer.writeField("type", entity.getContentType());
    }

    @Override
    public String listName() {
        return listName;
    }

    @Override
    public String fieldName(ItemRef entity) {
        return "content";
    }
}