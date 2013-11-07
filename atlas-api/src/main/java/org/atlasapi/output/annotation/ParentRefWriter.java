package org.atlasapi.output.annotation;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.math.BigInteger;

import javax.annotation.Nonnull;

import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.output.EntityWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public final class ParentRefWriter implements EntityWriter<ParentRef> {
    
    private NumberToShortStringCodec idCodec;
    private final String fieldName;

    public ParentRefWriter(String fieldName, NumberToShortStringCodec idCodec) {
        this.idCodec = checkNotNull(idCodec);
        this.fieldName = checkNotNull(fieldName);
    }

    @Override
    public void write(@Nonnull ParentRef entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        BigInteger id = entity.getId().toBigInteger();
        writer.writeField("id", idCodec.encode(id));
    }

    @Override
    public String fieldName(ParentRef entity) {
        return fieldName;
    }
}