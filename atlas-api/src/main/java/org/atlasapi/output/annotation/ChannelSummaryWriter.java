package org.atlasapi.output.annotation;

import java.io.IOException;
import java.math.BigInteger;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.NumberToShortStringCodec;

public class ChannelSummaryWriter extends OutputAnnotation<Channel> {

    private final NumberToShortStringCodec codec;

    public ChannelSummaryWriter(NumberToShortStringCodec codec) {
        super();
        this.codec = codec;
    }

    @Override
    public void write(Channel entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeField("id", codec.encode(BigInteger.valueOf(entity.getId())));
        writer.writeField("type", entity.getClass().getSimpleName().toLowerCase());
        writer.writeField("uri", entity.getCanonicalUri());
        writer.writeList("aliases", "alias", entity.getAliasUrls(), ctxt);
        writer.writeField("title", entity.getTitle());
        writer.writeField("image", null);
    }

}
