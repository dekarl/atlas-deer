package org.atlasapi.users.videosource;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.users.videosource.model.VideoSourceOAuthProvider;

public class VideoSourceOauthProvidersListWriter implements
        EntityListWriter<VideoSourceOAuthProvider> {

    @Override
    public void write(VideoSourceOAuthProvider entity, FieldWriter writer,
            OutputContext ctxt) throws IOException {
        writer.writeField("namespace", entity.getNamespace().name().toLowerCase());
        writer.writeField("prompt", entity.getLoginPromptMessage());
        writer.writeField("authRequestUrl", entity.getAuthRequestUrl());
        writer.writeField("image", entity.getImage());
    }

    @Override
    public String fieldName(VideoSourceOAuthProvider entity) {
        return "link_providers";
    }

    @Override
    public String listName() {
        return "link_providers";
    }

}
