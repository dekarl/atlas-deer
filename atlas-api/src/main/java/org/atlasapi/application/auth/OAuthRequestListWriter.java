package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class OAuthRequestListWriter implements EntityListWriter<OAuthRequest> {

    @Override
    public void write(OAuthRequest entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("login_url", entity.getAuthUrl().toExternalForm());
        writer.writeField("token", entity.getToken());
    }

    @Override
    public String fieldName(OAuthRequest entity) {
        return "oauth_request";
    }

    @Override
    public String listName() {
        return "oauth_requests";
    }

}
