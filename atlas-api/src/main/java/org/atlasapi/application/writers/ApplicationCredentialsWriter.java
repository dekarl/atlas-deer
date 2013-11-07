package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class ApplicationCredentialsWriter implements EntityListWriter<ApplicationCredentials> {

    @Override
    public void write(ApplicationCredentials entity, FieldWriter writer, OutputContext ctxt)
            throws IOException {
        writer.writeField("apiKey", entity.getApiKey());
    }

    @Override
    public String fieldName(ApplicationCredentials entity) {
        return "credentials";
    }

    @Override
    public String listName() {
        return "credentials";
    }

}
