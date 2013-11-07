package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthProvider;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;


public class AuthProvidersQueryResultWriter implements QueryResultWriter<OAuthProvider> {
    private final EntityListWriter<OAuthProvider> authProvidersWriter;
    
    public AuthProvidersQueryResultWriter(EntityListWriter<OAuthProvider> authProvidersWriter) {
        this.authProvidersWriter = authProvidersWriter;
    }
    
    @Override
    public void write(QueryResult<OAuthProvider> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }
    
    private void writeResult(QueryResult<OAuthProvider> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<OAuthProvider> resources = result.getResources();
            writer.writeList(authProvidersWriter, resources, ctxt);
        } else {
            writer.writeObject(authProvidersWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
