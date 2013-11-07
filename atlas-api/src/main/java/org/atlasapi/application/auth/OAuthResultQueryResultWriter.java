package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthResult;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;


public class OAuthResultQueryResultWriter implements QueryResultWriter<OAuthResult> {
    private final EntityListWriter<OAuthResult> oauthResultWriter;
    
    public OAuthResultQueryResultWriter(EntityListWriter<OAuthResult> oauthResultWriter) {
        this.oauthResultWriter = oauthResultWriter;
    }
    
    @Override
    public void write(QueryResult<OAuthResult> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }
    
    private void writeResult(QueryResult<OAuthResult> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<OAuthResult> resources = result.getResources();
            writer.writeList(oauthResultWriter, resources, ctxt);
        } else {
            writer.writeObject(oauthResultWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
