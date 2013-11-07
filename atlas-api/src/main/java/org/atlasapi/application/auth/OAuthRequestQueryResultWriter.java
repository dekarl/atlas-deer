package org.atlasapi.application.auth;

import java.io.IOException;

import org.atlasapi.application.model.auth.OAuthRequest;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import com.google.common.collect.FluentIterable;


public class OAuthRequestQueryResultWriter implements QueryResultWriter<OAuthRequest> {
    private final EntityListWriter<OAuthRequest> oauthRequestWriter;
    
    public OAuthRequestQueryResultWriter(EntityListWriter<OAuthRequest> oauthRequestWriter) {
        this.oauthRequestWriter = oauthRequestWriter;
    }
    
    @Override
    public void write(QueryResult<OAuthRequest> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }
    
    private void writeResult(QueryResult<OAuthRequest> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<OAuthRequest> resources = result.getResources();
            writer.writeList(oauthRequestWriter, resources, ctxt);
        } else {
            writer.writeObject(oauthRequestWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}
