package org.atlasapi.users.videosource;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.users.videosource.model.VideoSourceOAuthProvider;

import com.google.common.collect.FluentIterable;

public class VideoSourceOAuthProvidersQueryResultWriter implements QueryResultWriter<VideoSourceOAuthProvider> {
    private final EntityListWriter<VideoSourceOAuthProvider> linkedServiceProvidersWriter;
    
    public VideoSourceOAuthProvidersQueryResultWriter(EntityListWriter<VideoSourceOAuthProvider> linkedServiceProvidersWriter) {
        this.linkedServiceProvidersWriter = linkedServiceProvidersWriter;
    }
    
    @Override
    public void write(QueryResult<VideoSourceOAuthProvider> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }
    
    private void writeResult(QueryResult<VideoSourceOAuthProvider> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<VideoSourceOAuthProvider> resources = result.getResources();
            writer.writeList(linkedServiceProvidersWriter, resources, ctxt);
        } else {
            writer.writeObject(linkedServiceProvidersWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
