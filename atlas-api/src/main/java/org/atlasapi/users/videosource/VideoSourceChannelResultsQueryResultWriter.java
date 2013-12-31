package org.atlasapi.users.videosource;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.users.videosource.model.VideoSourceChannelResults;

import com.google.common.collect.FluentIterable;


public class VideoSourceChannelResultsQueryResultWriter implements
        QueryResultWriter<VideoSourceChannelResults> {
 
    private final EntityListWriter<VideoSourceChannelResults> resultsWriter;
    
    public VideoSourceChannelResultsQueryResultWriter(
            EntityListWriter<VideoSourceChannelResults> resultsWriter) {
        super();
        this.resultsWriter = resultsWriter;
    }

    @Override
    public void write(QueryResult<VideoSourceChannelResults> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }
    
    private void writeResult(QueryResult<VideoSourceChannelResults> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<VideoSourceChannelResults> resources = result.getResources();
            writer.writeList(resultsWriter, resources, ctxt);
        } else {
            writer.writeObject(resultsWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
