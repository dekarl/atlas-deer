package org.atlasapi.query.v4.topic;

import java.io.IOException;

import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.topic.Topic;

import com.google.common.collect.FluentIterable;

public class TopicQueryResultWriter implements QueryResultWriter<Topic> {

    private final EntityListWriter<Topic> topicListWriter;
    
    public TopicQueryResultWriter(EntityListWriter<Topic> topicListWriter) {
        this.topicListWriter = topicListWriter;
    }

    @Override
    public void write(QueryResult<Topic> result, ResponseWriter writer) throws IOException {
        writer.startResponse();
        writeResult(result, writer);
        writer.finishResponse();
    }

    private void writeResult(QueryResult<Topic> result, ResponseWriter writer)
        throws IOException {

        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Topic> topics = result.getResources();
            writer.writeList(topicListWriter, topics, ctxt);
        } else {
            writer.writeObject(topicListWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
    
}
