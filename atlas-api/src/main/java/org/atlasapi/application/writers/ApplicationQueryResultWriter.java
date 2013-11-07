package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.Application;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.output.useraware.UserAwareQueryResultWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;

import com.google.common.collect.FluentIterable;

public class ApplicationQueryResultWriter implements UserAwareQueryResultWriter<Application> {

    private final EntityListWriter<Application> applicationListWriter;

    public ApplicationQueryResultWriter(EntityListWriter<Application> applicationListWriter) {
        this.applicationListWriter = applicationListWriter;
    }

    @Override
    public void write(UserAwareQueryResult<Application> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }

    private void writeResult(UserAwareQueryResult<Application> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<Application> resources = result.getResources();
            writer.writeList(applicationListWriter, resources, ctxt);
        } else {
            writer.writeObject(applicationListWriter, result.getOnlyResource(), ctxt);
        }

    }

    private OutputContext outputContext(UserAwareQueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }
}
