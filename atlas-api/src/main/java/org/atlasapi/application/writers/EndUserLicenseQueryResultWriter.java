package org.atlasapi.application.writers;

import java.io.IOException;

import org.atlasapi.application.EndUserLicense;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.OutputContext;
import org.atlasapi.output.QueryResultWriter;
import org.atlasapi.output.ResponseWriter;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;

import com.google.common.collect.FluentIterable;


public class EndUserLicenseQueryResultWriter implements QueryResultWriter<EndUserLicense> {
    private final EntityListWriter<EndUserLicense> endUserLicenseListWriter;
    
    public EndUserLicenseQueryResultWriter(EntityListWriter<EndUserLicense> endUserLicenseListWriter) {
        super();
        this.endUserLicenseListWriter = endUserLicenseListWriter;
    }

    @Override
    public void write(QueryResult<EndUserLicense> result, ResponseWriter responseWriter)
            throws IOException {
        responseWriter.startResponse();
        writeResult(result, responseWriter);
        responseWriter.finishResponse();
    }
    
    private void writeResult(QueryResult<EndUserLicense> result, ResponseWriter writer)
            throws IOException {
        OutputContext ctxt = outputContext(result.getContext());

        if (result.isListResult()) {
            FluentIterable<EndUserLicense> resources = result.getResources();
            writer.writeList(endUserLicenseListWriter, resources, ctxt);
        } else {
            writer.writeObject(endUserLicenseListWriter, result.getOnlyResource(), ctxt);
        }
    }
    
    private OutputContext outputContext(QueryContext queryContext) {
        return OutputContext.valueOf(queryContext);
    }

}
