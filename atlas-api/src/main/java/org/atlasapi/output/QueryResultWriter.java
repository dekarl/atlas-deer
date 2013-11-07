package org.atlasapi.output;

import java.io.IOException;

import org.atlasapi.query.common.QueryResult;

public interface QueryResultWriter<T> {

    void write(QueryResult<T> result, ResponseWriter responseWriter) throws IOException;

}