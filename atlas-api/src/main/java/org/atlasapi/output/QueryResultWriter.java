package org.atlasapi.output;

import java.io.IOException;

import org.atlasapi.query.common.QueryResult;

/**
 * <p>Writes out the result of a query, a {@link QueryResult} via the provided
 * {@link ResponseWriter}</p>
 * 
 * @param <T>
 */
public interface QueryResultWriter<T> {

    void write(QueryResult<T> result, ResponseWriter responseWriter) throws IOException;

}