package org.atlasapi.query.common;

public interface QueryExecutor<T> {

    QueryResult<T> execute(Query<T> query) throws QueryExecutionException;
    
}
