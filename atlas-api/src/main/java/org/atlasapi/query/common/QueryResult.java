package org.atlasapi.query.common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.FluentIterable;

public abstract class QueryResult<T> {

    public static final <T> SingleQueryResult<T> singleResult(T resource, QueryContext context) {
        return new SingleQueryResult<T>(resource, context);
    }

    public static final <T> ListQueryResult<T> listResult(Iterable<T> resource, QueryContext context) {
        return new ListQueryResult<T>(resource, context);
    }
    
    private final QueryContext context;

    protected QueryResult(QueryContext context) {
        this.context = checkNotNull(context);
    }

    public QueryContext getContext() {
        return context;
    }

    public abstract boolean isListResult();

    public abstract FluentIterable<T> getResources();

    public abstract T getOnlyResource();

    public static final class SingleQueryResult<T> extends QueryResult<T> {

        private final T resource;

        public SingleQueryResult(T resource, QueryContext context) {
            super(context);
            this.resource = checkNotNull(resource);
        }

        @Override
        public boolean isListResult() {
            return false;
        }

        @Override
        public FluentIterable<T> getResources() {
            throw new IllegalStateException(
                "QueryResult.getResources() cannot be called on single result");
        }

        public T getOnlyResource() {
            return resource;
        }

    }

    public static final class ListQueryResult<T> extends QueryResult<T> {

        private final FluentIterable<T> resources;

        public ListQueryResult(Iterable<T> resources, QueryContext context) {
            super(context);
            this.resources = FluentIterable.from(resources);
        }
        
        @Override
        public boolean isListResult() {
            return true;
        }

        public FluentIterable<T> getResources() {
            return resources;
        }

        public T getOnlyResource() {
            throw new IllegalStateException(
                "QueryResult.getOnlyResource() cannot be called on single result");
        }

    }

}
