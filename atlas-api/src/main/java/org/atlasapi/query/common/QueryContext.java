package org.atlasapi.query.common;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.query.annotation.ActiveAnnotations;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.metabroadcast.common.query.Selection;

public class QueryContext {

    private static final QueryContext STANDARD = new QueryContext(
            ApplicationSources.defaults(), 
            ActiveAnnotations.standard()    
    );
    
    public static final QueryContext standard() {
        return STANDARD;
    }
    
    private final ApplicationSources appSources;
    private final ActiveAnnotations annotations;
    private final Optional<Selection> selection;

    public QueryContext(ApplicationSources appSources, ActiveAnnotations annotations) {
        this(appSources, annotations, null);
    }
    
    public QueryContext(ApplicationSources appSources, ActiveAnnotations annotations,
        Selection selection) {
        this.appSources = checkNotNull(appSources);
        this.annotations = checkNotNull(annotations);
        this.selection = Optional.fromNullable(selection);
    }

    public ApplicationSources getApplicationSources() {
        return this.appSources;
    }

    public ActiveAnnotations getAnnotations() {
        return this.annotations;
    }
    
    public Optional<Selection> getSelection() {
        return this.selection;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof QueryContext) {
            QueryContext other = (QueryContext) that;
            return appSources.equals(other.appSources)
                && annotations.equals(other.annotations)
                && selection.equals(other.selection);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return appSources.hashCode() ^ annotations.hashCode() ^ selection.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("config", appSources)
            .add("annotations", annotations)
            .add("selection", selection)
            .toString();
    }
    
}
