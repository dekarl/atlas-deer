package org.atlasapi.query.annotation;

import org.atlasapi.query.common.InvalidAnnotationException;

import com.google.common.collect.ImmutableSetMultimap;

final class CombinedResourceAnnotationIndex implements
        ContextualAnnotationIndex {

    private final Index index;

    public CombinedResourceAnnotationIndex(ImmutableSetMultimap<String, PathAnnotation> bindings) {
        this.index = new Index(bindings);
    }

    @Override
    public ActiveAnnotations resolve(Iterable<String> keys)
            throws InvalidAnnotationException {
        return index.resolve(keys);
    }
}