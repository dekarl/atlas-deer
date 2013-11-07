package org.atlasapi.query.common;

import java.util.List;

import com.google.common.collect.ImmutableList;

public abstract class AbstractAttributeCoercer<I, O> implements AttributeCoercer<I, O> {

    @Override
    public final List<O> apply(Iterable<I> input) {
        ImmutableList.Builder<O> values = ImmutableList.builder();
        for (I i : input) {
            values.add(coerce(i));
        }
        return values.build();
    }

    protected abstract O coerce(I input);

}
