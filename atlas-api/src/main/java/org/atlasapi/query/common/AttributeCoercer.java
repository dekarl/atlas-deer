package org.atlasapi.query.common;

import java.util.List;

public interface AttributeCoercer<I, O> {

    public List<O> apply(Iterable<I> values) throws InvalidAttributeValueException;

}
