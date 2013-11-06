package org.atlasapi.criteria.operator;

import org.atlasapi.criteria.operator.Operators.Equals;

public interface EqualsOperatorVisitor<V> {

    V visit(Equals equals);

}
