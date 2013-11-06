package org.atlasapi.criteria.operator;

import org.atlasapi.criteria.operator.Operators.After;
import org.atlasapi.criteria.operator.Operators.Before;

public interface DateTimeOperatorVisitor<V> extends EqualsOperatorVisitor<V> {

    V visit(Before before);

    V visit(After after);

}
