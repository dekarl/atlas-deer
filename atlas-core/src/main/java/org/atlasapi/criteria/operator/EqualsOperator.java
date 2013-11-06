package org.atlasapi.criteria.operator;

public interface EqualsOperator extends Operator {

    <V> V accept(EqualsOperatorVisitor<V> v);

}
