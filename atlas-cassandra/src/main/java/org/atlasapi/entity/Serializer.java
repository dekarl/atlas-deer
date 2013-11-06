package org.atlasapi.entity;

public interface Serializer<F, T> {

    T serialize(F src);

    F deserialize(T dest);

}