package org.atlasapi.output;

import java.io.IOException;

import javax.annotation.Nullable;


public interface FieldWriter {

    void writeField(String field, @Nullable Object obj) throws IOException;

    <T> void writeObject(EntityWriter<? super T> writer, @Nullable T obj, OutputContext ctxt)
        throws IOException;

    void writeList(String field, String elem, Iterable<?> list, OutputContext ctxt)
        throws IOException;

    <T> void writeList(EntityListWriter<? super T> listWriter,
                       Iterable<T> list, OutputContext ctxt) throws IOException;

}