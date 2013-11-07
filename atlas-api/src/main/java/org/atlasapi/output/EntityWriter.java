package org.atlasapi.output;

import java.io.IOException;

import javax.annotation.Nonnull;


public interface EntityWriter<T> {

    void write(@Nonnull T entity, @Nonnull FieldWriter writer, @Nonnull OutputContext ctxt) throws IOException;
    
    @Nonnull String fieldName(T entity);
    
}
