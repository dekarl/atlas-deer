package org.atlasapi.output;

import javax.annotation.Nonnull;

public interface EntityListWriter<T> extends EntityWriter<T> {

    @Nonnull String listName();
    
}
