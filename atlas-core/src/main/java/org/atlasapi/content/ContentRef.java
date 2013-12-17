package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiable;
import org.atlasapi.entity.Sourced;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

public abstract class ContentRef implements Identifiable, Sourced {

    protected final Id id;
    protected final Publisher source;

    public ContentRef(Id id, Publisher source) {
        this.id = checkNotNull(id);
        this.source = checkNotNull(source);
    }

    public Id getId() {
        return id;
    }

    public Publisher getPublisher() {
        return source;
    }
    
    public abstract ContentType getContentType();
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public final String toString() {
        return toStringHelper().toString();
    }

    protected ToStringHelper toStringHelper() {
        return Objects.toStringHelper(getClass())
            .omitNullValues()
            .add("id", id)
            .add("source", source)
            .add("type", getContentType());
    }
    
}
