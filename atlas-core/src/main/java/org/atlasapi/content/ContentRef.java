package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.ResourceType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects.ToStringHelper;

public abstract class ContentRef extends ResourceRef {

    public ContentRef(Id id, Publisher source) {
        super(id, source);
    }

    public abstract ContentType getContentType();
    
    @Override
    public final ResourceType getResourceType() {
        return ResourceType.CONTENT;
    }

    protected ToStringHelper toStringHelper() {
        return super.toStringHelper().add("type", getContentType());
    }
    
}
