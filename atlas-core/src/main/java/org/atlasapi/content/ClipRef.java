package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

public class ClipRef extends ItemRef {

    public ClipRef(Id id, Publisher source, String sortKey, DateTime updated) {
        super(id, source, sortKey, updated);
    }

    @Override
    public ContentType getContentType() {
        return ContentType.CLIP;
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ClipRef) {
            ClipRef other = (ClipRef) that;
            return id.equals(other.id)
                && source.equals(other.source)
                && getContentType().equals(other.getContentType());
        }
        return false;
    }

    
}
