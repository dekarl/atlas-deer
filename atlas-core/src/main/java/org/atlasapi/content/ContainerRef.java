package org.atlasapi.content;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

public abstract class ContainerRef extends ContentRef {

    public ContainerRef(Id id, Publisher source) {
        super(id, source);
    }

}
