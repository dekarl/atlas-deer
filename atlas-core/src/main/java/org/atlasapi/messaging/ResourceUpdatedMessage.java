package org.atlasapi.messaging;

import org.atlasapi.entity.ResourceRef;

import com.metabroadcast.common.time.Timestamp;


/**
 * Message signaling that a given entity has been created or updated.
 */
public class ResourceUpdatedMessage extends AbstractMessage {

    private ResourceRef updatedResource;

    public ResourceUpdatedMessage(String messageId, Timestamp timestamp, ResourceRef updatedResource) {
        super(messageId, timestamp);
        this.updatedResource = updatedResource;
    }
    
    public ResourceRef getUpdatedResource() {
        return updatedResource;
    }
    
}
