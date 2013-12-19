package org.atlasapi.messaging;


/**
 * Base interface for workers that process {@link org.atlasapi.persistence.messaging.Message Message}s.
 */
public interface Worker<M extends Message> {
    
    /**
     * Process a {@link org.atlasapi.persistence.messaging.Message Message}.
     */
    void process(M message);

}
