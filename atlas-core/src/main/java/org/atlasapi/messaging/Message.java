package org.atlasapi.messaging;


/**
 * Base interface for messages to be dispatched to {@link org.atlasapi.messaging.messaging.worker.Worker}s.
 */
public interface Message {
    
    /**
     * Get a unique id for this message.
     */
    String getMessageId();
    
    /**
     * Get the timestamp when the message happened
     */
    Long getTimestamp();
    
    /**
     * Get the id of the entity this message refers to.
     */
    String getEntityId();
    
    /**
     * Get the type of the entity this message refers to.
     */
    String getEntityType();
    
    /**
     * Get the source identifier of the entity to which this message refers.
     */
    String getEntitySource();
    
    /**
     * 
     */
    boolean canCoalesce();
    
    /**
     * Dispatch this message to the given {@link org.atlasapi.messaging.messaging.worker.Worker}.
     */
    void dispatchTo(Worker worker);
}
