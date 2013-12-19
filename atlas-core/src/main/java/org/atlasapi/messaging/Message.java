package org.atlasapi.messaging;

import com.metabroadcast.common.time.Timestamp;


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
    Timestamp getTimestamp();
    
}
