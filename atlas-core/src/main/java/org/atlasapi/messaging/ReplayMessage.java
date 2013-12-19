package org.atlasapi.messaging;

import com.metabroadcast.common.time.Timestamp;


/**
 * Message signaling the replay of a given source message.
 */
public class ReplayMessage<O extends Message> extends AbstractMessage {

    private final O original;
    
    public ReplayMessage(String messageId, Timestamp timestamp, O original) {
        super(messageId, timestamp);
        this.original = original;
    }

    public O getOriginal() {
        return original;
    }

}
