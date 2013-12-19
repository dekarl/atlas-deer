package org.atlasapi.messaging;

import com.metabroadcast.common.time.Timestamp;

public abstract class AbstractMessage implements Message {

    private final String messageId;
    private final Timestamp timestamp;
    
    public AbstractMessage(String messageId, Timestamp timestamp) {
        this.messageId = messageId;
        this.timestamp = timestamp;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public Timestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public final boolean equals(Object that) {
        if (that instanceof Message) {
            Message other = (Message) that;
            return this.messageId.equals(other.getMessageId());
        } else {
            return false;
        }
    }

    @Override
    public final int hashCode() {
        return this.messageId.hashCode();
    }

    @Override
    public String toString() {
        return messageId;
    }
    
}
