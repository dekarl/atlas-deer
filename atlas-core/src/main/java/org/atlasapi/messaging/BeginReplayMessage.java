package org.atlasapi.messaging;

import com.metabroadcast.common.time.Timestamp;


/**
 * Message signaling the beginning of a messages replay.
 */
public class BeginReplayMessage extends AbstractMessage {

    public BeginReplayMessage(String messageId, Timestamp timestamp) {
        super(messageId, timestamp);
    }

}
