package org.atlasapi.messaging;

import com.metabroadcast.common.time.Timestamp;


/**
 * Message signaling the end of a messages replay.
 */
public class EndReplayMessage extends AbstractMessage {

    public EndReplayMessage(String messageId, Timestamp timestamp) {
        super(messageId, timestamp);
    }

}
