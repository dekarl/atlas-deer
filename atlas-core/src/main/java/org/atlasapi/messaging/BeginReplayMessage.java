package org.atlasapi.messaging;


/**
 * Message signaling the beginning of a messages replay.
 */
public class BeginReplayMessage extends AbstractMessage {

    public BeginReplayMessage(String messageId, Long timestamp, String entityId, String entityType, String entitySource) {
        super(messageId, timestamp, entityId, entityType, entitySource);
    }

    @Override
    public void dispatchTo(Worker worker) {
        worker.process(this);
    }
}
