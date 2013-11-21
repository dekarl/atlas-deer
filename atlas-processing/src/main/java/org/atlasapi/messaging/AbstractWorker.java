package org.atlasapi.messaging;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;

/**
 * Base {@link org.atlasapi.messaging.messaging.worker.Worker} class providing
 * {@link org.atlasapi.persistence.messaging.Message} unmarshaling and
 * dispatching.
 */
public abstract class AbstractWorker implements Worker {

    private final MessageSerializer serializer;

    public AbstractWorker(MessageSerializer serializer) {
        this.serializer = serializer;
    }
    
    public void onMessage(String message) {
        try {
            Message event = serializer.deserialize(ByteSource.wrap(message.getBytes(Charsets.UTF_8)));
            event.dispatchTo(this);
        } catch (MessageException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    @Override
    public void process(EntityUpdatedMessage message) {
    }

    @Override
    public void process(BeginReplayMessage message) {
    }

    @Override
    public void process(EndReplayMessage message) {
    }

    @Override
    public void process(ReplayMessage message) {
    }
}
