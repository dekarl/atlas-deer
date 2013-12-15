package org.atlasapi.messaging;


/**
 * Base {@link org.atlasapi.messaging.messaging.worker.Worker} class providing
 * default implementations of message processing methods.
 */
public abstract class BaseWorker implements Worker {

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
