package org.atlasapi.messaging;


/**
 * Base {@link org.atlasapi.messaging.messaging.worker.Worker} class providing
 * default implementations of message processing methods.
 */
public abstract class BaseWorker<M extends Message> implements Worker<M> {

    @Override
    public Class<?> getType() {
        return getClass();
    }
    
}
