package org.atlasapi.messaging;


public class MessageLogger<M extends Message> extends BaseWorker<M> {

    private final MessageStore<M> store;

    public MessageLogger(MessageStore<M> store) {
        this.store = store;
    }

    @Override
    public void process(M message) {
        store.add(message);
    }
}