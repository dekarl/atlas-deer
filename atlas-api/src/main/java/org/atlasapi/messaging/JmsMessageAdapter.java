package org.atlasapi.messaging;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;

class JmsMessageAdapter<M extends Message> {
    
    private final MessageSerializer serializer;
    private final Worker<? super M> worker;

    public JmsMessageAdapter(MessageSerializer serializer, Worker<? super M> worker) {
        this.serializer = serializer;
        this.worker = worker;
    }
    
    public void onMessage(byte[] message) {
        try {
            M msg = serializer.deserialize(ByteSource.wrap(message));
            worker.process(msg);
        } catch (MessageException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
    
    public void onMessage(String message) {
        try {
            M msg = serializer.deserialize(ByteSource.wrap(message.getBytes(Charsets.UTF_8)));
            worker.process(msg);
        } catch (MessageException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
    
}