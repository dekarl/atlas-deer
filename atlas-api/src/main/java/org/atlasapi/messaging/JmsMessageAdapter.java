package org.atlasapi.messaging;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;

class JmsMessageAdapter {
    
    private final MessageSerializer serializer;
    private final Worker worker;

    public JmsMessageAdapter(MessageSerializer serializer, Worker worker) {
        this.serializer = serializer;
        this.worker = worker;
    }
    
    public void onMessage(byte[] message) {
        try {
            Message event = serializer.deserialize(ByteSource.wrap(message));
            event.dispatchTo(worker);
        } catch (MessageException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
    
    public void onMessage(String message) {
        try {
            Message event = serializer.deserialize(ByteSource.wrap(message.getBytes(Charsets.UTF_8)));
            event.dispatchTo(worker);
        } catch (MessageException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
    
}