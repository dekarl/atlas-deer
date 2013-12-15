package org.atlasapi.messaging;

import java.io.IOException;

public interface MessageSender {

    void sendMessage(Message message) throws IOException;

}