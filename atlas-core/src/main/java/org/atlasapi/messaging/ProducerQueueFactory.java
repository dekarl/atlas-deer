package org.atlasapi.messaging;

public interface ProducerQueueFactory {

    MessageSender makeMessageSender(String destinationName);

}