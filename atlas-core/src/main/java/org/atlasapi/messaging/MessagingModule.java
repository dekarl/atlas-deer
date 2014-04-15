package org.atlasapi.messaging;

import com.metabroadcast.common.queue.MessageConsumerFactory;
import com.metabroadcast.common.queue.MessageSenderFactory;

public interface MessagingModule {

    MessageSenderFactory messageSenderFactory();

    MessageConsumerFactory<?> messageConsumerFactory();

}