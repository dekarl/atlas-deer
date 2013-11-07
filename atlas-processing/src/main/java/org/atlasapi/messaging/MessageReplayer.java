package org.atlasapi.messaging;

import java.io.IOException;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Session;

import org.atlasapi.messaging.BeginReplayMessage;
import org.atlasapi.messaging.EndReplayMessage;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.MessageStore;
import org.atlasapi.messaging.ReplayMessage;
import org.atlasapi.serialization.json.JsonFactory;
import org.joda.time.DateTime;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class MessageReplayer {

    private final Clock clock = new SystemClock();
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    private final MessageStore store;
    private final JmsTemplate producer;

    public MessageReplayer(MessageStore store, JmsTemplate producer) {
        this.store = store;
        this.producer = producer;
    }

    public void replay(String destination, DateTime from, DateTime to, Optional<String> source) {
        producer.send(destination, new JsonMessageCreator(new BeginReplayMessage(UUID.randomUUID().toString(), clock.now().getMillis(), null, null, null)));
        for (final Message original : store.get(from, to, source)) {
            producer.send(destination, new JsonMessageCreator(new ReplayMessage(UUID.randomUUID().toString(), clock.now().getMillis(), null, null, null, original)));
        }
        producer.send(destination, new JsonMessageCreator(new EndReplayMessage(UUID.randomUUID().toString(), clock.now().getMillis(), null, null, null)));
    }

    private class JsonMessageCreator implements MessageCreator {

        private final Message message;

        public JsonMessageCreator(Message message) {
            this.message = message;
        }

        @Override
        public javax.jms.Message createMessage(Session session) throws JMSException {
            try {
                return session.createTextMessage(mapper.writeValueAsString(message));
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }
    }
}