package org.atlasapi.messaging.workers;

import java.io.IOException;

import org.atlasapi.messaging.BeginReplayMessage;
import org.atlasapi.messaging.EndReplayMessage;
import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.ReplayMessage;
import org.atlasapi.messaging.Worker;
import org.atlasapi.serialization.json.JsonFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Base {@link org.atlasapi.persistence.messaging.worker.Worker} class providing
 * {@link org.atlasapi.persistence.messaging.Message} unmarshaling and
 * dispatching.
 */
public abstract class AbstractWorker implements Worker {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();

    public void onMessage(String message) {
        try {
            Message event = mapper.readValue(message, Message.class);
            event.dispatchTo(this);
        } catch (IOException ex) {
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
