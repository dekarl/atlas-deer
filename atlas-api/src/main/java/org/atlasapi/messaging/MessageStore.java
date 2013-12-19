package org.atlasapi.messaging;

import org.atlasapi.messaging.Message;
import org.joda.time.DateTime;

import com.google.common.base.Optional;

public interface MessageStore<M extends Message> {

    /**
     *
     * @param message
     */
    void add(M message);

    /**
     *
     * @param from
     * @param to
     * @param source 
     * @return
     */
    Iterable<M> get(DateTime from, DateTime to, Optional<String> source);
}