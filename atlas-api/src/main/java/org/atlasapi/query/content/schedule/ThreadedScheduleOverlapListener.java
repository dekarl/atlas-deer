package org.atlasapi.query.content.schedule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PreDestroy;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;

public class ThreadedScheduleOverlapListener implements ScheduleOverlapListener {

    private final ScheduleOverlapListener delegate;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final AdapterLog log;

    public ThreadedScheduleOverlapListener(ScheduleOverlapListener delegate, AdapterLog log) {
        this.delegate = delegate;
        this.log = log;
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }

    @Override
    public void itemRemovedFromSchedule(Item item, Broadcast broadcast) {
        executor.execute(new NotifyDelegate(item, broadcast));
    }

    class NotifyDelegate implements Runnable {

        private final Item item;
        private final Broadcast broadcast;

        public NotifyDelegate(Item item, Broadcast broadcast) {
            this.item = item;
            this.broadcast = broadcast;
        }

        public void run() {
            try {
            delegate.itemRemovedFromSchedule(item, broadcast);
            } catch (Exception e) {
                log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(getClass()).withDescription(e.getMessage()));
            }
        }
    }
}
