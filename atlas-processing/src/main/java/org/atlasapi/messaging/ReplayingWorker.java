package org.atlasapi.messaging;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.atlasapi.messaging.BeginReplayMessage;
import org.atlasapi.messaging.EndReplayMessage;
import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.Message;
import org.atlasapi.messaging.ReplayMessage;
import org.atlasapi.messaging.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplayingWorker extends AbstractWorker {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ReentrantLock replayLock = new ReentrantLock();
    private final Condition replayCondition = replayLock.newCondition();
    private final AtomicBoolean replaying = new AtomicBoolean(false);
    private final AtomicLong latestReplayTime = new AtomicLong(0);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    
    private final Worker delegate;
    private long replayThreshold;
    
    public ReplayingWorker(Worker delegate) {
        this.delegate = delegate;
        this.replayThreshold = 60000;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(new ReplayCircuitBreaker(), replayThreshold, replayThreshold, TimeUnit.MILLISECONDS);
    }
    
    public void stop() {
        scheduler.shutdownNow();
    }

    public void setReplayThreshold(long replayThreshold) {
        this.replayThreshold = replayThreshold;
    }

    @Override
    public void process(EntityUpdatedMessage message) {
        doProcess(message);
    }

    @Override
    public void process(BeginReplayMessage message) {
        doBeginReplay();
    }

    @Override
    public void process(EndReplayMessage message) {
        doEndReplay();
    }

    @Override
    public void process(ReplayMessage message) {
        doReplay(message);
    }

    private void doBeginReplay() {
        if (replaying.compareAndSet(false, true)) {
            log.warn("Starting replaying...");
            latestReplayTime.set(new Date().getTime());
        }
    }

    private void doEndReplay() {
        if (replaying.compareAndSet(true, false)) {
            log.warn("Finishing replaying...");
            latestReplayTime.set(0);
            replayLock.lock();
            try {
                replayCondition.signalAll();
            } finally {
                replayLock.unlock();
            }
        }
    }

    private void doReplay(ReplayMessage message) {
        if (replaying.get()) {
            latestReplayTime.set(new Date().getTime());
            message.getOriginal().dispatchTo(delegate);
        } else {
            log.warn("Cannot replay message outside of BeginReplayMessage - EndReplayMessage boundaries.");
        }
    }

    private void doProcess(Message message) {
        while (replaying.get()) {
            log.warn("In BeginReplayMessage - EndReplayMessage boundaries, waiting...");
            replayLock.lock();
            try {
                replayCondition.awaitUninterruptibly();
            } finally {
                replayLock.unlock();
            }
        }
        message.dispatchTo(delegate);
    }

    private class ReplayCircuitBreaker implements Runnable {

        @Override
        public void run() {
            if (latestReplayTime.get() > 0) {
                long now = new Date().getTime();
                long elapsed = now - latestReplayTime.get();
                if (elapsed > replayThreshold) {
                    log.warn("Too much time ({}) passed since last replay message, interrupting...", elapsed);
                    doEndReplay();
                }
            }
        }
    }
}
