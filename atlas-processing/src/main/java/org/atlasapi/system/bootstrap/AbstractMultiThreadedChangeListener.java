package org.atlasapi.system.bootstrap;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class AbstractMultiThreadedChangeListener<T> implements ChangeListener<T> {

    private static final int NO_KEEP_ALIVE = 0;
    private final Logger log = LoggerFactory.getLogger(AbstractMultiThreadedChangeListener.class);

    private final ThreadPoolExecutor executor;

    public AbstractMultiThreadedChangeListener(int concurrencyLevel) {
        this(new ThreadPoolExecutor(concurrencyLevel, concurrencyLevel,
                NO_KEEP_ALIVE, TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<Runnable>(100 * Runtime.getRuntime().availableProcessors()),
                new ThreadFactoryBuilder().setNameFormat(AbstractMultiThreadedChangeListener.class + " Thread %d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy()));
    }

    public AbstractMultiThreadedChangeListener(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void beforeChange() {
        // No-op
    }

    @Override
    public void afterChange() {
        // No-op
    }

    @Override
    public void onChange(Iterable<T> changed) {
        for (final T change : changed) {
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        onChange(change);
                    } catch (Exception ex) {
                        log.warn("Failed to process content {}, exception follows.", change);
                        log.warn(ex.getMessage(), ex);
                    }
                }
            });
        }
    }

    protected abstract void onChange(T change);
}
