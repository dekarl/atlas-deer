package org.atlasapi.messaging.workers;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.atlasapi.content.BrandRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.BeginReplayMessage;
import org.atlasapi.messaging.EndReplayMessage;
import org.atlasapi.messaging.ReplayMessage;
import org.atlasapi.messaging.ReplayingWorker;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.messaging.Worker;
import org.mockito.Mock;
import org.mockito.testng.MockitoTestNGListener;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.metabroadcast.common.time.Timestamp;

@Listeners(MockitoTestNGListener.class)
public class ReplayingWorkerTest {

    private final ExecutorService executor = Executors.newCachedThreadPool();
    @Mock Worker<ResourceUpdatedMessage> delegate;
    ReplayingWorker<ResourceUpdatedMessage> replayingWorker;
    ResourceUpdatedMessage rum
        = new ResourceUpdatedMessage("1", Timestamp.of(0L), new BrandRef(Id.valueOf(1), Publisher.BBC));
    
    
    @BeforeMethod
    public void setup() {
        replayingWorker = new ReplayingWorker<>(delegate);
    }
    
    @Test
    public void testNormalProcessing() {
        long threshold = 100;

        replayingWorker.setReplayThreshold(threshold);
        try {
            replayingWorker.start();

            replayingWorker.process(rum);

            verify(delegate, times(1)).process(rum);
        } finally {
            replayingWorker.stop();
        }
    }

    @Test
    public void testReplayProcessing() {
        long threshold = 100;
        replayingWorker.setReplayThreshold(threshold);
        try {
            replayingWorker.start();

            ReplayMessage<ResourceUpdatedMessage> replay
                = new ReplayMessage<>("2", Timestamp.of(2), rum);  

            replayingWorker.process(mock(BeginReplayMessage.class));
            replayingWorker.process(replay);
            replayingWorker.process(mock(EndReplayMessage.class));

            verify(delegate, times(1)).process(rum);
        } finally {
            replayingWorker.stop();
        }
    }

    @Test
    public void testNormalProcessingIsSuspendedDuringReplay() throws InterruptedException {
        final long threshold = 500;

        replayingWorker.setReplayThreshold(threshold);
        try {
            replayingWorker.start();

            replayingWorker.process(mock(BeginReplayMessage.class));

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    replayingWorker.process(rum);
                }
            });

            Thread.sleep(100);

            verify(delegate, times(0)).process(rum);
        } finally {
            replayingWorker.stop();
        }
    }

    @Test
    public void testNormalProcessingIsResumedAfterReplay() throws InterruptedException {
        final long threshold = 10000;

        replayingWorker.setReplayThreshold(threshold);
        final CountDownLatch processLatch = new CountDownLatch(1);
        try {
            replayingWorker.start();

            replayingWorker.process(mock(BeginReplayMessage.class));

            executor.submit(new Runnable() {
                @Override
                public void run() {
                    replayingWorker.process(rum);
                    processLatch.countDown();
                }
            });

            replayingWorker.process(mock(EndReplayMessage.class));

            assertTrue(processLatch.await(1, TimeUnit.SECONDS));

            verify(delegate, times(1)).process(rum);
        } finally {
            replayingWorker.stop();
        }
    }

    @Test
    public void testNormalProcessingIsResumedAfterReplayInterruption() throws InterruptedException {
        final long threshold = 1000;

        replayingWorker.setReplayThreshold(threshold);
        final CountDownLatch processLatch = new CountDownLatch(1);
        try {
            replayingWorker.start();

            replayingWorker.process(mock(BeginReplayMessage.class));

            executor.submit(new Runnable() {

                @Override
                public void run() {
                    replayingWorker.process(rum);
                    processLatch.countDown();
                }
            });

            assertTrue(processLatch.await(5000, TimeUnit.SECONDS));

            verify(delegate, times(1)).process(rum);
        } finally {
            replayingWorker.stop();
        }
    }

}
