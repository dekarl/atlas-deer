package org.atlasapi.system.bootstrap;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.scheduling.FailedRunHandler;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.UpdateProgress;

public class ExecutorServiceScheduledTaskTest {

    
    @Test
    public void testTasksRunInParallel() {
        
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean task1ran = new AtomicBoolean(false);
        final AtomicBoolean task2ran = new AtomicBoolean(false);
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Supplier<Iterable<? extends Callable<UpdateProgress>>> taskSupplier =
            supply(ImmutableList.of(new Callable<UpdateProgress>() {
                @Override
                public UpdateProgress call() throws Exception {
                    barrier.await();
                    task1ran.set(true);
                    return UpdateProgress.START;
                }
            }, new Callable<UpdateProgress>() {
                @Override
                public UpdateProgress call() throws Exception {
                    barrier.await();
                    task2ran.set(true);
                    return UpdateProgress.START;
                }
            }
        ));
        ExecutorServiceScheduledTask<UpdateProgress> task
            = new ExecutorServiceScheduledTask<UpdateProgress>(executor, taskSupplier, 2, 1, TimeUnit.SECONDS);
        
        task.run();
        
        assertTrue(task1ran.get());
        assertTrue(task2ran.get());
    }

    @Test
    public void testFailsOnTimeout() {
        
        final AtomicBoolean taskInterrupted = new AtomicBoolean(false);
        
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Supplier<Iterable<? extends Callable<UpdateProgress>>> taskSupplier =
            supply(ImmutableList.of(new Callable<UpdateProgress>() {
                @Override
                public UpdateProgress call() throws Exception {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        taskInterrupted.set(true);
                    }
                    return UpdateProgress.START;
                }
            }
        ));
        ExecutorServiceScheduledTask<UpdateProgress> task
            = new ExecutorServiceScheduledTask<UpdateProgress>(executor, taskSupplier, 1, 250, TimeUnit.MILLISECONDS,
                    new FailedRunHandler() {
                        @Override
                        public void handle(ScheduledTask task, Throwable e) {
                            assertTrue(e instanceof RuntimeException);
                        }
                    });
        
        task.run();
        
        assertTrue(taskInterrupted.get());
    }

    @Test
    public void testFailsAfterThreeAttemptsToSubmitJob() {
        
        final AtomicBoolean taskInterrupted = new AtomicBoolean(false);
        final AtomicBoolean neverRun = new AtomicBoolean(true);
        
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Supplier<Iterable<? extends Callable<UpdateProgress>>> taskSupplier =
                supply(ImmutableList.of(new Callable<UpdateProgress>() {
                    @Override
                    public UpdateProgress call() throws Exception {
                        try {
                            Thread.sleep(50000);
                        } catch (InterruptedException ie) {
                            taskInterrupted.set(true);
                        }
                        return UpdateProgress.START;
                    }
                },new Callable<UpdateProgress>() {
                    @Override
                    public UpdateProgress call() throws Exception {
                        neverRun.set(false);
                        return UpdateProgress.START;
                    }
                }));
        ExecutorServiceScheduledTask<UpdateProgress> task
        = new ExecutorServiceScheduledTask<UpdateProgress>(executor, taskSupplier, 1, 250, TimeUnit.MILLISECONDS,
                new FailedRunHandler() {
                    @Override
                    public void handle(ScheduledTask task, Throwable e) {
                        assertTrue(e instanceof RuntimeException);
                    }
                });
        
        task.run();
        
        assertTrue(taskInterrupted.get());
        assertTrue(neverRun.get());
    }
    
    private Supplier<Iterable<? extends Callable<UpdateProgress>>> supply(final Iterable<? extends Callable<UpdateProgress>> tasks) {
        return new Supplier<Iterable<? extends Callable<UpdateProgress>>>() {
            @Override
            public Iterable<? extends Callable<UpdateProgress>> get() {
                return tasks;
            }
        };
    }
    
    
    
}
