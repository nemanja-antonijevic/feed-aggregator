package com.nantonijevic.feeds.testing;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import reactor.blockhound.BlockingMethod;
import reactor.blockhound.BlockingOperationError;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.engine.TestExecutionResult;
import reactor.blockhound.BlockHound;

public final class BlockHoundTestExecutionListener
        implements TestExecutionListener {

    private static boolean installed;
    private static final ConcurrentLinkedQueue<String> DETECTIONS =
            new ConcurrentLinkedQueue<>();

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        synchronized (BlockHoundTestExecutionListener.class) {
            if (installed) {
                return;
            }

            BlockHound.builder()
                    .loadIntegrations()
                    // Mongo driver 5.8.1 seeds SecureRandom while creating a server-session UUID
                    // on its I/O event loop: FileInputStream.readBytes <- UUID.randomUUID <-
                    // ServerSessionPool.createNewServerSessionIdentifier. Keep this allowance
                    // to that one driver method; the async command path exposes no app frame.
                    .allowBlockingCallsInside(
                            "com.mongodb.internal.session.ServerSessionPool",
                            "createNewServerSessionIdentifier"
                    )
                    // Mongo driver cluster-clock contention parks in
                    // ClusterClock.advance -> ReentrantLock.lockInterruptibly -> Unsafe.park.
                    // This shared cluster-time lock is driver-owned, not application work.
                    .allowBlockingCallsInside(
                            "com.mongodb.internal.connection.ClusterClock",
                            "advance"
                    )
                    // Mongo driver 5.8.1 also parks while closing pooled connections:
                    // Unsafe.park -> ReentrantLock.lock ->
                    // DefaultConnectionPool$OpenConcurrencyLimiter.tryHandOverOrRelease.
                    // ConcurrentPool.StateAndPermits.releasePermit is nested in this method.
                    .allowBlockingCallsInside(
                            "com.mongodb.internal.connection.DefaultConnectionPool$OpenConcurrencyLimiter",
                            "tryHandOverOrRelease"
                    )
                    // The async handshake path releases the same pool permit in a separate method:
                    // DefaultConnectionPool$OpenConcurrencyLimiter.releasePermit -> Unsafe.park.
                    .allowBlockingCallsInside(
                            "com.mongodb.internal.connection.DefaultConnectionPool$OpenConcurrencyLimiter",
                            "releasePermit"
                    )
                    // Connection release also parks in the nested permit pool:
                    // ConcurrentPool$StateAndPermits.releasePermit -> ReentrantLock.lock -> Unsafe.park.
                    .allowBlockingCallsInside(
                            "com.mongodb.internal.connection.ConcurrentPool$StateAndPermits",
                            "releasePermit"
                    )
                    // The concurrent JSON-post test exposes Jackson's first-use cache lock:
                    // Unsafe.park -> ReentrantLock.lock ->
                    // DeserializerCache._createAndCacheValueDeserializer. This is framework
                    // deserializer initialization; the stack has no application frame.
                    .allowBlockingCallsInside(
                            "tools.jackson.databind.deser.DeserializerCache",
                            "_createAndCacheValueDeserializer"
                    )
                    .blockingMethodCallback(
                            BlockHoundTestExecutionListener::logDetection
                    )
                    .install();
            installed = true;
        }
    }

    @Override
    public void executionFinished(
            TestIdentifier testIdentifier,
            TestExecutionResult testExecutionResult
    ) {
        flushDetections();
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        flushDetections();
    }

    private static void flushDetections() {
        String detection;
        while ((detection = DETECTIONS.poll()) != null) {
            System.out.println(detection);
        }
    }

    private static void logDetection(BlockingMethod method) {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        String firstApplicationFrame = Arrays.stream(stack)
                .filter(frame -> frame.getClassName()
                        .startsWith("com.nantonijevic.feeds."))
                .filter(frame -> !frame.getClassName().equals(
                        BlockHoundTestExecutionListener.class.getName()
                ))
                .findFirst()
                .map(StackTraceElement::toString)
                .orElse("<none>");
        String blockingStack = Arrays.stream(stack)
                .dropWhile(frame -> !frame.getClassName()
                                .equals(method.getClassName())
                        || !frame.getMethodName().equals(method.getName()))
                .limit(20)
                .map(StackTraceElement::toString)
                .collect(java.util.stream.Collectors.joining(" <- "));

        DETECTIONS.add(String.format(
                "BLOCKHOUND_DETECTION blocking=%s thread=%s firstAppFrame=%s trace=%s",
                method,
                Thread.currentThread().getName(),
                firstApplicationFrame,
                blockingStack
        ));
        throw new BlockingOperationError(method);
    }
}
