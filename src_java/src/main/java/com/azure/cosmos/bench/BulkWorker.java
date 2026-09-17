package com.azure.cosmos.bench;

import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosBulkExecutionOptions;
import com.azure.cosmos.models.CosmosBulkExecutionThresholdsState;
import com.azure.cosmos.models.CosmosBulkItemResponse;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One independent bulk-ingest pipeline: a single {@code executeBulkOperations} call fed by a live
 * {@link Sinks.Many} emitter, with a {@link Semaphore} bounding in-flight operations and an
 * application-level retry loop that re-injects throttled/transient failures back into the same sink.
 *
 * <p>This mirrors the per-writer design in the Azure distributed-bulk sample. Several of these run
 * concurrently (one per shard) to saturate a large VM; each keeps its own sink, semaphore, and
 * retry-count state, and all share one throughput-control group (wired via the bulk options) so
 * aggregate RU stays governed.</p>
 *
 * <p>Why a sink and not a finite {@code Flux.range}: a live emitter lets the retry path push a failed
 * operation back into a pipeline that is already running. A finite Flux cannot be appended to once it
 * is emitting, so real app-level retry requires this shape.</p>
 */
final class BulkWorker {

    /** Retriable status codes (transient throttling / server / connection conditions). */
    static boolean shouldRetry(int status) {
        return status == 429   // throttled
                || status == 408   // request timeout
                || status == 449   // retry-with (transient concurrency)
                || status == 500   // internal
                || status == 503   // service unavailable
                || status == 410;  // gone (partition split/move)
    }

    private final CosmosAsyncContainer container;
    private final BenchmarkConfig cfg;
    private final CosmosBulkExecutionOptions options;
    private final ScheduledExecutorService retryScheduler;
    private final Scheduler responseScheduler;

    // Live input emitter. Unicast + onBackpressureBuffer: one subscriber (the bulk call), buffered.
    private final Sinks.Many<CosmosItemOperation> emitter =
            Sinks.many().unicast().onBackpressureBuffer();

    // Bounds in-flight ops so a fast producer cannot outrun the SDK and exhaust memory.
    private final Semaphore semaphore;

    // Shared counters (across all workers).
    private final AtomicLong success;
    private final AtomicLong throttled;
    private final AtomicLong failed;
    private final AtomicLong totalRu; // RU * 1000 fixed-point

    // Per-worker book-keeping to know when this shard is fully drained.
    private final AtomicLong scheduled = new AtomicLong();
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean producerDone = false;

    private static final Sinks.EmitFailureHandler EMIT_RETRY =
            (signalType, result) -> result == Sinks.EmitResult.FAIL_NON_SERIALIZED;

    BulkWorker(CosmosAsyncContainer container, BenchmarkConfig cfg,
               CosmosBulkExecutionThresholdsState thresholds, ScheduledExecutorService retryScheduler,
               Scheduler responseScheduler, AtomicLong success, AtomicLong throttled,
               AtomicLong failed, AtomicLong totalRu) {
        this.container = container;
        this.cfg = cfg;
        this.retryScheduler = retryScheduler;
        this.responseScheduler = responseScheduler;
        this.success = success;
        this.throttled = throttled;
        this.failed = failed;
        this.totalRu = totalRu;
        this.semaphore = new Semaphore(Math.max(1, cfg.maxInFlightPerWorker));

        // One options object per worker, but sharing the JVM-scoped thresholds state so the SDK's
        // adaptive micro-batch-size tuner keeps learning across the whole run. Starting small
        // (initialMicroBatchSize) lets it ramp up to the container's sweet spot instead of
        // overshooting into 429s immediately.
        CosmosBulkExecutionOptions o = new CosmosBulkExecutionOptions(thresholds)
                .setInitialMicroBatchSize(cfg.initialMicroBatchSize)
                .setMaxMicroBatchSize(cfg.maxMicroBatchSize)
                .setMaxMicroBatchConcurrency(cfg.maxMicroBatchConcurrency);
        if (cfg.throughputControlEnabled) {
            o.setThroughputControlGroupName(cfg.throughputControlGroup);
        }
        this.options = o;
    }

    /**
     * Run this worker's shard [start, end) to completion (including retries) and block until drained.
     * Returns when every scheduled op has terminally succeeded or exhausted its retries.
     */
    void runShard(long start, long end, String filler) {
        // Subscribe ONCE. The bulk call is a cold Flux, so we must not also block() it (that would
        // re-subscribe and spin up a second pipeline). Instead we complete a latch when the terminal
        // signal arrives (all ops drained + emitter completed) and await that.
        var disposable = container
                .executeBulkOperations(emitter.asFlux(), options)
                .publishOn(responseScheduler) // move response handling off the SDK IO threads
                .doOnNext(this::handleResponse)
                .doFinally(signal -> done.countDown())
                .subscribe();
        try {
            for (long i = start; i < end; i++) {
                acquire();
                scheduled.incrementAndGet();
                emitter.emitNext(newCreate(i, filler, 0), EMIT_RETRY);
            }
            producerDone = true;
            maybeComplete();
            done.await(); // block this worker thread until the pipeline terminates
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            disposable.dispose();
        }
    }

    private void acquire() {
        boolean got = false;
        while (!got) {
            try {
                got = semaphore.tryAcquire(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleResponse(CosmosBulkOperationResponse<?> response) {
        @SuppressWarnings("unchecked")
        CosmosItemOperation op = response.getOperation();
        CosmosBulkItemResponse item = response.getResponse();
        Exception ex = response.getException();

        int status = item != null ? item.getStatusCode()
                : (ex instanceof CosmosException ce ? ce.getStatusCode() : -1);

        if (item != null && item.isSuccessStatusCode()) {
            terminalSuccess(item.getRequestCharge());
            return;
        }
        if (status == 409) {
            // Conflict on CREATE == item already exists; treat as success for idempotent re-runs.
            terminalSuccess(item != null ? item.getRequestCharge() : 0);
            return;
        }
        if (status == 429) {
            throttled.incrementAndGet();
        }
        if (shouldRetry(status)) {
            Duration retryAfter = (ex instanceof CosmosException ce) ? ce.getRetryAfterDuration() : null;
            scheduleRetry(op, retryAfter);
        } else {
            terminalFailure(status, ex);
        }
    }

    private void terminalSuccess(double requestCharge) {
        success.incrementAndGet();
        totalRu.addAndGet((long) (requestCharge * 1000));
        semaphore.release();
        onScheduledDone();
        long done = success.get();
        if (done > 0 && done % 100_000 == 0) {
            System.out.printf("  ... %,d succeeded%n", done);
        }
    }

    private void terminalFailure(int status, Exception ex) {
        failed.incrementAndGet();
        semaphore.release();
        onScheduledDone();
        if (failed.get() <= 5) {
            System.out.printf("  [error sample] status=%d ex=%s%n",
                    status, ex != null ? ex.getMessage() : "n/a");
        }
    }

    private void scheduleRetry(CosmosItemOperation op, Duration retryAfter) {
        OpCtx ctx = op.getContext();
        int retryCount = ctx.retryCount;
        if (retryCount >= cfg.maxRetryCount) {
            // Exhausted: terminal failure. (Do NOT release the in-flight permit twice.)
            terminalFailure(-2, new RuntimeException("retries exhausted after " + retryCount));
            return;
        }
        // Jittered backoff: min 10ms/retry, up to ~1s * retryCount, honoring server Retry-After.
        int base = 10 * (retryCount + 1)
                + ThreadLocalRandom.current().nextInt(990 * Math.max(1, retryCount + 1));
        int delayMs = Math.max(base,
                retryAfter != null ? Math.min((int) retryAfter.toMillis(), 5000) : 0);

        CosmosItemOperation retry = newCreate(ctx.docIndex, ctx.filler, retryCount + 1);
        retryScheduler.schedule(
                () -> emitter.emitNext(retry, EMIT_RETRY), delayMs, TimeUnit.MILLISECONDS);
        // NOTE: the permit stays held across the retry (the op is still in flight); it is released
        // only on terminal success/failure. That keeps the in-flight bound honest.
    }

    /** A terminally-completed op frees its slot in the scheduled tally and may complete the sink. */
    private void onScheduledDone() {
        long remaining = scheduled.decrementAndGet();
        if (remaining == 0) {
            maybeComplete();
        }
    }

    private void maybeComplete() {
        if (producerDone && scheduled.get() == 0) {
            emitter.emitComplete(EMIT_RETRY);
        }
    }

    /** Build a CREATE operation carrying its own retry context (doc index + retry count). */
    private CosmosItemOperation newCreate(long i, String filler, int retryCount) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        java.util.List<Float> emb = new java.util.ArrayList<>(cfg.vectorDim);
        for (int d = 0; d < cfg.vectorDim; d++) {
            emb.add(rnd.nextFloat() * 2.0f - 1.0f);
        }
        String docid = java.util.UUID.randomUUID().toString();
        VectorDoc doc = new VectorDoc(
                java.util.UUID.randomUUID().toString(), docid, "Document " + i, filler, emb);
        return CosmosBulkOperations.getCreateItemOperation(
                doc, new PartitionKey(docid), new OpCtx(i, retryCount, filler));
    }

    /** Per-operation context: doc index, retry count, and filler so a retry can rebuild the doc. */
    static final class OpCtx {
        final long docIndex;
        final int retryCount;
        final String filler;

        OpCtx(long docIndex, int retryCount, String filler) {
            this.docIndex = docIndex;
            this.retryCount = retryCount;
            this.filler = filler;
        }
    }
}
