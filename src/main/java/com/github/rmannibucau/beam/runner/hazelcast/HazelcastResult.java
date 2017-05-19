package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.JetInstance;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class HazelcastResult implements Serializable, PipelineResult {
    public static class BatchMode extends HazelcastResult {
        private final Future<?> task;
        private final JetInstance instance;
        private final boolean shutdown;

        public BatchMode(final JetInstance instance, final HazelcastPipelineOptions options, final Future<?> submit) {
            this.instance = instance;
            this.task = submit;
            this.shutdown = options.isShutdownOnDone();
        }

        @Override
        public State getState() {
            if (task.isDone()) {
                if (shutdown && instance.getHazelcastInstance().getLifecycleService().isRunning()) {
                    instance.shutdown();
                }
                return State.DONE;
            }
            if (task.isCancelled()) {
                if (shutdown && instance.getHazelcastInstance().getLifecycleService().isRunning()) {
                    instance.shutdown();
                }
                return State.CANCELLED;
            }
            return State.RUNNING;
        }

        @Override
        public State cancel() throws IOException {
            task.cancel(true);
            return getState();
        }

        @Override
        public State waitUntilFinish(final Duration duration) {
            try {
                task.get(duration.getMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                Thread.interrupted();
            } catch (final ExecutionException e) {
                throw new Pipeline.PipelineExecutionException(e.getCause());
            } catch (final TimeoutException e) {
                try {
                    cancel();
                } catch (final IOException e1) {
                    throw new Pipeline.PipelineExecutionException(e);
                }
            }
            return getState();
        }

        @Override
        public State waitUntilFinish() {
            try {
                task.get();
            } catch (final InterruptedException e) {
                Thread.interrupted();
            } catch (final ExecutionException e) {
                throw new Pipeline.PipelineExecutionException(e.getCause());
            }
            return getState();
        }

        @Override
        public MetricResults metrics() {
            // TODO
            return null;
        }
    }
}
