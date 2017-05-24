package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;

public class HazelcastRunner extends PipelineRunner<HazelcastResult> {
    private final HazelcastPipelineOptions options;

    private HazelcastRunner(final HazelcastPipelineOptions options) {
        this.options = options;
    }

    @Override
    public HazelcastResult run(final Pipeline pipeline) {
        final JetInstance instance = getInstance();

        MetricsEnvironment.setMetricsSupported(true);

        // TODO: detect streaming and impl it

        /*
        if (options.isStreaming()) { // http://jet.hazelcast.org/getting-started/
            CheckpointDir checkpointDir = new CheckpointDir(options.getCheckpointDir());
            SparkRunnerStreamingContextFactory streamingContextFactory =
                    new SparkRunnerStreamingContextFactory(pipeline, options, checkpointDir);
            final JavaStreamingContext jssc =
                    JavaStreamingContext.getOrCreate(
                            checkpointDir.getSparkCheckpointDir().toString(), streamingContextFactory);

            // Checkpoint aggregator/metrics values
            jssc.addStreamingListener(
                    new JavaStreamingListenerWrapper(
                            new AggregatorsAccumulator.AccumulatorCheckpointingSparkListener()));
            jssc.addStreamingListener(
                    new JavaStreamingListenerWrapper(
                            new MetricsAccumulator.AccumulatorCheckpointingSparkListener()));

            // register user-defined listeners.
            for (JavaStreamingListener listener : options.as(SparkContextOptions.class).getListeners()) {
                LOG.info("Registered listener {}." + listener.getClass().getSimpleName());
                jssc.addStreamingListener(new JavaStreamingListenerWrapper(listener));
            }

            // register Watermarks listener to broadcast the advanced WMs.
            jssc.addStreamingListener(new JavaStreamingListenerWrapper(new WatermarksListener(jssc)));

            // The reason we call initAccumulators here even though it is called in
            // SparkRunnerStreamingContextFactory is because the factory is not called when resuming
            // from checkpoint (When not resuming from checkpoint initAccumulators will be called twice
            // but this is fine since it is idempotent).
            initAccumulators(options, jssc.sparkContext());

            startPipeline =
                    executorService.submit(
                            new Runnable() {

                                @Override
                                public void run() {
                                    LOG.info("Starting streaming pipeline execution.");
                                    jssc.start();
                                }
                            });

            result = new SparkPipelineResult.StreamingMode(startPipeline, jssc);
        } else {*/ // https://github.com/hazelcast/hazelcast-jet-code-samples
        // create the evaluation context
        // update the cache candidates
        // updateCacheCandidates(pipeline, translator, evaluationContext);

        // initAccumulators(options, jsc);

        // batch support
        final HazelcastPipelineVisitor visitor = new HazelcastPipelineVisitor(instance, options);
        pipeline.traverseTopologically(visitor);
        return new HazelcastResult.BatchMode(instance, options, instance.newJob(visitor.getDag()).execute());
        //}
    }

    private JetInstance getInstance() {
        // todo: migrate to client there
        return Jet.newJetInstance(new JetConfig()
                .setInstanceConfig(new InstanceConfig()
                        .setCooperativeThreadCount(1)));
    }

    public static HazelcastRunner fromOptions(final PipelineOptions options) {
        return new HazelcastRunner(options.as(HazelcastPipelineOptions.class));
    }
}
