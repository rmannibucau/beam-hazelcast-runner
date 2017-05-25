package com.github.rmannibucau.beam.runner.hazelcast;

import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;

import static java.util.Collections.singleton;

public class HazelcastRegistrar implements PipelineOptionsRegistrar, PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
        return singleton(HazelcastPipelineOptions.class);
    }

    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
        return singleton(HazelcastRunner.class);
    }
}
