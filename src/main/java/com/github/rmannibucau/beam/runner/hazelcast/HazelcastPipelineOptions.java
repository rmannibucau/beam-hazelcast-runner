package com.github.rmannibucau.beam.runner.hazelcast;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface HazelcastPipelineOptions extends PipelineOptions {
    @Description("Is underlying Hazelcast Jet instance managed by the execution or assumed existing.")
    boolean isShutdownOnDone();
    void setShutdownOnDone(boolean doit);

    @Description("How list/maps are prefixed based on the beam model.")
    String getNamingPrefix();
    void setNamingPrefix(String prefix);
}
