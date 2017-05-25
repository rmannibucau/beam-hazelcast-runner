package com.github.rmannibucau.beam.runner.hazelcast;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class HazelcastRunnerTest implements Serializable {
    @Rule
    public transient ExpectedException thrown = ExpectedException.none();

    @Test
    public void wordCountShouldSucceed() throws Throwable {
        final Pipeline p = getPipeline();

        final PCollection<KV<String, Long>> counts =
                p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
                        .apply(MapElements.<String, String>via(new SimpleFunction<String, String>() {
                            @Override
                            public String apply(final String input) {
                                return input;
                            }
                        }))
                        .apply(Count.perElement());
        final PCollection<String> countStrs =
                counts.apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return String.format("%s: %s", input.getKey(), input.getValue());
                    }
                }));

        PAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

        System.out.println(p);

        final PipelineResult result = p.run();
        assertEquals(PipelineResult.State.DONE, result.waitUntilFinish());
        System.out.println(HazelcastResult.class.cast(result).get()); // todo: some assert or we assume PAssert did it?
    }

    private Pipeline getPipeline() {
        final PipelineOptions opts = PipelineOptionsFactory.create();
        opts.setRunner(HazelcastRunner.class);
        return Pipeline.create(opts);
    }
}
