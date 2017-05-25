package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;

public class PassthroughProcessorSupplier implements ProcessorSupplier {
    @Override
    public Collection<? extends Processor> get(final int count) {
        return Collections.singleton(new AbstractProcessor() {
            @Override
            protected boolean tryProcess(final int ordinal, @Nonnull final Object item) throws Exception {
                emit(ordinal, item);
                return true;
            }
        });
    }
}
