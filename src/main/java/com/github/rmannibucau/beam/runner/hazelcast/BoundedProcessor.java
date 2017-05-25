package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Traverser;
import org.apache.beam.sdk.io.BoundedSource;

import java.io.IOException;
import java.util.Iterator;

import static com.hazelcast.jet.Traversers.traverseIterable;

public class BoundedProcessor extends AbstractProcessor {
    private final BoundedSource.BoundedReader<?> reader;
    private final Traverser<Object> traverser;

    public BoundedProcessor(final BoundedSource.BoundedReader<?> reader) {
        this.reader = reader;
        this.traverser = traverseIterable(() -> new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                try {
                    return reader.advance();
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public Object next() {
                return reader.getCurrent();
            }
        });
    }

    @Override
    public boolean complete() {
        try {
            return emitCooperatively(traverser);
        } finally {
            try {
                reader.close();
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }
}
