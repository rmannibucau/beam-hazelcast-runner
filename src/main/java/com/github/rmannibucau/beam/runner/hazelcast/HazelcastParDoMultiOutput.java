package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.ReadyCheckingSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.SimpleDoFnRunner;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.java.repackaged.com.google.common.collect.Iterables;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class HazelcastParDoMultiOutput implements ProcessorSupplier, DoFnRunners.OutputManager {
    private final PushbackSideInputDoFnRunner<?, ?> runner;
    private final ThreadLocal<Consumer<Object>> emitter = new ThreadLocal<>();
    private final String name;

    public HazelcastParDoMultiOutput(final TransformHierarchy.Node node, final PipelineOptions options, final ParDo.MultiOutput<?, ?> fn) {
        this.name = node.getFullName();
        final ExecutionContext.StepContext context = new ExecutionContext.StepContext() {
            @Override
            public String getStepName() {
                return null;
            }

            @Override
            public String getTransformName() {
                return null;
            }

            @Override
            public void noteOutput(final WindowedValue<?> output) {
                // no-op
            }

            @Override
            public void noteOutput(final TupleTag<?> tag, final WindowedValue<?> output) {
                // no-op
            }

            @Override
            public <T, W extends BoundedWindow> void writePCollectionViewData(final TupleTag<?> tag, final Iterable<WindowedValue<T>> data, final Coder<Iterable<WindowedValue<T>>> dataCoder, final W window, final Coder<W> windowCoder) throws IOException {
                // no-op
            }

            @Override
            public StateInternals stateInternals() {
                return null;
            }

            @Override
            public TimerInternals timerInternals() {
                return null;
            }
        };
        final SideInputReader sideInputReader = NullSideInputReader.of(fn.getSideInputs());
        final List<TupleTag<?>> additionalOutputs = fn.getAdditionalOutputTags().getAll();
        final WindowingStrategy windowingStrategy = PCollection.class.cast(Iterables.getOnlyElement(node.getInputs().values())).getWindowingStrategy();
        final SimpleDoFnRunner doFnRunner = new SimpleDoFnRunner(options, fn.getFn(), sideInputReader, this, fn.getMainOutputTag(), additionalOutputs, context, windowingStrategy);
        runner = SimplePushbackSideInputDoFnRunner.create(doFnRunner, fn.getSideInputs(), new ReadyCheckingSideInputReader() {
            @Override
            public boolean isReady(final PCollectionView<?> view, final BoundedWindow window) {
                return !sideInputReader.isEmpty();
            }

            @Nullable
            @Override
            public <T> T get(final PCollectionView<T> view, final BoundedWindow window) {
                return sideInputReader.get(view, window);
            }

            @Override
            public <T> boolean contains(final PCollectionView<T> view) {
                return sideInputReader.get(view, GlobalWindow.INSTANCE) != null;
            }

            @Override
            public boolean isEmpty() {
                return sideInputReader.isEmpty();
            }
        });
    }

    @Override
    public void init(final Context context) {
        runner.startBundle();
    }

    @Override
    public void complete(final Throwable error) {
        runner.finishBundle();
        if (error != null) {
            throw new IllegalStateException(error);
        }
    }

    @Override
    public Collection<? extends Processor> get(final int count) { // todo: respect count
        return Collections.singleton(new AbstractProcessor() {
            @Override
            protected boolean tryProcess(final int ordinal, final Object item) throws Exception {
                emitter.set(this::emit);
                try {
                    // todo: fix it more accurately, surely a type to add during visit
                    final WindowedValue windowedValue = WindowedValue.class.isInstance(item) ?
                            WindowedValue.class.cast(item) : WindowedValue.valueInGlobalWindow(item);
                    runner.processElementInReadyWindows(windowedValue);
                } finally {
                    emitter.remove();
                }
                return true;
            }

            @Override
            public String toString() {
                return HazelcastParDoMultiOutput.class.getSimpleName() + "#Processor{" + name + "}";
            }
        });
    }

    @Override
    public <T> void output(final TupleTag<T> tag, final WindowedValue<T> output) {
        emitter.get().accept(output);
    }

    @Override
    public String toString() {
        return "HazelcastParDoMultiOutput{" +
                "name='" + name + '\'' +
                '}';
    }
}
