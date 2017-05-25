package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Vertex;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.rmi.dgc.VMID;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class HazelcastPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
    private final DAG dag;
    private final JetInstance instance;
    private final HazelcastPipelineOptions options;
    private final String prefix;

    private boolean finalized;
    private final LinkedList<LinkedList<Vertex>> previousCompositeVertices = new LinkedList<>();
    private final Map<TransformHierarchy.Node, Vertex> nodeVerticesMapping = new HashMap<>();

    public HazelcastPipelineVisitor(final boolean prefix, final JetInstance instance, final HazelcastPipelineOptions options) {
        this.dag = new DAG();
        this.instance = instance;
        this.prefix = !prefix ? "" : (options.getNamingPrefix() == null ? new VMID().toString() + "#" : options.getNamingPrefix());
        this.options = options;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
        checkState();
        if (!nodeVerticesMapping.containsKey(node)) {
            final Vertex vertex = dag.newVertex(getExecutionName(node.getFullName()), (ProcessorSupplier) count -> Collections.singleton(new AbstractProcessor() {
                @Override
                protected boolean tryProcess(final int ordinal, @Nonnull final Object item) throws Exception {
                    emit(ordinal, item);
                    return true;
                }
            }));
            nodeVerticesMapping.put(node, vertex);
            handleEdge(node, vertex);
        }
        final LinkedList<Vertex> nodes = new LinkedList<>();
        nodes.add(nodeVerticesMapping.get(node));
        previousCompositeVertices.addLast(nodes);
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(final TransformHierarchy.Node node) {
        checkState();
        if (node.isRootNode()) {
            finalized = true;
        }
        //previousCompositeVertices.removeLast(); // TODO: see how to handle that properly (GraphGenerator helps)
    }

    @Override
    public void visitPrimitiveTransform(final TransformHierarchy.Node node) { // todo: revise if we should have processor affected to nodes/Address
        final PTransform<?, ?> transform = node.getTransform();
        if (Read.Bounded.class.isInstance(transform)) {
            final Read.Bounded<?> bounded = Read.Bounded.class.cast(transform);
            final BoundedSource<?> source = bounded.getSource();
            try {
                final Vertex vertex = dag.newVertex(getExecutionName(node.getFullName()), (ProcessorSupplier) count -> {
                    if (count == 1) {
                        try {
                            return Collections.singleton(new BoundedProcessor(instance, source.createReader(options)));
                        } catch (final IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                    try {
                        final List<Processor> list = source.split(count, options).stream()
                                .map(bs -> {
                                    try {
                                        return bs.createReader(options);
                                    } catch (final IOException e) {
                                        throw new IllegalStateException(e);
                                    }
                                })
                                .map(r -> new BoundedProcessor(instance, r))
                                .collect(toList());
                        if (list.size() < count) { // check split impl (doesnt have to respect the contract but we need to do it ourself)
                            for (int i = list.size(); i < count; i++) {
                                list.add(new AbstractProcessor() {});
                            }
                        }
                        return list;
                    } catch (final Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
                handleEdge(node, vertex);
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
            System.out.println("rb");
        } else if (Read.Unbounded.class.isInstance(transform)) { // do we map it on IStream?
            System.out.println("ru");
        } else if (ParDo.SingleOutput.class.isInstance(transform)) {
            System.out.println("pds");
        } else if (ParDo.MultiOutput.class.isInstance(transform)) {
            final ParDo.MultiOutput<?, ?> fn = ParDo.MultiOutput.class.cast(transform);
            final Vertex vertex = dag.newVertex(getExecutionName(node.getFullName()), new HazelcastParDoMultiOutput(node, options, fn));
            handleEdge(node, vertex);
        } else if (Flatten.Iterables.class.isInstance(transform)) {// flatmap
            System.out.println("fi");
        } else if (Flatten.PCollections.class.isInstance(transform)) { // flatmap
            System.out.println("fpc"); // TODO
        } else if (Window.class.isInstance(transform)) {
            // no-op
        } else if (GroupByKey.class.isInstance(transform)) {
            // no-op
        } else if (Window.Assign.class.isInstance(transform)) {
            // no-op
        } else {
            throw new UnsupportedOperationException(node.getFullName());
        }
    }

    public DAG getDag() {
        return dag;
    }

    private void handleEdge(final TransformHierarchy.Node node, final Vertex vertex) {
        final LinkedList<Vertex> last = previousCompositeVertices.peekLast();
        if (last != null && !last.isEmpty()) {
            final Vertex lastVertex = last.peekLast();
            dag.edge(Edge.from(lastVertex, dag.getOutboundEdges(lastVertex.getName()).size()).to(vertex, dag.getInboundEdges(vertex.getName()).size()));
        } else if (node.getEnclosingNode() != null) {
            final Vertex source = nodeVerticesMapping.get(node.getEnclosingNode());
            dag.edge(Edge.from(source, dag.getOutboundEdges(source.getName()).size()).to(vertex, dag.getInboundEdges(vertex.getName()).size()));
        }
        if (!previousCompositeVertices.isEmpty()) {
            previousCompositeVertices.peekLast().add(vertex);
        }
    }

    private void checkState() {
        if (finalized) {
            throw new IllegalStateException("Already finalized pipeline");
        }
    }

    private String getExecutionName(final String name) {
        return prefix + name;
    }
}
