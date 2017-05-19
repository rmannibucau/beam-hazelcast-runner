package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;

import java.rmi.dgc.VMID;

public class HazelcastPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
    private final DAG dag;
    private final JetInstance instance;
    private final String prefix;
    private boolean finalized;

    private Edge edge;

    public HazelcastPipelineVisitor(final JetInstance instance, final HazelcastPipelineOptions options) {
        this.dag = new DAG();
        this.instance = instance;
        this.prefix = options.getNamingPrefix() == null ? new VMID().toString() + "#" : options.getNamingPrefix();
    }

    @Override
    public CompositeBehavior enterCompositeTransform(final TransformHierarchy.Node node) {
        checkState();
        return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void leaveCompositeTransform(final TransformHierarchy.Node node) {
        checkState();
        if (node.isRootNode()) {
            finalized = true;
        }
    }

    @Override
    public void visitPrimitiveTransform(final TransformHierarchy.Node node) {
        if (node.getInputs().isEmpty()) { // PBegin
            final String listName = getExecutionName(node.getFullName());
            final IStreamList<Object> list = instance.getList(listName);
            // put node.outputs in list somehow
            addVertex(dag.newVertex(node.getFullName(), Processors.readList(listName)));
        } else {
            // todo: handle inputs
            // addVertex(dag.newVertex(node.getFullName(), Processors.groupAndAccumulate()));
        }
    }

    public DAG getDag() {
        return dag;
    }

    private void addVertex(final Vertex vertex) {
        if (edge != null) {
            dag.edge(edge.to(vertex));
        }
        edge = Edge.from(vertex);
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