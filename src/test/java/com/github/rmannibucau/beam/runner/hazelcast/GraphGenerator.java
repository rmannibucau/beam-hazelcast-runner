package com.github.rmannibucau.beam.runner.hazelcast;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import edu.uci.ics.jung.algorithms.layout.AbstractLayout;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.decorators.EdgeShape;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.Renderer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.collections15.Transformer;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.awt.image.AffineTransformOp;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class GraphGenerator implements Serializable {
    @Test
    public void generateGraph() throws Throwable {
        final Pipeline p = getPipeline();

        p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
                .apply(MapElements.<String, String>via(new SimpleFunction<String, String>() {
                    @Override
                    public String apply(final String input) {
                        return input;
                    }
                }))
                .apply(Count.perElement())
                .apply(MapElements.<KV<String, Long>, String>via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return String.format("%s: %s", input.getKey(), input.getValue());
                    }
                }));

        System.out.println(p);

        final HazelcastPipelineVisitor visitor = new HazelcastPipelineVisitor(
                false,
                Jet.newJetInstance(new JetConfig()
                        .setInstanceConfig(new InstanceConfig()
                                .setCooperativeThreadCount(1))),
                PipelineOptionsFactory.fromArgs(new String[0]).create().as(HazelcastPipelineOptions.class));

        p.traverseTopologically(visitor);

        final DAG dag = visitor.getDag(); // truthly speaking using a graph visitor would allow to get beam graph, for now i want jet one but you get the idea
        final DirectedSparseGraph<Vertex, Edge> diagram = new DirectedSparseGraph<>();
        dag.forEach(v -> {
            diagram.addVertex(v);
            dag.getOutboundEdges(v.getName()).forEach(e -> diagram.addEdge(e, e.getSource(), e.getDestination()));
        });

        // all that should be configurable
        final Dimension outputSize = new Dimension((int) (1024 * 2.5), 1024);
        final Function<String, String> labeller = s -> Stream.of(s.split("/")).collect(Collectors.joining("/<br>", "<html>", "</html>"));
        final LevelLayout layout = new LevelLayout(diagram);
        final VisualizationViewer<Vertex, Edge> viewer = new VisualizationViewer<>(layout);
        layout.setVertexShapeTransformer(viewer.getRenderContext().getVertexShapeTransformer());
        layout.setSize(outputSize);
        layout.setIgnoreSize(true);
        layout.reset();
        layout.setSize(outputSize);
        layout.reset();
        viewer.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<Vertex>() {
            @Override
            public String transform(final Vertex vertex) {
                return labeller.apply(vertex.getName());
            }
        });
        viewer.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller<Edge>() {
            @Override
            public String transform(final Edge edge) {
                return labeller.apply(edge.getSourceOrdinal() + " -> " + edge.getDestOrdinal());
            }
        });
        viewer.getRenderContext().setEdgeShapeTransformer(new EdgeShape.Line<>());
        viewer.getRenderContext().getEdgeLabelRenderer().setRotateEdgeLabels(false);
        viewer.getRenderer().getVertexLabelRenderer().setPosition(Renderer.VertexLabel.Position.CNTR);
        viewer.setPreferredSize(layout.getSize());
        viewer.setSize(layout.getSize());

        saveView(layout.getSize(), outputSize, viewer);
    }

    private Pipeline getPipeline() {
        final PipelineOptions opts = PipelineOptionsFactory.create();
        opts.setRunner(HazelcastRunner.class);
        return Pipeline.create(opts);
    }

    private static void saveView(final Dimension currentSize, final Dimension desiredSize,
                                 final VisualizationViewer<Vertex, Edge> viewer) throws Exception {
        BufferedImage bi = new BufferedImage(currentSize.width, currentSize.height, BufferedImage.TYPE_INT_ARGB);

        final Graphics2D g = bi.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON);
        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);

        final boolean db = viewer.isDoubleBuffered();
        viewer.setDoubleBuffered(false);
        viewer.paint(g);
        viewer.setDoubleBuffered(db);
        if (!currentSize.equals(desiredSize)) {
            double xFactor = desiredSize.width * 1. / currentSize.width;
            double yFactor = desiredSize.height * 1. / currentSize.height;
            double factor = Math.min(xFactor, yFactor);

            final AffineTransform tx = new AffineTransform();
            tx.scale(factor, factor);
            final AffineTransformOp op = new AffineTransformOp(tx, AffineTransformOp.TYPE_BILINEAR);
            BufferedImage biNew = new BufferedImage((int) (bi.getWidth() * factor), (int) (bi.getHeight() * factor), bi.getType());
            bi = op.filter(bi, biNew);
        }
        g.dispose();

        OutputStream os = null;
        try {
            os = new FileOutputStream(new File("target/graph.png"));
            if (!ImageIO.write(bi, "png", os)) {
                throw new IllegalStateException("can't save picture graph.png");
            }
        } finally {
            if (os != null) {
                try {
                    os.flush();
                    os.close();
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    public class LevelLayout extends AbstractLayout<Vertex, Edge> {
        private static final int X_MARGIN = 4;

        private Transformer<Vertex, Shape> vertexShapeTransformer;
        private boolean adjust;

        public LevelLayout(final Graph<Vertex, Edge> VertexEdgeGraph) {
            super(VertexEdgeGraph);
        }

        @Override
        public void initialize() {
            final Map<Vertex, Integer> level = levels();
            final List<List<Vertex>> vertices = sortVertexByLevel(level);
            final int ySpace = maxHeight(vertices);
            final int nLevels = vertices.size();
            final int yLevel = Math.max(0, getSize().height - nLevels * ySpace) / Math.max(1, nLevels - 1);

            int y = ySpace / 2;
            int maxWidth = getSize().width;
            for (final List<Vertex> currentVertexs : vertices) {
                if (currentVertexs.size() == 1) { // only 1 => centering manually
                    setLocation(currentVertexs.iterator().next(), new Point(getSize().width / 2, y));
                } else {
                    int x = 0;
                    final int xLevel = Math.max(0, getSize().width - width(currentVertexs) - X_MARGIN) / (currentVertexs.size() - 1);
                    currentVertexs.sort(new NodeComparator(graph, locations));

                    for (final Vertex Vertex : currentVertexs) {
                        final Rectangle b = getBound(Vertex, vertexShapeTransformer);
                        final int step = b.getBounds().width / 2;
                        x += step;
                        setLocation(Vertex, new Point(x, y));
                        x += xLevel + step;
                    }

                    maxWidth = Math.max(maxWidth, x - xLevel);
                }
                y += yLevel + ySpace;
            }

            if (adjust) {
                setIgnoreSize(false);
                setSize(new Dimension(maxWidth, y + ySpace));
                initialize();
                setIgnoreSize(true);
            }
        }

        @Override
        public void reset() {
            initialize();
        }

        private int width(List<Vertex> Vertexs) {
            int sum = 0;
            for (Vertex Vertex : Vertexs) {
                sum += getBound(Vertex, vertexShapeTransformer).width;
            }
            return sum;
        }

        private int maxHeight(final List<List<Vertex>> Vertexs) {
            int max = 0;
            for (List<Vertex> list : Vertexs) {
                for (Vertex n : list) {
                    max = Math.max(max, getBound(n, vertexShapeTransformer).height);
                }
            }
            return max;
        }

        private Rectangle getBound(final Vertex n, final Transformer<Vertex, Shape> vst) {
            if (vst == null) {
                return new Rectangle(0, 0);
            }
            return vst.transform(n).getBounds();
        }

        private List<List<Vertex>> sortVertexByLevel(final Map<Vertex, Integer> level) {
            int levels = max(level);

            List<List<Vertex>> sorted = new ArrayList<>();
            for (int i = 0; i < levels; i++) {
                sorted.add(new ArrayList<>());
            }

            for (final Map.Entry<Vertex, Integer> entry : level.entrySet()) {
                sorted.get(entry.getValue()).add(entry.getKey());
            }

            sorted.forEach(l -> l.sort((o1, o2) -> {
                final int incidentEdges1 = graph.getIncidentEdges(o1).stream().mapToInt(Edge::getSourceOrdinal).max().orElse(0);
                final int incidentEdges2 = graph.getIncidentEdges(o2).stream().mapToInt(Edge::getSourceOrdinal).max().orElse(0);
                if (incidentEdges1 == incidentEdges2) {
                    return o1.getName().compareTo(o2.getName());
                }
                return incidentEdges1 - incidentEdges2;
            }));

            return sorted;
        }

        private int max(final Map<Vertex, Integer> level) {
            int i = 0;
            for (Map.Entry<Vertex, Integer> l : level.entrySet()) {
                if (l.getValue() >= i) {
                    i = l.getValue() + 1;
                }
            }
            return i;
        }

        private Map<Vertex, Integer> levels() {
            final Map<Vertex, Integer> out = graph.getVertices().stream().collect(toMap(identity(), i -> 0));

            final Map<Vertex, Collection<Vertex>> successors = new HashMap<>();
            final Map<Vertex, Collection<Vertex>> predecessors = new HashMap<>();
            for (final Vertex v : graph.getVertices()) {
                successors.put(v, graph.getSuccessors(v));
                predecessors.put(v, graph.getPredecessors(v));
            }

            boolean done;
            do {
                done = true;
                for (final Vertex v : graph.getVertices()) {
                    int level = out.get(v);
                    for (final Vertex successor : successors.get(v)) {
                        if (out.get(successor) <= level
                                && successor != v
                                && !predecessors.get(v).contains(successor)) {
                            done = false;
                            out.put(successor, level + 1);
                        }
                    }
                }
            } while (!done);

            final int min = out.values().stream().mapToInt(i -> i).min().orElse(0);
            return out.entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.getValue() - min));
        }

        public void setVertexShapeTransformer(Transformer<Vertex, Shape> vertexShapeTransformer) {
            this.vertexShapeTransformer = vertexShapeTransformer;
        }

        public void setIgnoreSize(boolean adjust) {
            this.adjust = adjust;
        }
    }

    public static class NodeComparator implements Comparator<Vertex> { // sort by predecessor location
        private Graph<Vertex, Edge> graph;
        private Map<Vertex, Point2D> locations;

        public NodeComparator(final Graph<Vertex, Edge> diagram, final Map<Vertex, Point2D> points) {
            graph = diagram;
            locations = points;
        }

        @Override
        public int compare(final Vertex o1, final Vertex o2) {
            // mean value is used but almost always there is only one predecessor
            int m1 = mean(graph.getPredecessors(o1));
            int m2 = mean(graph.getPredecessors(o2));
            return m1 - m2;
        }

        private int mean(final Collection<Vertex> p) {
            return p.isEmpty() ? 0 : (int) p.stream().mapToDouble(n -> locations.get(n).getX()).sum() / p.size();
        }
    }
}
