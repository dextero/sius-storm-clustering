package pl.edu.agh.sius.clustering;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import pl.edu.agh.sius.clustering.visualizer.Visualizer;

import java.util.*;

public class DataSource extends BaseRichSpout {

    private final Random rng;

    private ArrayList<ClusterDef> clusters = new ArrayList<>();
    private SpoutOutputCollector collector;
    private long counter = 0;

    public static final double DIMENSION_SIZE = 1.0;
    public static final double MAX_VARIANCE = 0.05;

    public DataSource(int numClusters,
                      int numDimensions) {
        assert numClusters > 0;
        assert numDimensions > 0;

        rng = new Random();

        for (int clusterIdx = 0; clusterIdx < numClusters; clusterIdx++) {
            double[] mean = new double[numDimensions];
            double[] variance = new double[numDimensions];

            for (int dimIdx = 0; dimIdx < mean.length; dimIdx++) {
                mean[dimIdx] = rng.nextDouble() * DIMENSION_SIZE;
                variance[dimIdx] = rng.nextDouble() * MAX_VARIANCE;
            }

            clusters.add(new ClusterDef(mean, variance));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value", "timestamp"));
    }

    public void open(Map map,
                     TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }

    public static List<ColoredPoint> points = new ArrayList<>();
    public static final int MAX_POINTS = 100;

    public void nextTuple() {
        int clusterIdx = rng.nextInt(clusters.size());
        ClusterDef cluster = clusters.get(clusterIdx);
        double[] point = cluster.generatePoint();

        points.add(new ColoredPoint(point, clusterIdx));
        if (points.size() >= MAX_POINTS) {
            Visualizer.points = points;
            points = new ArrayList<>();
        }

        collector.emit(new Values(point, counter++));
    }
}
