package pl.edu.agh.sius.clustering;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

public class DataSource extends BaseRichSpout {
    private final Random rng;

    private ArrayList<ClusterDef> clusters = new ArrayList<>();
    private SpoutOutputCollector collector;

    public static final double DIMENSION_SIZE = 1.0;
    public static final double MAX_VARIANCE = 0.1;

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
        outputFieldsDeclarer.declare(new Fields("value"));
    }

    public void open(Map map,
                     TopologyContext topologyContext,
                     SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }

    public void nextTuple() {
        int clusterIdx = rng.nextInt(clusters.size());
        ClusterDef cluster = clusters.get(clusterIdx);
        collector.emit(new Values(cluster.generatePoint()));
    }
}