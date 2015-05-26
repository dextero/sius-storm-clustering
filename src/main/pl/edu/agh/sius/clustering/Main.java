package pl.edu.agh.sius.clustering;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.clojure.RichShellSpout;
import backtype.storm.generated.StreamInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.Serializable;
import java.util.*;

public class Main {
    private static boolean finished;

    public static class ClusterDef implements Serializable {
        private final double[] mean;
        private final double[] variance;
        private final Random rng;

        public ClusterDef(double[] mean,
                          double[] variance) {
            assert mean.length == variance.length;

            this.mean = mean;
            this.variance = variance;
            this.rng = new Random();
        }

        public double[] generatePoint() {
            double[] ret = new double[this.mean.length];
            for (int i = 0; i < ret.length; i++) {
                ret[i] = rng.nextGaussian() * variance[i] + mean[i];
            }
            return ret;
        }
    }

    public static class DataSource extends BaseRichSpout {
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

    private static class StdoutPrinterBolt extends BaseRichBolt {
        public int counter = 0;

        public void prepare(Map map,
                            TopologyContext topologyContext,
                            OutputCollector outputCollector) {
        }

        public void execute(Tuple tuple) {
            double[] value = (double[]) tuple.getValue(0);
            System.out.println("result: " + Arrays.toString(value));
            if (++counter >= 100) {
                finished = true;
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        StdoutPrinterBolt printer = new StdoutPrinterBolt();

        builder.setSpout("source", new DataSource(3, 2));
        builder.setBolt("output", printer).shuffleGrouping("source");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());

        while (!finished) {
            System.err.println("waiting");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.err.println("killing topology");
        cluster.killTopology("test");
        cluster.shutdown();
    }
}
