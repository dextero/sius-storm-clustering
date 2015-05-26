package pl.edu.agh.sius.clustering;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

public class Main {
    private static boolean finished;

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
