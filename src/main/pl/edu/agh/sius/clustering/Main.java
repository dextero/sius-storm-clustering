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
import pl.edu.agh.sius.clustering.bolt.DataFilterBolt;

import java.util.*;

public class Main {
    private static boolean finished;

    private static class StdoutPrinterBolt extends BaseRichBolt {
        public int counter = 0;
        public int x;
        public int y;

        public StdoutPrinterBolt(int x,
                                 int y) {
            this.x = x;
            this.y = y;
        }

        public void prepare(Map map,
                            TopologyContext topologyContext,
                            OutputCollector outputCollector) {
        }

        public void execute(Tuple tuple) {
            double[] value = (double[]) tuple.getValue(0);
            System.out.println("result " + x + "," + y + ": " + Arrays.toString(value));
            if (++counter >= 100) {
                finished = true;
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new DataSource(3, 2));

        final int DIM_SIZE = 5;
        for (int y = 0; y < DIM_SIZE; ++y) {
            for (int x = 0; x < DIM_SIZE; x++) {
                DoubleRange[] range = new DoubleRange[] {
                    new DoubleRange(x * DataSource.DIMENSION_SIZE / ((double) DIM_SIZE),
                                    (x + 1) * DataSource.DIMENSION_SIZE / ((double) DIM_SIZE)),
                    new DoubleRange(y * DataSource.DIMENSION_SIZE / ((double) DIM_SIZE),
                                    (y + 1) * DataSource.DIMENSION_SIZE / ((double) DIM_SIZE))
                };

                String name = "filter" + x + "," + y;
                String name2 = "output" + x + "," + y;
                builder.setBolt(name, new DataFilterBolt(range)).shuffleGrouping("source");
                builder.setBolt(name2, new StdoutPrinterBolt(x, y)).shuffleGrouping(name);
            }
        }

        Config config = new Config();
//        config.setDebug(true);

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
