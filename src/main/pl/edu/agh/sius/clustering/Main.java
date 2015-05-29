package pl.edu.agh.sius.clustering;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import pl.edu.agh.sius.clustering.bolt.DataFilterBolt;

public class Main {
    public static boolean finished;

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
