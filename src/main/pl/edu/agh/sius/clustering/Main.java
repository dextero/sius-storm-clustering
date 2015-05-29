package pl.edu.agh.sius.clustering;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import pl.edu.agh.sius.clustering.bolt.DStreamBolt;
import pl.edu.agh.sius.clustering.bolt.DataFilterBolt;
import pl.edu.agh.sius.clustering.bolt.MergingBolt;
import pl.edu.agh.sius.clustering.visualizer.Visualizer;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static boolean finished;

    public static final int DIM_SIZE = 5;
    public static final int DIM_CUBES_PER_BOLT = 5;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new DataSource(3, 2));

        List<String> mergeInputs = new ArrayList<>();

        final double CUBE_SIZE = DataSource.DIMENSION_SIZE / ((double) (DIM_SIZE * DIM_CUBES_PER_BOLT));
        final double BOLT_CUBE_SIZE = DIM_CUBES_PER_BOLT * CUBE_SIZE;
        for (int y = 0; y < DIM_SIZE; ++y) {
            for (int x = 0; x < DIM_SIZE; x++) {
                DoubleRange[] range = new DoubleRange[] {
                    new DoubleRange(x * BOLT_CUBE_SIZE, (x + 1) * BOLT_CUBE_SIZE),
                    new DoubleRange(y * BOLT_CUBE_SIZE, (y + 1) * BOLT_CUBE_SIZE)
                };

                String name = "filter" + x + "," + y;
//                String name2 = "output" + x + "," + y;
                String name3 = "dstream" + x + "," + y;
                builder.setBolt(name, new DataFilterBolt(range)).shuffleGrouping("source");
                builder.setBolt(name3, new DStreamBolt(new double[] { CUBE_SIZE, CUBE_SIZE })).shuffleGrouping(name);
//                builder.setBolt(name2, new StdoutPrinterBolt(x, y)).shuffleGrouping(name3);
                mergeInputs.add(name3);
            }
        }

        BoltDeclarer mergingBolt = builder.setBolt("merging", new MergingBolt(DIM_SIZE)).setMaxTaskParallelism(1);
        for (String input : mergeInputs) {
            mergingBolt.shuffleGrouping(input);
        }

        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);
        config.put(Config.LOGVIEWER_APPENDER_NAME, "org.apache.log4j.varia.NullAppender");
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());

        Visualizer.show(42);

        while (!finished) {
//            System.err.println("waiting");
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
