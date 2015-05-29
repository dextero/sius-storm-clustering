package pl.edu.agh.sius.clustering;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import pl.edu.agh.sius.clustering.bolt.DStreamBolt;
import pl.edu.agh.sius.clustering.bolt.DataFilterBolt;
import pl.edu.agh.sius.clustering.bolt.MergingBolt;
import pl.edu.agh.sius.clustering.visualizer.Visualizer;

import com.badlogic.gdx.backends.lwjgl.LwjglApplication;
import com.badlogic.gdx.backends.lwjgl.LwjglApplicationConfiguration;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static boolean finished;

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new DataSource(3, 2));

        List<String> mergeInputs = new ArrayList<>();

        final double BOLT_CUBE_SIZE = Constants.DIM_CUBES_PER_BOLT * Constants.CUBE_SIZE;
        final double[] CUBE_SIZE = new double[] { Constants.CUBE_SIZE, Constants.CUBE_SIZE };
        for (int y = 0; y < Constants.DIM_SIZE; ++y) {
            for (int x = 0; x < Constants.DIM_SIZE; x++) {
                DoubleRange[] range = new DoubleRange[] {
                    new DoubleRange(x * BOLT_CUBE_SIZE, (x + 1) * BOLT_CUBE_SIZE),
                    new DoubleRange(y * BOLT_CUBE_SIZE, (y + 1) * BOLT_CUBE_SIZE)
                };

                String name = "filter" + x + "," + y;
//                String name2 = "output" + x + "," + y;
                String name3 = "dstream" + x + "," + y;
                builder.setBolt(name, new DataFilterBolt(range)).shuffleGrouping("source");
                builder.setBolt(name3, new DStreamBolt(CUBE_SIZE)).shuffleGrouping(name);
//                builder.setBolt(name2, new StdoutPrinterBolt(x, y)).shuffleGrouping(name3);
                mergeInputs.add(name3);
            }
        }

        BoltDeclarer mergingBolt = builder.setBolt("merging", new MergingBolt()).setMaxTaskParallelism(1);
        for (String input : mergeInputs) {
            mergingBolt.shuffleGrouping(input);
        }

        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);
        config.put(Config.LOGVIEWER_APPENDER_NAME, "org.apache.log4j.varia.NullAppender");
        config.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", config, builder.createTopology());

        LwjglApplicationConfiguration cfg = new LwjglApplicationConfiguration();
        cfg.title = "hello-world";
        cfg.width = 900;
        cfg.height = 900;

        new LwjglApplication(new Visualizer(), cfg);

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
