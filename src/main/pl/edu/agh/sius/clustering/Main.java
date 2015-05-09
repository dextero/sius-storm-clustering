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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Main {
    private static boolean finished;

    public static class IntegerSourceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private int currIndex;
        private static List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("number", "finished"));
        }

        public void open(Map map,
                         TopologyContext topologyContext,
                         SpoutOutputCollector spoutOutputCollector) {
            collector = spoutOutputCollector;
        }

        public void nextTuple() {
            if (currIndex < values.size()) {
                collector.emit(new Values(values.get(currIndex),
                                          currIndex >= values.size() - 1));
                ++currIndex;
            }
        }
    }

    public static class IntegerDoublerBolt extends BaseRichBolt {
        private OutputCollector collector;

        public void prepare(Map map,
                            TopologyContext topologyContext,
                            OutputCollector outputCollector) {
            collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            collector.emit(new Values(tuple.getInteger(0) * 2, tuple.getBoolean(1)));
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("double_number", "finished"));
        }
    }

    private static class StdoutPrinterBolt extends BaseRichBolt {
        public void prepare(Map map,
                            TopologyContext topologyContext,
                            OutputCollector outputCollector) {
        }

        public void execute(Tuple tuple) {
            System.out.println("result: " + tuple.getInteger(0));
            Main.finished = tuple.getBoolean(1);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        StdoutPrinterBolt printer = new StdoutPrinterBolt();

        builder.setSpout("source", new IntegerSourceSpout());
        builder.setBolt("doubler", new IntegerDoublerBolt()).shuffleGrouping("source");
        builder.setBolt("output", printer).shuffleGrouping("doubler");

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
