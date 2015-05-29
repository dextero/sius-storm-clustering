package pl.edu.agh.sius.clustering;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;

class StdoutPrinterBolt extends BaseRichBolt {
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
            Main.finished = true;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
