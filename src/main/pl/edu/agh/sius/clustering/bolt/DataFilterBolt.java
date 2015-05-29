package pl.edu.agh.sius.clustering.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import pl.edu.agh.sius.clustering.DoubleRange;

import java.util.Map;

public class DataFilterBolt extends BaseRichBolt {
    DoubleRange[] forwardRange;
    OutputCollector collector;

    public DataFilterBolt(DoubleRange[] forwardRange) {
        assert forwardRange != null;

        this.forwardRange = forwardRange;
    }

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);

        double[] point = (double[]) tuple.getValue(0);
        for (int i = 0; i < point.length; i++) {
            if (point[i] < forwardRange[i].min || point[i] >= forwardRange[i].max) {
                return;
            }
        }

        collector.emit(new Values(point, tuple.getLong(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("point", "timestamp"));
    }
}
