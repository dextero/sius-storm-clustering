package pl.edu.agh.sius.clustering.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import pl.edu.agh.sius.clustering.CharacteristicVector;
import pl.edu.agh.sius.clustering.PositionWrapper;
import pl.edu.agh.sius.clustering.VectorUtils;

import java.util.HashMap;
import java.util.Map;

public class DStreamBolt extends BaseRichBolt {
    private OutputCollector collector;

    private final Map<PositionWrapper, CharacteristicVector> cubes = new HashMap<>();
    private final double[] cubeSize;

    public DStreamBolt(double[] cubeSize) {
        this.cubeSize = cubeSize;
    }

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        double[] point = (double[]) tuple.getValue(0);
        long timestamp = tuple.getLong(1);
        int[] cubePos = VectorUtils.divFloor(point, cubeSize);

        CharacteristicVector cv = getOrCreateCharacteristicVector(cubePos);
        cv.update(timestamp);

        collector.ack(tuple);
        collector.emit(new Values(cv.clone(), timestamp));
    }

    private CharacteristicVector getOrCreateCharacteristicVector(int[] cubePos) {
        PositionWrapper pos = new PositionWrapper(cubePos);
        CharacteristicVector cv = cubes.get(pos);
        if (cv == null) {
            cv = new CharacteristicVector(cubePos);
            cubes.put(pos, cv);
        }
        return cv;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("cv", "timestamp"));
    }
}
