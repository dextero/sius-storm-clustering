package pl.edu.agh.sius.clustering.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import pl.edu.agh.sius.clustering.CharacteristicVector;
import pl.edu.agh.sius.clustering.VectorUtils;

import java.util.HashMap;
import java.util.Map;

public class DStreamBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final Map<int[], CharacteristicVector> cubes = new HashMap<>();
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
        int[] cubePos = VectorUtils.divFloor(point, cubeSize);

        CharacteristicVector cv = getOrCreateCharacteristicVector(cubePos);
        // TODO: update
        collector.ack(tuple);
    }

    private CharacteristicVector getOrCreateCharacteristicVector(int[] cubePos) {
        CharacteristicVector cv = cubes.get(cubePos);
        if (cv == null) {
            cv = new CharacteristicVector();
            cubes.put(cubePos, cv);
        }
        return cv;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // TODO
    }
}
