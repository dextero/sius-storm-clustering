package pl.edu.agh.sius.clustering.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import pl.edu.agh.sius.clustering.CharacteristicVector;
import pl.edu.agh.sius.clustering.DoubleRange;
import pl.edu.agh.sius.clustering.Main;

import java.util.HashMap;
import java.util.Map;

public class MergingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<PositionWrapper, Double> cubes = new HashMap<>();
    private int counter = 0;

    public static final int MESSAGES_PER_UPDATE = 100;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        CharacteristicVector cv = (CharacteristicVector) tuple.getValue(0);

        cubes.put(new PositionWrapper(cv.position), cv.density);
        if (counter++ >= MESSAGES_PER_UPDATE) {
            update();
        }
    }

    private void update() {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<PositionWrapper, Double> cube: cubes.entrySet()) {
            builder.append("pos: ").append(cube.getKey()).append(" = ").append(cube.getValue()).append("\n");
        }
        System.err.println(builder.toString());
        counter = 0;

        Main.finished = true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clusters"));
    }
}
