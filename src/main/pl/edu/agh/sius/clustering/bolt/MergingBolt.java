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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MergingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<PositionWrapper, Double> cubes = new HashMap<>();
    private int counter = 0;
    private int dimSize;

    public static final int MESSAGES_PER_UPDATE = 100;

    public MergingBolt(int dimSize) {
        this.dimSize = dimSize;
    }

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

    public static final double DENSITY_MIN = 1.1;

    private List<List<PositionWrapper>> initialClustering() {
        // TODO: optimize
        List<List<PositionWrapper>> clusters = new ArrayList<>();

        for (Map.Entry<PositionWrapper, Double> cube: cubes.entrySet()) {
            PositionWrapper currPos = cube.getKey();
            double density = cube.getValue();

            if (density < DENSITY_MIN) {
                continue;
            }

            List<Integer> clustersToMerge = new ArrayList<>();

            for (int dx = -1; dx <= 1; ++dx) {
                for (int dy = -1; dy <= 1; dy++) {
                    PositionWrapper nbrPos = new PositionWrapper(new int[] {
                            currPos.pos[0] + dx,
                            currPos.pos[1] + dy
                    });

                    for (int i = 0; i < clusters.size(); ++i) {
                        List<PositionWrapper> cluster = clusters.get(i);
                        if (cluster.indexOf(nbrPos) != -1) {
                            clustersToMerge.add(i);
                            break;
                        }
                    }
                }
            }

            List<PositionWrapper> newCluster = new ArrayList<>();
            newCluster.add(currPos);
            List<List<PositionWrapper>> newClusters = new ArrayList<>();
            newClusters.add(newCluster);
            for (int i = 0; i < clusters.size(); i++) {
                if (clustersToMerge.contains(i)) {
                    newCluster.addAll(clusters.get(i));
                } else {
                    newClusters.add(clusters.get(i));
                }
            }

            clusters = newClusters;
        }

        return clusters;
    }

    private void update() {
        List<List<PositionWrapper>> clusters = initialClustering();

        StringBuilder builder = new StringBuilder();
        for (List<PositionWrapper> cluster: clusters) {
            builder.append("cluster:");
            for (PositionWrapper position : cluster) {
                builder.append(" ").append(position).append(" = ").append(cubes.get(position));
            }
            builder.append("\n");
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
