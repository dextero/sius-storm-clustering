package pl.edu.agh.sius.clustering.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import pl.edu.agh.sius.clustering.CharacteristicVector;
import pl.edu.agh.sius.clustering.Constants;
import pl.edu.agh.sius.clustering.PositionWrapper;
import pl.edu.agh.sius.clustering.visualizer.Visualizer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MergingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<PositionWrapper, CharacteristicVector> cubes = new HashMap<>();
    private int counter = 0;
    private Map<PositionWrapper, CharacteristicVector.Density> lastAdjustDensities = new HashMap<>();
    private Map<Integer, List<PositionWrapper>> clusters = new HashMap<>();

    public static final int MESSAGES_PER_UPDATE = 1000;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        CharacteristicVector cv = (CharacteristicVector) tuple.getValue(0);

        cubes.put(new PositionWrapper(cv.position), cv);
        if (counter++ >= MESSAGES_PER_UPDATE) {
            update();
        }
    }

    private Map<Integer, List<PositionWrapper>> initialClustering() {
        // TODO: optimize
        Map<Integer, List<PositionWrapper>> clusters = new HashMap<>();

        for (Map.Entry<PositionWrapper, CharacteristicVector> cube: cubes.entrySet()) {
            PositionWrapper currPos = cube.getKey();
            double density = cube.getValue().density;

            if (density < Constants.DENSITY_MIN) {
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
            Map<Integer, List<PositionWrapper>> newClusters = new HashMap<>();
            newClusters.put(0, newCluster);

            for (int i = 0; i < clusters.size(); i++) {
                if (clustersToMerge.contains(i)) {
                    newCluster.addAll(clusters.get(i));
                } else {
                    newClusters.put(clusters.size(), clusters.get(i));
                }
            }

            clusters = newClusters;
        }

        return clusters;
    }

    private List<PositionWrapper> neighbors(PositionWrapper center) {
        List<PositionWrapper> nbrs = new ArrayList<>();

        for (int x = -1; x <= 1; ++x) {
            if (center.pos[0] + x >= 0 && center.pos[0] + x < Constants.DIM_SIZE + Constants.CUBE_SIZE) {
                for (int y = -1; y <= 1; ++y) {
                    if (x == center.pos[0] && y == center.pos[1]) {
                        continue;
                    }
                    if (center.pos[1] + y >= 1 && center.pos[1] + y < Constants.DIM_SIZE + Constants.CUBE_SIZE) {
                        PositionWrapper pos = new PositionWrapper(new int[]{x, y});
                        if (cubes.containsKey(pos)) {
                            nbrs.add(pos);
                        }
                    }
                }
            }
        }

        return nbrs;
    }

    private int clusterSize(int cluster) {
        if (cluster == CharacteristicVector.NO_CLASS) {
            return 1;
        }
        return clusters.get(cluster).size();
    }

    private void mergeCluster(int oldCluster,
                              int newClusterIdx) {
        for (PositionWrapper p : clusters.get(oldCluster)) {
            cubes.get(p).cluster = newClusterIdx;

        }
        clusters.get(newClusterIdx).addAll(clusters.get(oldCluster));
        clusters.remove(oldCluster);
    }

    private boolean willBeOutsideGridIfWeAddAnotherNeighborToItsCluster(PositionWrapper outsideGridPos,
                                                                        int outsideGridCluster) {
        // TODO: are diagonal grids neighbors too?
        return neighbors(outsideGridPos).stream()
                .filter(p -> cubes.get(p).cluster == outsideGridCluster)
                .count() < 7;

    }

    private void adjustClustering() {
        Map<PositionWrapper, CharacteristicVector.Density> currDensities = new HashMap<>();

        for (Map.Entry<PositionWrapper, CharacteristicVector> cube : cubes.entrySet()) {
            CharacteristicVector.Density densityLevel = cube.getValue().getDensityLevel();
            PositionWrapper currPos = cube.getKey();
            CharacteristicVector curr = cube.getValue();
            currDensities.put(currPos, densityLevel);

            if (densityLevel != lastAdjustDensities.get(currPos)) {
                List<PositionWrapper> cluster = clusters.get(cubes.get(currPos).cluster);
                cluster.remove(currPos);

                ensureClusterConnected(cluster, clusters);
            } else if (densityLevel == CharacteristicVector.Density.Dense) {
                PositionWrapper nbrPos = neighbors(currPos).get(0);
                // TODO: find h, assign to nbr
                CharacteristicVector nbr = cubes.get(nbrPos);
                CharacteristicVector.Density nbrDensity = nbr.getDensityLevel();
                if (nbrDensity == CharacteristicVector.Density.Dense) {
                    if (curr.cluster != CharacteristicVector.NO_CLASS) {
                        assignGridToCluster(currPos, curr, nbr.cluster);
                    } else if (nbr.cluster != CharacteristicVector.NO_CLASS) {
                        if (clusterSize(curr.cluster) > clusterSize(nbr.cluster)) {
                            mergeCluster(nbr.cluster, curr.cluster);
                        } else {
                            mergeCluster(curr.cluster, nbr.cluster);
                        }
                    }
                } else if (nbrDensity == CharacteristicVector.Density.Transitional) {
                    if (curr.cluster == CharacteristicVector.NO_CLASS
                            && willBeOutsideGridIfWeAddAnotherNeighborToItsCluster(nbrPos, nbr.cluster)) {
                        assignGridToCluster(currPos, curr, nbr.cluster);
                    } else if (clusterSize(curr.cluster) >= clusterSize(nbr.cluster)) {
                        assignGridToCluster(nbrPos, nbr, curr.cluster);
                    }
                }
            }
        }
    }

    private void assignGridToCluster(PositionWrapper pos,
                                     CharacteristicVector grid,
                                     int cluster) {
        if (grid.cluster != CharacteristicVector.NO_CLASS) {
            clusters.get(grid.cluster).remove(pos);
        }

        grid.cluster = cluster;
        clusters.get(cluster).add(pos);
    }

    private void ensureClusterConnected(List<PositionWrapper> cluster,
                                        Map<Integer, List<PositionWrapper>> clusters) {
        throw new NotImplementedException();
    }

    private void update() {
        if (clusters == null) {
            clusters = initialClustering();
        } else {
            adjustClustering();
        }
        Visualizer.clusters = clusters.values().stream().collect(Collectors.toList());

//        StringBuilder builder = new StringBuilder();
//        for (List<PositionWrapper> cluster: clusters) {
//            builder.append("cluster:");
//            for (PositionWraspper position : cluster) {
//                builder.append(" ").append(position).append(" = ").append(cubes.get(position));
//            }
//            builder.append("\n");
//        }
//        System.err.println(builder.toString());
        counter = 0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clusters"));
    }
}
