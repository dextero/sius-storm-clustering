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

import java.util.*;
import java.util.stream.Collectors;

public class MergingBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<PositionWrapper, CharacteristicVector> grids = new HashMap<>();
    private int counter = 0;
    private long timestamp = 0;
    private Map<PositionWrapper, CharacteristicVector.Density> lastAdjustDensities = new HashMap<>();
    private Map<Integer, List<PositionWrapper>> clusters = null;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        CharacteristicVector cv = (CharacteristicVector) tuple.getValue(0);
        timestamp = tuple.getLong(1);

        grids.put(new PositionWrapper(cv.position), cv);
        if (counter++ >= Constants.MESSAGES_PER_UPDATE) {
            update();
        }
    }

    private Map<Integer, List<PositionWrapper>> initialClustering() {
        // TODO: optimize
        Map<Integer, List<PositionWrapper>> clusters = new HashMap<>();

        for (Map.Entry<PositionWrapper, CharacteristicVector> grid: grids.entrySet()) {
            PositionWrapper currPos = grid.getKey();
            CharacteristicVector curr = grid.getValue();

            if (curr.density < Constants.DENSITY_MIN) {
                continue;
            }

            List<Integer> clustersToMerge = new ArrayList<>();

            for (PositionWrapper nbrPos : neighbors(grids.keySet(), currPos)) {
                int cluster = grids.get(nbrPos).cluster;
                if (cluster != CharacteristicVector.NO_CLASS
                        && cluster != curr.cluster) {
                    clustersToMerge.add(cluster);
                    break;
                }
            }

            Map<Integer, List<PositionWrapper>> newClusters = new HashMap<>();

            int newClusterId = nextId(newClusters);

            List<PositionWrapper> newCluster = new ArrayList<>();
            newCluster.add(currPos);
            curr.cluster = newClusterId;

            newClusters.put(newClusterId, newCluster);

            for (Map.Entry<Integer, List<PositionWrapper>> indexCluster : clusters.entrySet()) {
                int index = indexCluster.getKey();
                List<PositionWrapper> cluster = indexCluster.getValue();

                if (clustersToMerge.contains(index)) {
                    newCluster.addAll(clusters.get(index));
                    clusters.get(index)
                            .forEach(p -> grids.get(p).cluster = newClusterId);
                } else {
                    int newId = nextId(newClusters);
                    newClusters.put(newId, cluster);
                    cluster.forEach(p -> grids.get(p).cluster = newId);
                }
            }

            clusters = newClusters;
        }

        return clusters;
    }

    private List<PositionWrapper> neighbors(Set<PositionWrapper> grids,
                                            PositionWrapper center) {
        List<PositionWrapper> nbrs = new ArrayList<>();

        for (int x = -1; x <= 1; ++x) {
            if (center.pos[0] + x >= 0 && center.pos[0] + x < Constants.TOTAL_CUBES_PER_DIM) {
                for (int y = -1; y <= 1; ++y) {
                    if (x == 0 && y == 0) {
                        continue;
                    }
                    if (center.pos[1] + y >= 0 && center.pos[1] + y < Constants.TOTAL_CUBES_PER_DIM) {
                        PositionWrapper pos = new PositionWrapper(new int[]{center.pos[0] + x, center.pos[1] + y});
                        if (grids.contains(pos)) {
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
            return 0;
        }
        if (!clusters.containsKey(cluster)) {
            System.err.println("wat");
        }
        return clusters.get(cluster).size();
    }

    private void mergeCluster(int oldCluster,
                              int newClusterIdx) {
        if (oldCluster == newClusterIdx) {
            throw new ThisShouldNeverHappenException();
        }
        for (PositionWrapper p : clusters.get(oldCluster)) {
            grids.get(p).cluster = newClusterIdx;

        }
        clusters.get(newClusterIdx).addAll(clusters.get(oldCluster));
        clusters.remove(oldCluster);
    }

    private boolean willBeOutsideGridIfWeAddAnotherNeighborToItsCluster(PositionWrapper outsideGridPos,
                                                                        int outsideGridCluster) {
        // TODO: are diagonal grids neighbors too?
        return neighbors(grids.keySet(), outsideGridPos).stream()
                .filter(p -> grids.get(p).cluster == outsideGridCluster)
                .count() < 7;

    }

    private static class Pair<A, B> {
        public A first;
        public B second;

        public Pair(A first,
                    B second) {
            this.first = first;
            this.second = second;
        }
    }

    private void ensureClusterIdsOk() {
        for (CharacteristicVector cv : grids.values()) {
            if (cv.cluster != -1
                    && (!clusters.containsKey(cv.cluster)
                        || !clusters.get(cv.cluster).contains(new PositionWrapper(cv.position)))) {
                System.err.println("asdasd");
            }
        }
    }

    private void adjustClustering() {
        Map<PositionWrapper, CharacteristicVector.Density> currDensities = new HashMap<>();

        for (Map.Entry<PositionWrapper, CharacteristicVector> grid : grids.entrySet()) {
            CharacteristicVector.Density densityLevel = grid.getValue().getDensityLevel();
            PositionWrapper currPos = grid.getKey();
            CharacteristicVector curr = grid.getValue();
            currDensities.put(currPos, densityLevel);

            if (densityLevel != lastAdjustDensities.get(currPos)
                    && curr.cluster != CharacteristicVector.NO_CLASS) {
                int clusterIdx = grids.get(currPos).cluster;
                List<PositionWrapper> cluster = clusters.get(clusterIdx);
                removeFromCluster(currPos, clusterIdx);
                if (!cluster.isEmpty()) {
                    ensureClusterConnected(cluster, clusters);
                }
            } else if (densityLevel == CharacteristicVector.Density.Dense) {
                List<PositionWrapper> neighbors = neighbors(grids.keySet(), currPos);

                if (neighbors.isEmpty()
                        || neighbors.stream().allMatch(pos -> grids.get(pos).cluster == CharacteristicVector.NO_CLASS)) {
                    curr.cluster = nextId(clusters);
                    List<PositionWrapper> newCluster = new ArrayList<>();
                    newCluster.add(currPos);
                    clusters.put(curr.cluster, newCluster);
                    continue;
                }

                PositionWrapper nbrPos = getNeighborWithBiggestCluster(curr, neighbors);
                if (nbrPos == null) {
                    continue;
                }

                CharacteristicVector nbr = grids.get(nbrPos);
                CharacteristicVector.Density nbrDensity = nbr.getDensityLevel();
                if (nbrDensity == CharacteristicVector.Density.Dense) {
                    if (curr.cluster == CharacteristicVector.NO_CLASS) {
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
            } else if (densityLevel == CharacteristicVector.Density.Transitional) {
                // TODO
            }
        }

        lastAdjustDensities = currDensities;
    }

    private PositionWrapper getNeighborWithBiggestCluster(CharacteristicVector curr,
                                                          List<PositionWrapper> neighbors) {
        PositionWrapper nbrPos = null;
        int nbrClusterSize = -1;
        for (PositionWrapper neighborPos : neighbors) {
            int nbrClusterIdx = grids.get(neighborPos).cluster;
            if (nbrClusterIdx != curr.cluster) {
                int currNbrClusterSize = clusterSize(nbrClusterIdx);
                if (currNbrClusterSize > nbrClusterSize) {
                    nbrPos = neighborPos;
                    nbrClusterSize = currNbrClusterSize;
                }
            }
        }
        return nbrPos;
    }

    private void assignGridToCluster(PositionWrapper pos,
                                     CharacteristicVector grid,
                                     int cluster) {
        if (grid.cluster != CharacteristicVector.NO_CLASS) {
            removeFromCluster(pos, grid.cluster);
        }

        grid.cluster = cluster;
        clusters.get(cluster).add(pos);
    }

    private void removeFromCluster(PositionWrapper pos,
                                   int clusterIdx) {
        List<PositionWrapper> cluster = clusters.get(clusterIdx);
        if (!cluster.contains(pos)) {
            System.err.println("wat");
        }
        cluster.remove(pos);
        grids.get(pos).cluster = CharacteristicVector.NO_CLASS;

        if (cluster.isEmpty()) {
            clusters.remove(clusterIdx);
        }
    }

    private void ensureClusterConnected(List<PositionWrapper> cluster,
                                        Map<Integer, List<PositionWrapper>> clusters) {
        Map<PositionWrapper, Boolean> gridVisited = new HashMap<>();
        for (PositionWrapper pos : cluster) {
            gridVisited.put(pos, false);
        }

        Queue<PositionWrapper> queue = new ArrayDeque<>();
        queue.add(cluster.get(0));
        gridVisited.put(cluster.get(0), true);
        int numVisited = 1;

        while (!queue.isEmpty()) {
            for (PositionWrapper nbrPos : neighbors(gridVisited.keySet(), queue.poll())) {
                if (!gridVisited.get(nbrPos)) {
                    queue.add(nbrPos);
                    gridVisited.put(nbrPos, true);
                    ++numVisited;
                }
            }
        }

        if (numVisited != cluster.size()) {
            List<PositionWrapper> newCluster = new ArrayList<>();

            for (Map.Entry<PositionWrapper, Boolean> e : gridVisited.entrySet()) {
                if (!e.getValue()) {
                    newCluster.add(e.getKey());
                    cluster.remove(e.getKey());
                }
            }

            int newClusterIdx = nextId(clusters);
            clusters.put(newClusterIdx, newCluster);
            newCluster.forEach(pos -> grids.get(pos).cluster = newClusterIdx);
        }
   }

    private int nextId(Map<Integer, ?> map) {
        int id = map.size();
        while (map.containsKey(id)) {
            ++id;
        }
        return id;
    }

    private void update() {
        if (clusters == null) {
            clusters = initialClustering();
        } else {
//            removeSporaticGrids();
            adjustClustering();
        }

        Visualizer.clusters = clusters.values().stream().collect(Collectors.toList());

//        StringBuilder builder = new StringBuilder();
//        for (List<PositionWrapper> cluster: clusters) {
//            builder.append("cluster:");
//            for (PositionWraspper position : cluster) {
//                builder.append(" ").append(position).append(" = ").append(grids.get(position));
//            }
//            builder.append("\n");
//        }
//        System.err.println(builder.toString());
        counter = 0;
    }

    private void removeSporaticGrids() {
        Iterator<Map.Entry<PositionWrapper, CharacteristicVector>> iterator = grids.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<PositionWrapper, CharacteristicVector> posGrid = iterator.next();
            CharacteristicVector grid = posGrid.getValue();

            if (grid.status == CharacteristicVector.Status.Sporadic) {
                if (grid.timeLastUpdated == grid.timeLastRemovedSporadic) {
                    iterator.remove();
                    continue;
                } else {
                    grid.status = CharacteristicVector.Status.Normal;
                }
            }

            double densityThreshold = Constants.LOW_THRESHOLD_PARAMETER * (1 - Math.pow(Constants.DECAY_FACTOR, timestamp - grid.timeLastUpdated + 1))
                    / Constants.TOTAL_CUBES_PER_SPACE * (1 - Constants.DECAY_FACTOR);

            if (grid.density < densityThreshold) {
                if (timestamp >= (1 + Constants.SPORADIC_GRID_DELETE_CONSTANT) * grid.timeLastRemovedSporadic) {
                    grid.status = CharacteristicVector.Status.Sporadic;
                    grid.timeLastRemovedSporadic = grid.timeLastUpdated;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clusters"));
    }
}
