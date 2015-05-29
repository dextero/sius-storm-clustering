package pl.edu.agh.sius.clustering;

public class ColoredPoint {
    public double[] pos;
    public int cluster;

    public ColoredPoint(double[] pos,
                        int cluster) {
        this.pos = pos;
        this.cluster = cluster;
    }
}
