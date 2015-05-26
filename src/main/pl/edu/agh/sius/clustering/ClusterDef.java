package pl.edu.agh.sius.clustering;

import java.io.Serializable;
import java.util.Random;

public class ClusterDef implements Serializable {
    private final double[] mean;
    private final double[] variance;
    private final Random rng;

    public ClusterDef(double[] mean,
                      double[] variance) {
        assert mean.length == variance.length;

        this.mean = mean;
        this.variance = variance;
        this.rng = new Random();
    }

    public double[] generatePoint() {
        double[] ret = new double[this.mean.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = rng.nextGaussian() * variance[i] + mean[i];
        }
        return ret;
    }
}
