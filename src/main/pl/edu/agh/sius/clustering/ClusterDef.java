package pl.edu.agh.sius.clustering;

import java.io.Serializable;
import java.util.Random;

public class ClusterDef implements Serializable {
    private double[] mean;
    private final double[] variance;
    private final Random rng;

    public ClusterDef(double[] mean,
                      double[] variance) {
        assert mean.length == variance.length;

        this.mean = mean;
        this.variance = variance;
        this.rng = new Random(0);
    }

    public double[] generatePoint() {
        double[] ret = new double[this.mean.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = rng.nextGaussian() * variance[i] + mean[i];
        }
        return ret;
    }

    public void randomize(double moveFactor,
                          double sizeFactor) {
        for (int i = 0; i < mean.length; i++) {
            mean[i] = Math.min(Math.max(Constants.DIMENSION_SIZE * 0.05,
                                        mean[i] + rng.nextGaussian() * moveFactor),
                               Constants.DIMENSION_SIZE * 0.95);
            variance[i] = Math.min(Math.max(Constants.MIN_VARIANCE,
                                            variance[i] + rng.nextGaussian() * sizeFactor),
                                   Constants.MAX_VARIANCE);
        }
    }
}
