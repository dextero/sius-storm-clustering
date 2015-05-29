package pl.edu.agh.sius.clustering;

import java.util.Random;

public class VectorUtils {
    public static int[] divFloor(double[] a, double[] b) {
        assert a.length == b.length;

        int[] ret = new int[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = ((int) ((a[i] / b[i])));
        }
        return ret;
    }

    public static double[] random(Random rng,
                                  int dim,
                                  double max) {
        double[] ret = new double[dim];
        for (int i = 0; i < dim; i++) {
            ret[i] = rng.nextDouble() * max - (max * 0.5);
        }
        return ret;
    }
}
