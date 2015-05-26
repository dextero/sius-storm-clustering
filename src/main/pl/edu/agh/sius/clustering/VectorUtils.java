package pl.edu.agh.sius.clustering;

public class VectorUtils {
    public static int[] divFloor(double[] a, double[] b) {
        assert a.length == b.length

        int[] ret = new int[a.length];
        for (int i = 0; i < a.length; i++) {
            ret[i] = ((int) ((a[i] / b[i])));
        }
        return ret;
    }
}
