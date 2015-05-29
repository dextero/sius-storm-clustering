package pl.edu.agh.sius.clustering.bolt;

import java.util.Arrays;

class PositionWrapper {
    int[] pos;

    public PositionWrapper(int[] pos) {
        this.pos = pos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PositionWrapper that = (PositionWrapper) o;

        return Arrays.equals(pos, that.pos);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(pos);
    }

    @Override
    public String toString() {
        return Arrays.toString(pos);
    }
}
