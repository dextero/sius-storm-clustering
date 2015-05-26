package pl.edu.agh.sius.clustering;

public class CharacteristicVector {
    enum Status {
        Sporadic,
        Normal
    }

    Status status;
    double timeLastUpdated;
    double timeLastRemovedSporadic;
    double density;
    int[] position;
    DoubleRange[] boundingBox;
}
