package LBSNAnalysis;

import org.apache.commons.math3.exception.DimensionMismatchException;
import org.apache.commons.math3.ml.distance.DistanceMeasure;

/**
 * Created by XXX on 4/21/2016.
 */
public class distanceAmongCoordsDM implements DistanceMeasure {
    public double compute(double[] first, double[] second)
            throws DimensionMismatchException {
        //double lat1=first[0];
        //double lng1=first[1];
        //double lat2=second[0];
        //double lng2=second[1];

        double earthRadius = 6371000; //meters
        double dLat = Math.toRadians(second[0] - first[0]);//double dLat = Math.toRadians(lat2-lat1);
        double dLng = Math.toRadians(second[1] - first[1]);//double dLng = Math.toRadians(lng2-lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(first[0])) * Math.cos(Math.toRadians(second[0])) *
                        Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double dist = (earthRadius * c);

        return dist;
        //return MathArrays.distance(a, b);
    }
}
