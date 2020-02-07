package LBSNAnalysis;

/**
 * Created by XXX on 4/18/XXX.
 */

import net.sf.javaml.core.Instance;
import net.sf.javaml.distance.AbstractDistance;

public class distanceAmongCoords extends AbstractDistance {
    public distanceAmongCoords() {
    }

    public static double distFrom(double lat1, double lng1, double lat2, double lng2) {
        double earthRadius = 6371000; //meters
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLng / 2) * Math.sin(dLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double dist = (double) (earthRadius * c);

        return dist;
    }

    public double measure(Instance x, Instance y) {
        if (x.noAttributes() != y.noAttributes()) {
            throw new RuntimeException("Both instances should contain the same number of values.");
        } else {
            double sum = 0.0D;
            sum = distFrom(x.value(0), x.value(1), y.value(0), y.value(1));

            for (int i = 0; i < x.noAttributes(); ++i) {
                sum += Math.abs(x.value(i) - y.value(i));
            }

            return sum;
        }
    }
}
