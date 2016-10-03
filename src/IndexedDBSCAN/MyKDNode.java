package IndexedDBSCAN;

/**
 * Created by MAamir on 4/22/2016.
 */

import ca.pfv.spmf.patterns.cluster.DoubleArray;

class MyKDNode {

    DoubleArray values;  // contains a vector
    int d;  // a dimension
    MyKDNode above;  // node above
    MyKDNode below;  // node below

    /**
     * Constructor
     *
     * @param doubleArray a vector
     * @param d           a dimension
     */
    public MyKDNode(DoubleArray doubleArray, int d) {
        this.values = doubleArray;
        this.d = d;
    }


}
