package IndexedDBSCAN;

/**
 * Created by XXX on 4/22/XXX.
 */


import ca.pfv.spmf.algorithms.clustering.dbscan.DoubleArrayDBS;
import ca.pfv.spmf.patterns.cluster.DoubleArray;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.ArrayUtils;

/* This file is copyright (c) 2008-2015 Philippe Fournier-Viger
*
* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
*
* SPMF is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
*/

/**
 * This class represents a vector of double values used by the DBScan algorithm.
 * It has a "visited" flag to remember the node that have been already visited.
 *
 * @author Philippe Fournier-Viger
 */
public class MyDoubleArrayDBS extends DoubleArrayDBS implements KryoSerializable{

    boolean visited = false;

    /**
     * Constructor
     * @param data an array of double values
     */
    public MyDoubleArrayDBS(double[] data) {
        super(data);
    }

    public MyDoubleArrayDBS(Double[] data) {
        super(ArrayUtils.toPrimitive(data));
    }

    @Override
    public void write(Kryo kryo, Output output) {

    }

    @Override
    public void read(Kryo kryo, Input input) {

    }
}

