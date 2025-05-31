package org.vanilladb.core.sql.distfn;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorSpecies;
import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.VectorOperators;


public class EuclideanFn extends DistanceFn {

    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        float[] queryArr = query.asJavaVal();
        float[] targetArr = vec.asJavaVal();
        int len = vec.dimension();

        if (len != queryArr.length)
            throw new IllegalArgumentException("Vector dimensions do not match");

        // SIMD path
        int i = 0;
        FloatVector sumVec = FloatVector.zero(SPECIES);
        for (; i <= len - SPECIES.length(); i += SPECIES.length()) {
            FloatVector va = FloatVector.fromArray(SPECIES, queryArr, i);
            FloatVector vb = FloatVector.fromArray(SPECIES, targetArr, i);
            FloatVector diff = va.sub(vb);
            sumVec = sumVec.add(diff.mul(diff));
        }

        float simdSum = sumVec.reduceLanes(VectorOperators.ADD);
        
        // Fallback for remaining elements
        for (; i < len; i++) {
            float diff = queryArr[i] - targetArr[i];
            simdSum += diff * diff;
        }

        return Math.sqrt(simdSum);
    }
}    
