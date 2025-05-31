package org.vanilladb.core.storage.index.ivf;


import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;



public class KMeans {

    public static List<float[]> train(List<float[]> data, int k, int maxIter) {
        List<float[]> centroids = initRandomCentroids(data, k);
        Map<Integer, List<float[]>> clusters = new HashMap<>();

        for (int iter = 0; iter < maxIter; iter++) {
            clusters.clear();
            for (int i = 0; i < k; i++) clusters.put(i, new ArrayList<>());

            for (float[] point : data) {
                int nearest = getNearestCentroid(point, centroids);
                clusters.get(nearest).add(point);
            }

            for (int i = 0; i < k; i++) {
                if (!clusters.get(i).isEmpty()) {
                    centroids.set(i, computeMean(clusters.get(i)));
                }
            }
        }
        return centroids;
    }

    private static int getNearestCentroid(float[] point, List<float[]> centroids) {
        int best = -1;
        double bestDist = Double.MAX_VALUE;
        for (int i = 0; i < centroids.size(); i++) {
            double dist = l2Distance(point, centroids.get(i));
            if (dist < bestDist) {
                bestDist = dist;
                best = i;
            }
        }
        return best;
    }

    private static float[] computeMean(List<float[]> cluster) {
        int dim = cluster.get(0).length;
        float[] mean = new float[dim];
        for (float[] v : cluster)
            for (int i = 0; i < dim; i++)
                mean[i] += v[i];
        for (int i = 0; i < dim; i++) mean[i] /= cluster.size();
        return mean;
    }

    private static double l2Distance(float[] a, float[] b) {
        double dist = 0;
        for (int i = 0; i < a.length; i++)
            dist += Math.pow(a[i] - b[i], 2);
        return dist;
    }

    private static List<float[]> initRandomCentroids(List<float[]> data, int k) {
        Collections.shuffle(data);
        return new ArrayList<>(data.subList(0, k));
    }
}
