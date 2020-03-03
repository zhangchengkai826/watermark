package io.github.zhangchengkai826.watermark.function;

import java.util.List;
import java.util.function.Predicate;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class DecodingThresholdEvaluatingFunctionA extends DecodingThresholdEvaluatingFunction {
    @Override
    public double apply(List<Double> minValues, List<Double> maxValues) {
        final int numEncodedBits = minValues.size() + maxValues.size();
        final double p0 = (double) minValues.size() / numEncodedBits;
        final double p1 = 1 - p0;

        SummaryStatistics ss = new SummaryStatistics();
        for (double x : minValues)
            ss.addValue(x);
        final double mean0 = ss.getMean();
        final double stdev0 = ss.getStandardDeviation();
        final double var0 = ss.getVariance();

        ss.clear();
        for (double x : maxValues)
            ss.addValue(x);
        final double mean1 = ss.getMean();
        final double stdev1 = ss.getStandardDeviation();
        final double var1 = ss.getVariance();

        final double a = (var0 - var1) / (2 * var0 * var1);
        final double b = (mean0 * var1 - mean1 * var0) / (var0 * var1);
        final double c = Math.log((p0 * stdev1) / (p1 * stdev0))
                + (mean1 * mean1 * var0 - mean0 * mean0 * var1) / (2 * var0 * var1);
        final double deltaRoot = Math.sqrt(b * b - 4 * a * c);

        final double root0 = (-b - deltaRoot) / (2 * a);
        final double root1 = (-b + deltaRoot) / (2 * a);

        final Predicate<Double> secondOrderTest = root -> {
            final double m0 = root - mean0;
            final double m1 = root - mean1;
            final double secondOrderEst = p1 * m1 / (var1 * stdev1) * Math.exp(-m1 * m1 / (2 * var1))
                    - p0 * m0 / (var0 * stdev0) * Math.exp(-m0 * m0 / (2 * var0));
            return secondOrderEst > 0;
        };

        return secondOrderTest.test(root0) ? root0 : root1;
    }
}