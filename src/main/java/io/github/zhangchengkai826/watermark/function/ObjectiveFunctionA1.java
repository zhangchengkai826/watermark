package io.github.zhangchengkai826.watermark.function;

import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class ObjectiveFunctionA1 extends ObjectiveFunction {
    // It's in the range (0, 1)
    static final double STDEV_MULTIPLIER = 0.35;
    double stdevMultipiler = STDEV_MULTIPLIER;

    @Override
    public double apply(List<Double> dataVec) {
        SummaryStatistics ss = new SummaryStatistics();
        for (double data : dataVec)
            ss.addValue(data);
        double ref = ss.getMean() + stdevMultipiler * ss.getStandardDeviation();
        long numNotBelow = dataVec.stream().filter(d -> d >= ref).count();
        return (double) numNotBelow / dataVec.size();
    }
}