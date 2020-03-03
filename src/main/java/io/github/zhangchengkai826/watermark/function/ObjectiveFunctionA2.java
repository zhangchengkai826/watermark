package io.github.zhangchengkai826.watermark.function;

import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class ObjectiveFunctionA2 extends ObjectiveFunctionA1 {
    @Override
    public double apply(List<Double> dataVec) {
        SummaryStatistics ss = new SummaryStatistics();
        for (double data : dataVec)
            ss.addValue(data);
        double ref = ss.getMean() + stdevMultipiler * ss.getStandardDeviation();
        return dataVec.stream().filter(d -> d >= ref).reduce(0.0, (result, elem) -> result + elem);
    }
}