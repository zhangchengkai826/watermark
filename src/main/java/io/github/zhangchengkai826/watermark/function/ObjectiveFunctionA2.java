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
        double above = 0,  notAbove = 0;
        for(double data : dataVec) {
            if(data > ref) above += (data - ref);
            else notAbove += (ref - data);
        }
        return above / (above + notAbove);
    }
}