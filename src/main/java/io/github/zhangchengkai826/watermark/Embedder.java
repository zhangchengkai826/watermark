package io.github.zhangchengkai826.watermark;

import java.util.List;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class Embedder {
    // It's in the range (0, 1)
    static final double DEFAULT_HIDING_FUNC_PARAM_1 = 0.1;
    double hidingFuncParam1 = DEFAULT_HIDING_FUNC_PARAM_1;

    public Embedder() {}
    public Embedder(double hidingFuncParam1) {
        this.hidingFuncParam1 = hidingFuncParam1;
    }

    public DataSet embed(DataSet source, Watermark watermark) {
        return source;
    }

    double hidingFunc(List<Double> dataVec) {
        SummaryStatistics ss = new SummaryStatistics();
        for(double data: dataVec) ss.addValue(data);
        double ref = ss.getMean() + hidingFuncParam1 * ss.getStandardDeviation();
        long numNotBelow = dataVec.stream().filter(d -> d >= ref).count();
        return (double)dataVec.size() / numNotBelow;
    }
}