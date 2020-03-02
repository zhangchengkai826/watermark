package io.github.zhangchengkai826.watermark;

import java.util.List;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import io.github.zhangchengkai826.watermark.optimizer.GeneticOptimizerBuilder;
import io.github.zhangchengkai826.watermark.optimizer.ObjectiveFunction;
import io.github.zhangchengkai826.watermark.optimizer.Optimizer;

public class Embedder {

    public DataSet embed(DataSet source, Watermark watermark, String secretKey) {
        Partitioner partitioner = new Partitioner();
        partitioner.partition(source, secretKey);

        DataSet[] partitions = partitioner.getPartitions();
        int numEmbededBits = 0;
        for (DataSet partition : partitions) {
            for (int i = 0; i < partition.getNumCols(); i++) {
                if (partition.isColumnFixed(i)) {
                    boolean bit = watermark.getBit(numEmbededBits % watermark.getNumBits());
                    DataSet.ColumnDef colDef = partition.getColDef(i);

                    GeneticOptimizerBuilder optimizerBuilder = new GeneticOptimizerBuilder();
                    optimizerBuilder.setMagOfAlt(colDef.getConstraint().getMagOfAlt());
                    if (!colDef.getConstraint().getTryKeepAvg())
                        optimizerBuilder.noPenalty();
                    Optimizer optimizer = optimizerBuilder.build();

                    optimizer.setObjectiveFunc(new HidingFunction());

                    numEmbededBits++;
                }
            }
        }

        DataSet sourceEmb = partitioner.reconstruct();
        return sourceEmb;
    }

    static class HidingFunction extends ObjectiveFunction {
        // It's in the range (0, 1)
        static final double STDEV_MULTIPLIER = 0.1;
        double stdevMultipiler = STDEV_MULTIPLIER;

        @Override
        protected double apply(List<Double> dataVec) {
            SummaryStatistics ss = new SummaryStatistics();
            for (double data : dataVec)
                ss.addValue(data);
            double ref = ss.getMean() + stdevMultipiler * ss.getStandardDeviation();
            long numNotBelow = dataVec.stream().filter(d -> d >= ref).count();
            return (double) dataVec.size() / numNotBelow;
        }
    }
}