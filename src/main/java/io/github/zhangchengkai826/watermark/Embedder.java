package io.github.zhangchengkai826.watermark;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.zhangchengkai826.watermark.optimizer.GeneticOptimizerBuilder;
import io.github.zhangchengkai826.watermark.optimizer.ObjectiveFunction;
import io.github.zhangchengkai826.watermark.optimizer.Optimizer;

public class Embedder {
    private static final Logger LOGGER = LogManager.getLogger();

    public DataSet embed(DataSet source, Watermark watermark, String secretKey, int numPartitions) {
        Partitioner partitioner = new Partitioner(numPartitions);
        partitioner.partition(source, secretKey);

        DataSet[] partitions = partitioner.getPartitions();
        int numEmbededBits = 0;
        for (DataSet partition : partitions) {
            for (int i = 0; i < partition.getNumCols(); i++) {
                if (!partition.isColumnFixed(i)) {
                    boolean bit = watermark.getBit(numEmbededBits % watermark.getNumBits());
                    DataSet.ColumnDef colDef = partition.getColDef(i);

                    GeneticOptimizerBuilder optimizerBuilder = new GeneticOptimizerBuilder();
                    optimizerBuilder.setMagOfAlt(colDef.getConstraint().getMagOfAlt());
                    if (!colDef.getConstraint().getTryKeepAvg())
                        optimizerBuilder.noPenalty();
                    Optimizer optimizer = optimizerBuilder.build();

                    List<Double> originalDataVec = new ArrayList<>();
                    for (int j = 0; j < partition.getNumRows(); j++) {
                        switch (colDef.type) {
                            case INT4: {
                                originalDataVec.add(((Integer) partition.getRow(j).get(i)).doubleValue());
                                break;
                            }
                            case FLOAT4: {
                                originalDataVec.add(((Float) partition.getRow(j).get(i)).doubleValue());
                                break;
                            }
                            default: {
                                LOGGER.trace("Column Name: " + colDef.name + ", Column Type: " + colDef.type);
                                throw new RuntimeException(
                                        "For now, Embedder can only embed watermark to integer or real type columns.");
                            }
                        }
                    }
                    optimizer.setOriginalDataVec(originalDataVec);

                    optimizer.setObjectiveFunc(new HidingFunction());

                    if (bit)
                        optimizer.maximize();
                    else
                        optimizer.minimize();

                    List<Double> optimizedDataVec = optimizer.getOptimizedDataVec();
                    for (int j = 0; j < partition.getNumRows(); j++) {
                        Object optimizedDataElem;
                        switch (colDef.type) {
                            case INT4: {
                                optimizedDataElem = optimizedDataVec.get(j).intValue();
                                break;
                            }
                            case FLOAT4: {
                                optimizedDataElem = optimizedDataVec.get(j).floatValue();
                                break;
                            }
                            default: {
                                throw new RuntimeException(
                                        "For now, Embedder can only embed watermark to integer or real type columns.");
                            }
                        }
                        partition.getRow(j).set(i, optimizedDataElem);
                    }

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