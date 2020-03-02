package io.github.zhangchengkai826.watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.zhangchengkai826.watermark.optimizer.GeneticOptimizerBuilder;
import io.github.zhangchengkai826.watermark.optimizer.ObjectiveFunction;
import io.github.zhangchengkai826.watermark.optimizer.Optimizer;

public class Embedder {
    private static final Logger LOGGER = LogManager.getLogger();

    private List<Double> minimizedHidingFunValues = new ArrayList<>();
    private List<Double> maximizedHidingFunValues = new ArrayList<>();

    private Optional<Double> decodingThreshold = Optional.empty();

    public double getDecodingThreshold() {
        if (!decodingThreshold.isPresent())
            calcDecodingThreshold();
        return decodingThreshold.get();
    }

    private void calcDecodingThreshold() {
        final int numEncodedBits = minimizedHidingFunValues.size() + maximizedHidingFunValues.size();
        final double p0 = (double) minimizedHidingFunValues.size() / numEncodedBits;
        final double p1 = 1 - p0;

        SummaryStatistics ss = new SummaryStatistics();
        for (double x : minimizedHidingFunValues)
            ss.addValue(x);
        final double mean0 = ss.getMean();
        final double stdev0 = ss.getStandardDeviation();
        final double var0 = ss.getVariance();

        ss.clear();
        for (double x : maximizedHidingFunValues)
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

        decodingThreshold = Optional.of(secondOrderTest.test(root0) ? root0 : root1);
        LOGGER.trace("Decoding threshold: " + decodingThreshold.get());
    }

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

                    if (bit) {
                        optimizer.maximize();
                        maximizedHidingFunValues.add(optimizer.getOptimizedFunValue());
                    } else {
                        optimizer.minimize();
                        minimizedHidingFunValues.add(optimizer.getOptimizedFunValue());
                    }

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
            return (double) numNotBelow / dataVec.size();
        }
    }
}