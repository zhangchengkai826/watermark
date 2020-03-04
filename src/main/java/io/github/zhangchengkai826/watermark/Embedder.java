package io.github.zhangchengkai826.watermark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.zhangchengkai826.watermark.function.DecodingThresholdEvaluatingFunctionA;
import io.github.zhangchengkai826.watermark.function.ObjectiveFunction;
import io.github.zhangchengkai826.watermark.optimizer.GeneticOptimizerBuilder;
import io.github.zhangchengkai826.watermark.optimizer.Optimizer;

public class Embedder {
    private static final Logger LOGGER = LogManager.getLogger();

    // Each embeded column has its own list of minimizedHidingFunValue &
    // maximizedHidingFunValue.
    //
    // Map's key -> column's name, value -> column's list of minimizedHidingFunValue
    // & maximizedHidingFunValue
    private Optional<Map<String, List<Double>>> minimizedHidingFunValues = Optional.empty();

    private Map<String, List<Double>> getMinimizedHidingFunValues() {
        return minimizedHidingFunValues.get();
    }

    private void setMinimizedHidingFunValues(Map<String, List<Double>> newVal) {
        minimizedHidingFunValues = Optional.of(newVal);
    }

    private Optional<Map<String, List<Double>>> maximizedHidingFunValues = Optional.empty();

    private Map<String, List<Double>> getMaximizedHidingFunValues() {
        return maximizedHidingFunValues.get();
    }

    private void setMaximizedHidingFunValues(Map<String, List<Double>> newVal) {
        maximizedHidingFunValues = Optional.of(newVal);
    }

    // Each embeded column has its own decodingThreshold.
    //
    // Map's key -> column's name, value -> column's list of minimizedHidingFunValue
    // & maximizedHidingFunValue
    private Optional<Map<String, Double>> decodingThresholds = Optional.empty();

    public Map<String, Double> getDecodingThresholds() {
        return decodingThresholds.get();
    }

    private void calcDecodingThresholds() {
        Map<String, Double> thresholds = new HashMap<>();

        for (String colName : getMinimizedHidingFunValues().keySet()) {
            List<Double> minValues = getMinimizedHidingFunValues().get(colName);
            List<Double> maxValues = getMaximizedHidingFunValues().get(colName);

            double threshold = new DecodingThresholdEvaluatingFunctionA().apply(minValues, maxValues);
            LOGGER.trace("Decoding threshold for column '" + colName + "': " + threshold);
            thresholds.put(colName, threshold);
        }
        decodingThresholds = Optional.of(thresholds);
    }

    public DataSet embed(DataSet source, Watermark watermark, String secretKey, int numPartitions, ObjectiveFunction objectiveFunction) {
        Partitioner partitioner = new Partitioner(numPartitions);
        partitioner.partition(source, secretKey);

        setMinimizedHidingFunValues((new HashMap<>()));
        setMaximizedHidingFunValues((new HashMap<>()));
        for (int i = 0; i < source.getNumCols(); i++) {
            if (!source.isColumnFixed(i)) {
                int numEmbededBits = 0;
                DataSet.ColumnDef colDef = source.getColDef(i);
                getMinimizedHidingFunValues().put(colDef.name, new ArrayList<>());
                getMaximizedHidingFunValues().put(colDef.name, new ArrayList<>());
                for (DataSet partition : partitioner.getPartitions()) {
                    boolean bit = watermark.getBit(numEmbededBits % watermark.getNumBits());

                    GeneticOptimizerBuilder optimizerBuilder = new GeneticOptimizerBuilder();
                    optimizerBuilder.setMagOfAlt(colDef.getConstraint().getMagOfAlt());
                    if (!colDef.getConstraint().getTryKeepAvg())
                        optimizerBuilder.noPenalty();
                    Optimizer optimizer = optimizerBuilder.build();

                    optimizer.setOriginalDataVec(partition.getColumnAsDataVec(i));

                    optimizer.setObjectiveFunc(objectiveFunction);

                    if (bit) {
                        optimizer.maximize();
                        getMaximizedHidingFunValues().get(colDef.name).add(optimizer.getOptimizedFunValue());
                    } else {
                        optimizer.minimize();
                        getMinimizedHidingFunValues().get(colDef.name).add(optimizer.getOptimizedFunValue());
                    }

                    partition.setColumnByDataVec(i, optimizer.getOptimizedDataVec());

                    numEmbededBits++;
                }
            }
        }

        DataSet sourceEmb = partitioner.reconstruct();
        calcDecodingThresholds();
        return sourceEmb;
    }
}