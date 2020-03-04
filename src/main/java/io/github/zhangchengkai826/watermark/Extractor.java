package io.github.zhangchengkai826.watermark;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.github.zhangchengkai826.watermark.function.ObjectiveFunction;

public class Extractor {
    private static final Logger LOGGER = LogManager.getLogger();

    public Watermark extract(DataSet sourceEmb, int watermarkNumBits, String secretKey, int numPartitions,
            Map<String, Double> decodingThresholds, ObjectiveFunction objectiveFunction) {
        Partitioner partitioner = new Partitioner(numPartitions);
        partitioner.partition(sourceEmb, secretKey);
        DataSet[] partitions = partitioner.getPartitions();

        Watermark extractedWatermark = new Watermark(watermarkNumBits);
        int[][] votes = new int[2][watermarkNumBits];
        for (String colName : decodingThresholds.keySet()) {
            int colId = sourceEmb.findColIdByName(colName);
            if (colId == -1)
                continue;

            double threshold = decodingThresholds.get(colName);
            for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
                double funcValue = objectiveFunction.apply(partitions[partitionId].getColumnAsDataVec(colId));
                votes[funcValue >= threshold ? 1 : 0][partitionId % watermarkNumBits]++;
            }
        }
        for (int bitIndex = 0; bitIndex < watermarkNumBits; bitIndex++) {
            LOGGER.trace("Bit " + bitIndex + ": [0] - " + votes[0][bitIndex] + ", [1] - " + votes[1][bitIndex]);
            extractedWatermark.setBit(bitIndex, votes[1][bitIndex] > votes[0][bitIndex] ? true : false);
        }

        LOGGER.trace("Extracted watermark: " + extractedWatermark);
        return extractedWatermark;
    }
}