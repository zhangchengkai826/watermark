package io.github.zhangchengkai826.watermark;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class Partitioner {
    private static final Logger LOGGER = LogManager.getLogger();

    static final int DEFAULT_NUM_PARTITIONS = 5;

    DataSet[] partition(DataSet source, String secretKey) {
        return partition(source, secretKey, DEFAULT_NUM_PARTITIONS);
    }

    String generateRowFeature(DataSet dataSet, int rowId) {
        StringBuilder rowFeature = new StringBuilder();
        List<Object> row = dataSet.getRow(rowId);
        Set<Integer> fixedColId = dataSet.getFixedColId();
        for(int colId: fixedColId) {
            rowFeature.append(row.get(colId));
        }
        return rowFeature.toString();
    }

    int generatePartitonId(String rowFeature, String secretKey, int numPartitions) {
        byte[] hash = DigestUtils.sha1(secretKey + DigestUtils.sha1Hex(rowFeature + secretKey));
        return Math.abs(ByteBuffer.wrap(hash, 0, 4).getInt()) % numPartitions;
    }

    DataSet[] partition(DataSet source, String secretKey, int numPartitions) {
        DataSet[] partitions = new DataSet[numPartitions];
        for(int i = 0; i < partitions.length; i++) partitions[i] = new DataSet();
        
        for(int i = 0; i < source.getNumRows(); i++) {
            String rowFeature = generateRowFeature(source, i);
            int partitionId = generatePartitonId(rowFeature, secretKey, numPartitions);
            //LOGGER.trace("Row " + i + " belongs to partition " + partitionId);
            partitions[partitionId].addRow(source.getRow(i));
        }
        
        for(int i = 0; i < partitions.length; i++) {
            LOGGER.trace("Partition " + i + "'s size: " + partitions[i].getNumRows());
        }

        return partitions;
    }
}