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

    // It stores the most recent partition assignment.
    // partitionCache[a] = b means row a is assigned to partition b.
    private List<Integer> partitionCache = new ArrayList<>();

    private DataSet[] partitions;
    DataSet[] getPartitions() {
        return partitions;
    }

    void partition(DataSet source, String secretKey) {
        partition(source, secretKey, DEFAULT_NUM_PARTITIONS);
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

    void partition(DataSet source, String secretKey, int numPartitions) {
        partitionCache.clear();
        partitions = new DataSet[numPartitions];
        for(int i = 0; i < partitions.length; i++) partitions[i] = source.getLinkedCopyWithIndependentRaw();
        
        for(int i = 0; i < source.getNumRows(); i++) {
            String rowFeature = generateRowFeature(source, i);
            int partitionId = generatePartitonId(rowFeature, secretKey, numPartitions);
            //LOGGER.trace("Row " + i + " belongs to partition " + partitionId);
            partitions[partitionId].addRow(source.getRow(i));
            partitionCache.add(partitionId);
        }
        
        for(int i = 0; i < partitions.length; i++) {
            LOGGER.trace("Partition " + i + "'s size: " + partitions[i].getNumRows());
        }
    }

    DataSet reconstruct() {
        if(partitions.length == 0) throw new RuntimeException("Cannot reconstruct original DataSet when partitions is empty.");
        DataSet reconstructDataSet = partitions[0].getLinkedCopyWithIndependentRaw();
        int[] iters = new int[partitions.length];
        for(int partitionId: partitionCache) {
            reconstructDataSet.addRow(partitions[partitionId].getRow(iters[partitionId]++));
        }
        return reconstructDataSet;
    }
}