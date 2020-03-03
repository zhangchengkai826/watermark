package io.github.zhangchengkai826.watermark;

public class Extractor {
    public Watermark extract(DataSet source, String secretKey, int numPartitions, double decodingThreshold) {
        Partitioner partitioner = new Partitioner(numPartitions);
        partitioner.partition(source, secretKey);

        return new Watermark("watermarkStr");
    }
}