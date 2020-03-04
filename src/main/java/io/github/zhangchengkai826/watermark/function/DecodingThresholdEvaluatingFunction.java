package io.github.zhangchengkai826.watermark.function;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class DecodingThresholdEvaluatingFunction {
    private static final Logger LOGGER = LogManager.getLogger();

    public abstract double apply(List<Double> minValues, List<Double> maxValues);

    protected void debugCheckThresholdQuality(List<Double> minValues, List<Double> maxValues, double threshold) {
        Stream<Double> minValueStream = minValues.stream().sorted(Comparator.reverseOrder());
        Stream<Double> maxValueStream = maxValues.stream().sorted();

        Double[] minValuesSorted = minValueStream.toArray(size -> new Double[size]);
        Double[] maxValuesSorted = maxValueStream.toArray(size -> new Double[size]);
        LOGGER.debug("minValuesSorted: " + Arrays.toString(minValuesSorted));
        LOGGER.debug("maxValuesSorted: " + Arrays.toString(maxValuesSorted));
        LOGGER.debug("threshold: " + threshold);

        Double[] minNotBelowThreshold = minValueStream.filter(v -> v >= threshold).toArray(size -> new Double[size]);
        Double[] maxNotAboveThreshold = maxValueStream.filter(v -> v <= threshold).toArray(size -> new Double[size]);
        double errRate = ((double) minNotBelowThreshold.length + maxNotAboveThreshold.length)
                / (minValuesSorted.length + maxValuesSorted.length);
        LOGGER.debug("minNotBelowThreshold: " + Arrays.toString(minNotBelowThreshold));
        LOGGER.debug("maxNotAboveThreshold: " + Arrays.toString(maxNotAboveThreshold));
        LOGGER.debug(String.format("errRate: (%d + %d) / (%d + %d) = %.6f", minNotBelowThreshold.length,
                maxNotAboveThreshold.length, minValuesSorted.length, maxValuesSorted.length, errRate));
    }
}