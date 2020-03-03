package io.github.zhangchengkai826.watermark.function;

import java.util.List;

abstract class DecodingThresholdEvaluatingFunction {
    abstract double apply(List<Double> minValues, List<Double> maxValues);
}