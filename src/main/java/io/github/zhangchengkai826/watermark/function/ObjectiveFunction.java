package io.github.zhangchengkai826.watermark.function;

import java.util.List;

public abstract class ObjectiveFunction {
    public abstract double apply(List<Double> dataVec);
}