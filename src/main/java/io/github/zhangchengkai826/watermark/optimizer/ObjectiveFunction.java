package io.github.zhangchengkai826.watermark.optimizer;

import java.util.List;

public abstract class ObjectiveFunction {
    protected abstract double apply(List<Double> dataVec);
}