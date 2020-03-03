package io.github.zhangchengkai826.watermark.optimizer;

import java.util.List;
import java.util.Optional;

import io.github.zhangchengkai826.watermark.function.ObjectiveFunction;

public abstract class Optimizer {
    List<Double> originalDataVec;

    public void setOriginalDataVec(List<Double> originalDataVec) {
        this.originalDataVec = originalDataVec;
    }

    ObjectiveFunction objectiveFunc;

    public void setObjectiveFunc(ObjectiveFunction objectiveFunc) {
        this.objectiveFunc = objectiveFunc;
    }

    Optional<List<Double>> optimizedDataVec = Optional.empty();

    public List<Double> getOptimizedDataVec() {
        return optimizedDataVec.get();
    }

    Optional<Double> optimizedFunValue = Optional.empty();

    public double getOptimizedFunValue() {
        return optimizedFunValue.get();
    }

    public abstract void maximize();

    public abstract void minimize();
}