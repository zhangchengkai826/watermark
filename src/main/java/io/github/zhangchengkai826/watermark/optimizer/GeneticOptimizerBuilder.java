package io.github.zhangchengkai826.watermark.optimizer;

public class GeneticOptimizerBuilder extends OptimizerBuilder<GeneticOptimizer> {
    public GeneticOptimizerBuilder() {
        optimizer = new GeneticOptimizer();
    }

    public GeneticOptimizerBuilder setMagOfAlt(double magOfAlt) {
        optimizer.magOfAlt = magOfAlt;
        return this;
    }

    public GeneticOptimizerBuilder noPenalty() {
        optimizer.penaltyMultiplier = 0;
        return this;
    }
}