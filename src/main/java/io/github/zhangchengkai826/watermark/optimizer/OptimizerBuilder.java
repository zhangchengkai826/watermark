package io.github.zhangchengkai826.watermark.optimizer;

abstract class OptimizerBuilder<OptimizerType extends Optimizer> {
    OptimizerType optimizer;

    public OptimizerType build() {
        return optimizer;
    }
}