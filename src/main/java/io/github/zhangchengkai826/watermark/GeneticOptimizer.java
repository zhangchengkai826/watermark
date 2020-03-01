package io.github.zhangchengkai826.watermark;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

class GeneticOptimizer {
    class Population {
        class Individual {
            Individual(BitSet chromosome) {
                this.chromosome = chromosome;
            }
            BitSet chromosome;
    
            private Optional<Double> fitnessCache = Optional.empty();
            private double calcFitness() {
                List<Double> dataVec = antiChromosomize(chromosome);
                return objectiveFunc.apply(dataVec) + penaltyMultiplier * penaltyFunc.apply(dataVec, originalDataVec);
            }
            double getFitness() {
                if(!fitnessCache.isPresent()) fitnessCache = Optional.of(calcFitness());
                return fitnessCache.get();
            }
        }
    }

    List<Double> originalDataVec;

    Function<List<Double>, Double> objectiveFunc;

    Optional<List<Double>> optimizedDataVec = Optional.empty();

    // Each element of dataVec should not change (either increase or decrease) more than magOfAlt.
    static final double MAG_OF_ALT = 1.0;
    double magOfAlt = MAG_OF_ALT;

    // It should be less then 60.
    static final int GENE_LEN = 8;
    int geneLen = GENE_LEN;

    static final int POPULATION_SIZE = 10;
    int populationSize = POPULATION_SIZE;

    // It is usually a negative value.
    // If it is zero, penalty calculated by penalty function will not be applied.
    static final double DEFAULT_PENALTY_MULTIPLIER = -100.0;
    double penaltyMultiplier = DEFAULT_PENALTY_MULTIPLIER;

    static final BiFunction<List<Double>, List<Double>, Double> DEFAULT_PENALTY_FUNC = (curVar, originalVar) -> {
        return IntStream.range(0, curVar.size()).mapToDouble(i -> curVar.get(i) - originalVar.get(i)).sum();
    };
    BiFunction<List<Double>, List<Double>, Double> penaltyFunc = DEFAULT_PENALTY_FUNC;
    
    private Random random = new Random();

    // private BitSet genize(double curDataElem, double originalDataElem) {
    //     long quantized = (int)((curDataElem - originalDataElem + magOfAlt) / (magOfAlt * 2) * (1 << geneLen));
    //     if(quantized == (1 << geneLen)) quantized--;
    //     // The variable quantized ranges from 0 to (1 << genLen)-1, inclusive.
    //     BitSet gene = new BitSet(geneLen);
    //     for(int i = geneLen-1; i >= 0; i--) {
    //         if((quantized & 0x1) == 1) {
    //             gene.set(i);
    //             quantized >>= 1;
    //         }
    //     }
    //     return gene;
    // }
    private double antiGenize(BitSet gene, double originalDataElem) {
        long quantized = 0;
        for(int i = 0; i < geneLen; i++) {
            quantized <<= 1;
            if(gene.get(i)) {
                quantized |= 1;
            }
        }
        return ((double)quantized + 0.5) / (1 << geneLen) * (2 * magOfAlt) - magOfAlt + originalDataElem;
    }
    // private BitSet chromosomize(List<Double> dataVec) {
    //     final int numGenes = dataVec.size();
    //     BitSet chromosome = new BitSet(numGenes * geneLen);
    //     for(int i = 0; i < dataVec.size(); i++) {
    //         BitSet gene = genize(dataVec.get(i), originalDataVec.get(i));
    //         for(int j = geneLen-1; j >= 0; j--) {
    //             chromosome.set(geneLen*i + j, gene.get(j));
    //         }
    //     }
    //     return chromosome;
    // }
    private List<Double> antiChromosomize(BitSet chromosome) {
        List<Double> dataVec = new ArrayList<>();
        final int numGenes = chromosome.size() / geneLen;
        for(int i = 0; i < numGenes; i++) {
            BitSet gene = chromosome.get(i*geneLen, (i+1)*geneLen);
            dataVec.add(antiGenize(gene, originalDataVec.get(i)));
        }
        return dataVec;
    }

    private List<Individual> generateInitialPopulation() {
        List<Individual> population = new ArrayList<>();
        final int numGenes = originalDataVec.size();
        for(int i = 0; i < populationSize; i++) {
            BitSet chromosome = new BitSet(numGenes * geneLen);
            for(int j = 0; j < chromosome.size(); j++) {
                if(random.nextBoolean()) {
                    chromosome.set(j);
                }
            }
            population.add(new Individual(chromosome));
        }
        return population;
    }

    void maximize() {
        List<Individual> initialPopulation = generateInitialPopulation();
        int generationId = 0;
        while(true) {
            generationId++;

        }
    }
}