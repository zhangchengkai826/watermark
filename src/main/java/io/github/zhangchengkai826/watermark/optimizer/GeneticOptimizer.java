package io.github.zhangchengkai826.watermark.optimizer;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class GeneticOptimizer extends Optimizer {
    private static final Logger LOGGER = LogManager.getLogger();

    private GeneticOptimizer() {
    };

    class Population {
        class Individual {
            class Chromosome {
                Chromosome(BitSet bits) {
                    this.bits = bits;
                }

                Chromosome(Chromosome clonee) {
                    bits = (BitSet) bits.clone();
                }

                private BitSet bits;

                BitSet getBits() {
                    return bits;
                }

                private double antiGenize(BitSet bits, int geneBase, double originalDataElem) {
                    long quantized = 0;
                    for (int i = 0; i < geneLen; i++) {
                        quantized <<= 1;
                        if (bits.get(geneBase + i)) {
                            quantized |= 1;
                        }
                    }
                    return ((double) quantized + 0.5) / (1 << geneLen) * (2 * magOfAlt) - magOfAlt + originalDataElem;
                }

                List<Double> toDataVec() {
                    List<Double> dataVec = new ArrayList<>();
                    int geneBase = 0;
                    for (int i = 0; i < getNumGenes(); i++) {
                        dataVec.add(antiGenize(bits, geneBase, originalDataVec.get(i)));
                        geneBase += geneLen;
                    }
                    return dataVec;
                }

                void crossover(Chromosome another) {
                    int crossoverPoint = random.nextInt(getNumGenes()) * geneLen;
                    for (int i = 0; i < crossoverPoint; i++) {
                        boolean tmp = getBits().get(i);
                        getBits().set(i, another.getBits().get(i));
                        another.getBits().set(i, tmp);
                    }
                }

                void mutation() {
                    int mutationPoint = random.nextInt(getNumGenes() * geneLen);
                    getBits().flip(mutationPoint);
                }
            }

            Individual(BitSet bits) {
                this.chromosome = new Chromosome(bits);
            }

            Individual(Individual other) {
                chromosome = new Chromosome(chromosome);
            }

            private Chromosome chromosome;

            Chromosome getChromosome() {
                return chromosome;
            }

            private Optional<Double> fitnessCache = Optional.empty();

            private double calcFitness() {
                List<Double> dataVec = chromosome.toDataVec();
                return objectiveFunc.apply(dataVec) + penaltyMultiplier * penaltyFunc.apply(dataVec, originalDataVec);
            }

            double getFitness() {
                if (!fitnessCache.isPresent())
                    fitnessCache = Optional.of(calcFitness());
                return fitnessCache.get();
            }

            void crossover(Individual another) {
                chromosome.crossover(another.chromosome);
            }

            void mutation() {
                chromosome.mutation();
            }
        }

        private TreeSet<Individual> individuals = new TreeSet<>((x, y) -> (int) (y.getFitness() - x.getFitness()));

        void addIndividual(Individual individual) {
            individuals.add(individual);
        }

        Individual getFittestIndividual() {
            return individuals.first();
        }

        Individual cloneFittestIndividual() {
            return new Individual(individuals.first());
        }

        Individual clone2ndFittestIndividual() {
            Iterator<Individual> it = individuals.iterator();
            it.next();
            return new Individual(it.next());
        }

        Individual getLeastFittestIndividual() {
            return individuals.last();
        }

        Individual cloneLeastFittestIndividual() {
            return new Individual(individuals.last());
        }

        Individual clone2ndLeastFittestIndividual() {
            Iterator<Individual> it = individuals.descendingIterator();
            it.next();
            return new Individual(it.next());
        }

        private Population() {
        }

        void evolve() {
            Individual offspring1 = cloneFittestIndividual();
            Individual offspring2 = clone2ndFittestIndividual();

            offspring1.crossover(offspring2);
            if (random.nextInt() % 100 < mutationRate)
                offspring1.mutation();
            if (random.nextInt() % 100 < mutationRate)
                offspring2.mutation();

            individuals.pollLast();
            if (offspring1.getFitness() > offspring2.getFitness())
                individuals.add(offspring1);
            else
                individuals.add(offspring2);
        }

        void degrade() {
            Individual offspring1 = cloneLeastFittestIndividual();
            Individual offspring2 = clone2ndLeastFittestIndividual();

            offspring1.crossover(offspring2);
            if (random.nextInt() % 100 < mutationRate)
                offspring1.mutation();
            if (random.nextInt() % 100 < mutationRate)
                offspring2.mutation();

            individuals.pollFirst();
            if (offspring1.getFitness() < offspring2.getFitness())
                individuals.add(offspring1);
            else
                individuals.add(offspring2);
        }
    }

    class PopulationBuilder {
        private Population population = new Population();

        Population build() {
            return population;
        }

        PopulationBuilder addRandomizedIndividuals(int n) {
            final int numGenes = originalDataVec.size();
            for (int i = 0; i < n; i++) {
                BitSet bits = new BitSet(numGenes * geneLen);
                for (int j = 0; j < bits.size(); j++) {
                    if (random.nextBoolean()) {
                        bits.set(j);
                    }
                }
                population.addIndividual(population.new Individual(bits));
            }
            return this;
        }
    }

    int getNumGenes() {
        return originalDataVec.size();
    }

    // Each element of dataVec should not change (either increase or decrease) more
    // than magOfAlt.
    static final double MAG_OF_ALT = 1.0;
    double magOfAlt = MAG_OF_ALT;

    // It should be less then 60.
    static final int GENE_LEN = 8;
    int geneLen = GENE_LEN;

    static final int POPULATION_SIZE = 10;
    int populationSize = POPULATION_SIZE;

    // (0, 100)
    static final int MUTATION_RATE = 10;
    int mutationRate = MUTATION_RATE;

    // It is usually a negative value.
    // If it is zero, penalty calculated by penalty function will not be applied.
    static final double DEFAULT_PENALTY_MULTIPLIER = -100.0;
    double penaltyMultiplier = DEFAULT_PENALTY_MULTIPLIER;

    static final BiFunction<List<Double>, List<Double>, Double> DEFAULT_PENALTY_FUNC = (curVar, originalVar) -> {
        return IntStream.range(0, curVar.size()).mapToDouble(i -> curVar.get(i) - originalVar.get(i)).sum();
    };
    BiFunction<List<Double>, List<Double>, Double> penaltyFunc = DEFAULT_PENALTY_FUNC;

    private Random random = new Random();

    @Override
    public void maximize() {
        Population population = new PopulationBuilder().addRandomizedIndividuals(populationSize).build();
        int generationId = 0;
        double maxFitness = population.getFittestIndividual().getFitness();
        LOGGER.trace("Generation 0: maxFitness = " + maxFitness);
        double lastMaxFitness;
        do {
            generationId++;
            lastMaxFitness = maxFitness;
            population.evolve();
            maxFitness = population.getFittestIndividual().getFitness();
            LOGGER.trace("Generation " + generationId + ": maxFitness = " + maxFitness);
        } while (maxFitness > lastMaxFitness);
        optimizedDataVec = Optional.of(population.getFittestIndividual().getChromosome().toDataVec());
    }

    @Override
    public void minimize() {
        Population population = new PopulationBuilder().addRandomizedIndividuals(populationSize).build();
        int generationId = 0;
        double minFitness = population.getLeastFittestIndividual().getFitness();
        LOGGER.trace("Generation 0: maxFitness = " + minFitness);
        double lastMinFitness;
        do {
            generationId++;
            lastMinFitness = minFitness;
            population.degrade();
            minFitness = population.getLeastFittestIndividual().getFitness();
            LOGGER.trace("Generation " + generationId + ": maxFitness = " + minFitness);
        } while (minFitness < lastMinFitness);
        optimizedDataVec = Optional.of(population.getLeastFittestIndividual().getChromosome().toDataVec());
    }
}
