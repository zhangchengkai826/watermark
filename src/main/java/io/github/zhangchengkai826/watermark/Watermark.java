package io.github.zhangchengkai826.watermark;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;

class Watermark {
    private BitSet bits;
    private int numBits;

    Watermark(String watermarkStr) {
        byte[] bytes = watermarkStr.getBytes(StandardCharsets.UTF_8);
        numBits = bytes.length * 8;
        bits = BitSet.valueOf(bytes);
    }

    Watermark(int nbits) {
        numBits = nbits;
        bits = new BitSet(nbits);
    }

    @Override
    public String toString() {
        return new String(bits.toByteArray(), StandardCharsets.UTF_8);
    }

    // It returns true if bit at bitPosition is 1 (bitPosition is zero-based).
    // Bit's position increases from first byte to last byte, and from
    // least-significant bit to most-significant bit.
    boolean getBit(int bitIndex) {
        return bits.get(bitIndex);
    }

    void setBit(int bitIndex, boolean value) {
        bits.set(bitIndex, value);
    }

    int getNumBits() {
        return numBits;
    }
}