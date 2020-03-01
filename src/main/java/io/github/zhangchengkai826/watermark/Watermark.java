package io.github.zhangchengkai826.watermark;

import java.nio.charset.StandardCharsets;

class Watermark {
    byte[] raw;
    Watermark(String watermarkStr) {
        raw = watermarkStr.getBytes(StandardCharsets.UTF_8);
    }
    @Override
    public String toString() {
        return new String(raw, StandardCharsets.UTF_8);
    }
    // It returns true if bit at bitPosition is 1 (bitPosition is zero-based).
    // Bit's position increases from first byte to last byte, and from least-significant bit to most-significant bit.
    boolean isBitOn(int bitPosition) {
        int bytePosition = bitPosition / 8;
        int bitPositionInByte = bitPosition % 8;
        return ((raw[bytePosition] >> bitPositionInByte) & 0x1) == 1;
    }
}