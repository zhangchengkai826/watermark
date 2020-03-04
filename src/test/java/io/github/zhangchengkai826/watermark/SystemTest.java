package io.github.zhangchengkai826.watermark;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

import io.github.zhangchengkai826.watermark.function.ObjectiveFunction;
import io.github.zhangchengkai826.watermark.function.ObjectiveFunctionA2;

public class SystemTest {
    private static final Logger LOGGER = LogManager.getLogger();

    private void compareWatermarks(Watermark embededWatermark, Watermark extractedWatermark) {
        int numMismatched = 0;
        for (int bitIndex = 0; bitIndex < embededWatermark.getNumBits(); bitIndex++) {
            boolean embeded = embededWatermark.getBit(bitIndex);
            boolean extracted = extractedWatermark.getBit(bitIndex);
            LOGGER.trace(String.format("Bit %d: embeded - %d, extracted - %d", bitIndex, embeded ? 1 : 0,
                    extracted ? 1 : 0));
            if (embeded != extracted)
                numMismatched++;
        }
        LOGGER.trace(String.format("%d bits mismatched, recovery rate %.2f%%", numMismatched,
                100 - (float) numMismatched / embededWatermark.getNumBits() * 100));
    }

    @Test
    public void test1() throws IOException, SQLException, ClassNotFoundException {
        TestEnv testEnv = TestEnv.loadFromJsonRes("env.test.json");

        final Watermark embededWatermark = new Watermark("ck");
        final int numPartitions = embededWatermark.getNumBits() * 32;
        final ObjectiveFunction objectiveFunction = new ObjectiveFunctionA2();

        DataSet source;
        try (DbReader dbReader = new DbReader(testEnv.host, testEnv.port, testEnv.dbname, testEnv.user,
                testEnv.password)) {
            source = dbReader.read(testEnv.table);
        }
        assertEquals("DbReader should read all rows in the specified table into DataSet, no more, no less.",
                testEnv.tableNumRows, source.getNumRows());

        source.setColumnAsFixed(new String[] { "firmid", "fdate", "contnum", "setnum", "tradecomm", "buyorsal",
                "oppfirmid", "openflat", "oflatlose", "flatlose" });

        source.setColumnConstraintMagOfAlt("ftime", 300);
        source.setColumnConstraintMagOfAlt("contprice", 10);
        source.setColumnConstraintMagOfAlt("difprice", 1);
        source.setColumnConstraintMagOfAlt("bailmoney", 10);

        Embedder embedder = new Embedder();
        DataSet sourceEmb = embedder.embed(source, embededWatermark, testEnv.secretKey, numPartitions,
                objectiveFunction);
        final Map<String, Double> decodingThresholds = embedder.getDecodingThresholds();

        try (DbWriter dbWriter = new DbWriter(testEnv.host, testEnv.port, testEnv.dbname, testEnv.user,
                testEnv.password)) {
            dbWriter.write(testEnv.tableEmb, sourceEmb);
        }

        Extractor extractor = new Extractor();
        Watermark extractedWatermark = extractor.extract(sourceEmb, embededWatermark.getNumBits(), testEnv.secretKey,
                numPartitions, decodingThresholds, objectiveFunction);
        compareWatermarks(embededWatermark, extractedWatermark);
        assertEquals("Embeded watermark should be extracted intactly.", embededWatermark.toString(),
                extractedWatermark.toString());
    }
}
