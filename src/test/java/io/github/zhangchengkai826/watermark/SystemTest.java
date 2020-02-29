package io.github.zhangchengkai826.watermark;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

public class SystemTest {
    @Test public void test1() throws IOException, SQLException, ClassNotFoundException {
        TestEnv testEnv = TestEnv.loadFromJsonRes("env.test.json");
        DataSet source;
        try(DbReader dbReader = new DbReader(testEnv.host, testEnv.port, testEnv.dbname, testEnv.user, testEnv.password)) {
            source = dbReader.read(testEnv.table);
        }
        assertEquals("DbReader should read all rows in the specified table into DataSet, no more, no less.", testEnv.tableNumRows, source.getNumRows());
        Partitioner partitioner = new Partitioner();
        partitioner.partition(source, testEnv.secretKey);
    }
}
