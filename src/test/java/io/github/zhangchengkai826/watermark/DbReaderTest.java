package io.github.zhangchengkai826.watermark;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

public class DbReaderTest {
    @Test public void testRead() throws IOException, SQLException, ClassNotFoundException {
        DbConfig cfg = DbConfig.initFromJsonRes("db.json");
        try(DbReader dbReader = new DbReader(cfg.host, cfg.port, cfg.dbname, cfg.user, cfg.password)) {
            dbReader.read(cfg.table);
        }
        //assertTrue("someLibraryMethod should return 'true'", classUnderTest.someLibraryMethod());
    }
}
