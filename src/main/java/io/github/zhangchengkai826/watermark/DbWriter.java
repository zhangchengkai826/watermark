package io.github.zhangchengkai826.watermark;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;

public class DbWriter implements Closeable {
    Connection conn;

    public DbWriter(String host, int port, String dbname, String user, String password) throws SQLException {
        String connUrl = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
        Properties connProps = new Properties();
        connProps.setProperty("user", user);
        connProps.setProperty("password", password);
        conn = DriverManager.getConnection(connUrl, connProps);
        conn.setAutoCommit(false);
    }

    public void write(String table, DataSet source) throws SQLException {
        StringJoiner colNames = new StringJoiner(", ", " (", ") ");
        StringJoiner placeHolders = new StringJoiner(", ", " (", ") ");
        for(int i = 0; i < source.getNumCols(); i++) {
            colNames.add(source.getColDef(i).name);
            placeHolders.add("?");
        }
        String sql = "INSERT INTO " + table + colNames + "VALUES" + placeHolders;
        try(PreparedStatement statement = conn.prepareStatement(sql)) {
            for(int i = 0; i < source.getNumRows(); i++) {
                List<Object> row = source.getRow(i);
                for(int j = 0; j < row.size(); j++) {
                    switch(source.getColDef(j).type) {
                        case DATE: {
                            statement.setDate(j+1, (Date)row.get(j));
                            break;
                        }
                        case FLOAT4: {
                            statement.setFloat(j+1, (float)row.get(j));
                            break;
                        }
                        case INT4: {
                            statement.setInt(j+1, (int)row.get(j));
                            break;
                        }
                        case BPCHAR:
                        case OTHER:
                        default: {
                            statement.setString(j+1, (String)row.get(j));
                            break;
                        }
                    }
                }
                statement.addBatch();
            }
            statement.executeBatch();
        }
        conn.commit();
    }

    @Override
    public void close() throws IOException {
        try {
            conn.close();
        } catch(SQLException sqlex) {
            throw new IOException(sqlex);
        }
    }
}