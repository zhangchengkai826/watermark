package io.github.zhangchengkai826.watermark;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DbReader implements Closeable {
    Connection conn;

    public DbReader(String host, int port, String dbname, String user, String password) throws SQLException {
        String connUrl = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
        Properties connProps = new Properties();
        connProps.setProperty("user", user);
        connProps.setProperty("password", password);
        conn = DriverManager.getConnection(connUrl, connProps);
        conn.setAutoCommit(false);
    }

    public DataSet read(String table) throws SQLException {
        DataSet dataSet = new DataSet();
        try (PreparedStatement statement = conn.prepareStatement("SELECT * FROM " + table)) {
            statement.setFetchSize(50);
            try (ResultSet resultSet = statement.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int numOfCols = metaData.getColumnCount();
                for (int i = 1; i <= numOfCols; i++) {
                    dataSet.addColDef(metaData.getColumnLabel(i), metaData.getColumnTypeName(i));
                }
                while (resultSet.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= numOfCols; i++) {
                        switch (dataSet.getColDef(i - 1).type) {
                            case BPCHAR: {
                                row.add(resultSet.getString(i));
                                break;
                            }
                            case DATE: {
                                row.add(resultSet.getDate(i));
                                break;
                            }
                            case FLOAT4: {
                                row.add(resultSet.getFloat(i));
                                break;
                            }
                            case INT4: {
                                row.add(resultSet.getInt(i));
                                break;
                            }
                            case OTHER:
                            default: {
                                throw new RuntimeException("Unsupported db data type for column " + i);
                            }
                        }
                    }
                    dataSet.addRow(row);
                }
            }
        }
        dataSet.autoChooseFixedColumns();
        return dataSet;
    }

    @Override
    public void close() throws IOException {
        try {
            conn.close();
        } catch (SQLException sqlex) {
            throw new IOException(sqlex);
        }
    }
}