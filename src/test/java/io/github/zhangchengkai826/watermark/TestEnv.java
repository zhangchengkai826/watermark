package io.github.zhangchengkai826.watermark;

import java.io.IOException;
import java.io.InputStreamReader;

import com.google.gson.Gson;

class TestEnv {
    private TestEnv() {}

    String host;
    int port;
    String dbname;
    String user;
    String password;
    String table;
    String tableEmb;
    int tableNumRows;
    String secretKey;

    static TestEnv loadFromJsonRes(String path) throws IOException {
        Gson gson = new Gson();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try(InputStreamReader reader = new InputStreamReader(classLoader.getResourceAsStream(path))) {
            return gson.fromJson(reader, TestEnv.class);
        }
    }
}