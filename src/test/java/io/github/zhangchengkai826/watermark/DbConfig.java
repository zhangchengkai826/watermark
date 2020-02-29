package io.github.zhangchengkai826.watermark;

import java.io.IOException;
import java.io.InputStreamReader;

import com.google.gson.Gson;

class DbConfig {
    private DbConfig() {}

    String host;
    int port;
    String dbname;
    String user;
    String password;
    String table;

    static DbConfig initFromJsonRes(String path) throws IOException {
        Gson gson = new Gson();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try(InputStreamReader reader = new InputStreamReader(classLoader.getResourceAsStream(path))) {
            return gson.fromJson(reader, DbConfig.class);
        }
    }
}