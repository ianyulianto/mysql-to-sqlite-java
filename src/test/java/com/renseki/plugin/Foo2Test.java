package com.renseki.plugin;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.codejargon.fluentjdbc.api.mapper.ObjectMappers;
import org.codejargon.fluentjdbc.api.query.Mapper;
import org.sql2o.Sql2o;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Foo2Test {
    Gson GSON = (new GsonBuilder()).setDateFormat("yyyy-MM-dd HH:mm:ss").setPrettyPrinting().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    @Test
    public void test() throws Exception {

        Sql2o sql2o = new Sql2o("jdbc:mysql://localhost:3306/conecworld_negotiator_test", "conecworld", "pararaton1190");
        final DataSource ds = sql2o.getDataSource();



        final String path = "/home/ian/Workstation/Projects/Renseki/_plugins/mysqltosqlite/ZZROT_EUREKA_" + System.currentTimeMillis() + ".db";

        File res = new MysqlToSqliteBuilder(ds, new File(path))
//                .withExcludeTableReqex("ir_")
                .convert();

        String asd = "";

    }




}
