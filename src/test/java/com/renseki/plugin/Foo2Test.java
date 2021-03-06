package com.renseki.plugin;

import org.sql2o.Sql2o;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.io.File;

public class Foo2Test {


    @Test
    public void test() throws Exception {

        Sql2o sql2o = new Sql2o("jdbc:mysql://localhost:3306/conecworld_negotiator_test", "conecworld", "pararaton1190");
        final DataSource ds = sql2o.getDataSource();



        final String path = "/home/ian/Workstation/Projects/Renseki/_plugins/mysqltosqlite/ZZROT_EUREKA_" + System.currentTimeMillis() + ".db";

        final int threadCount = 4;
        File res = new MysqlToSqliteBuilder(ds, new File(path), threadCount)
                .withExcludeTableReqex("ir_model_relation")
//                .withExcludeDataTableRegex("(ir_audit|mobile_.*)")
                .withDefaultOrderByQuery("id ASC")
//                .addAdditionalWhereQuery("conecworld_receipt", "id > 100")
                .convert();

        String asd = "";

    }




}
