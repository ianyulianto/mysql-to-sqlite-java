package com.renseki.plugin;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.codejargon.fluentjdbc.api.mapper.ObjectMappers;
import org.codejargon.fluentjdbc.api.query.Mapper;
import org.sql2o.Sql2o;
import org.testng.annotations.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FooTest {
    Gson GSON = (new GsonBuilder()).setDateFormat("yyyy-MM-dd HH:mm:ss").setPrettyPrinting().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    private <T> Mapper<T> getMapper(Class<T> cls) {
        ObjectMappers objMappers = ObjectMappers.builder().build();
        return objMappers.forClass(cls);
    }


    @Test
    public void test() throws Exception {

        String query = "select * from ir_audit";
        String limit = "LIMIT %s OFFSET %s";
        Sql2o sql2o = new Sql2o("jdbc:mysql://localhost:3306/conecworld_negotiator_test", "conecworld", "pararaton1190");

        int count = 0;
        try ( org.sql2o.Connection conn = sql2o.open() ) {
            String countQ = "select count(1) from ir_audit";
            count = conn.createQuery(countQ).executeScalar(Integer.class);
        }


        final int batchSize = 1000;
        final int pageCount = count / batchSize + 1;
        List<Map<String, Object>> pool = new ArrayList<>();

//        -------

        System.out.println(new Date());
        Map<Integer, Future<List<Map<String, Object>>>> maps = new LinkedHashMap<>();
        LinkedList<Integer> list = new LinkedList<>();
        for ( int i = 0; i < pageCount; i++ ) {
            FooRunnable fooRunnable = new FooRunnable(sql2o.getDataSource(), i);

            Future<List<Map<String, Object>>> future = executor.submit(fooRunnable);
            maps.put(i, future);

            list.add(i);
        }


        while ( !list.isEmpty() ) {
            for ( Iterator<Integer> it = list.iterator(); it.hasNext(); ) {
                final int pageIdx = it.next();
                Future<List<Map<String, Object>>> future = maps.get(pageIdx);

                if ( future.isDone() ) {
                    List<Map<String, Object>> asd = future.get();
                    if ( asd != null ) {
                        pool.addAll(future.get());
                    }
                    it.remove();
                }
            }
        }

        String asd = "";
        System.out.println(new Date());

        System.out.println(GSON.toJson(pool));
    }



    private static class FooRunnable implements Callable<List<Map<String, Object>>> {

        private final DataSource ds;
        private final int index;
        private FooRunnable(DataSource ds, int index) {
            this.ds = ds;
            this.index = index;
        }

        @Override
        public List<Map<String, Object>> call() throws Exception {
            String query = "select * from ir_audit LIMIT 1000 OFFSET " + index;

            List<Map<String, Object>> res = new ArrayList<>();
            try ( Connection conn = ds.getConnection() ) {
                Statement st = conn.createStatement();

                ResultSet rs = st.executeQuery(query);
                res = resultSetToList(rs);
            }
            catch (Throwable t) {
                t.printStackTrace();
            }
            return res;
        }

        private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
            ResultSetMetaData md = rs.getMetaData();
            int columns = md.getColumnCount();
            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()){
                Map<String, Object> row = new HashMap<>(columns);
                for(int i = 1; i <= columns; ++i){
                    row.put(md.getColumnName(i), rs.getObject(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }
}
