package com.renseki.plugin;

import org.sql2o.Sql2o;
import org.tmatesoft.sqljet.core.SqlJetException;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MysqlToSqliteBuilder {

    private static List<Map<String, Object>> executeSql(DataSource ds, String query, Object[] params) {
        final List<Map<String, Object>> res = new ArrayList<>();

        final Sql2o sql2o = new Sql2o(ds);
        try ( org.sql2o.Connection conn = sql2o.open() ) {

            //  Execute Query
            try (
                    Connection jdbcConn = conn.getJdbcConnection();
                    PreparedStatement pst = jdbcConn.prepareStatement(query);
            ) {

                for ( int i = 0; i < params.length; i++) {

                    //  i + 1 -> karena PrepareStatement dimulai dari index 1
                    pst.setObject(i+1, params[i]);
                }

                ResultSet rs = pst.executeQuery();
                res.addAll(resultSetToList(rs));
            } catch (SQLException ignored) { }
        }
        return res;
    }

    private static List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int columns = md.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();
        while (rs.next()){
            Map<String, Object> row = new HashMap<>(columns);
            for(int i = 1; i <= columns; ++i){

                Object value = rs.getObject(i);
                row.put(md.getColumnLabel(i), value);
            }
            rows.add(row);
        }
        return rows;
    }

    private static final int READ_BATCH_SIZE = 1000;
    private final ExecutorService executor;

    private final DataSource dataSource;
    private final File sqliteOutput;

    private String excludeTableRegex;
    private String excludeDataTableRegex;
    private String defaultOrderByQuery;
    private Map<String, SqlQuery> additonalWhereQuery = new HashMap<>();
    private Map<String, String> orderByQuery = new HashMap<>();

    public MysqlToSqliteBuilder(DataSource dataSource, File sqliteOutput, int threadCount) {
        this.dataSource = dataSource;
        this.sqliteOutput = sqliteOutput;
        executor = Executors.newFixedThreadPool(threadCount);
    }

    public MysqlToSqliteBuilder withExcludeTableReqex(String regex) {
        this.excludeTableRegex = regex;
        return this;
    }

    public MysqlToSqliteBuilder withExcludeDataTableRegex(String regex) {
        this.excludeDataTableRegex = regex;
        return this;
    }

    public MysqlToSqliteBuilder withAdditionalWhereQuery(Map<String, SqlQuery> whereQuery) {
        this.additonalWhereQuery.putAll(whereQuery);
        return this;
    }

    public MysqlToSqliteBuilder addAdditionalWhereQuery(String tableName, SqlQuery whereQuery) {
        this.additonalWhereQuery.put(tableName, whereQuery);
        return this;
    }

    public MysqlToSqliteBuilder withOrderByQuery(Map<String, String> orderBy) {
        this.orderByQuery.putAll(orderBy);
        return this;
    }

    public MysqlToSqliteBuilder addOrderByQuery(String tableName, String orderBy) {
        this.orderByQuery.put(tableName, orderBy);
        return this;
    }

    public MysqlToSqliteBuilder withDefaultOrderByQuery(String defaultOrderByQuery) {
        this.defaultOrderByQuery = defaultOrderByQuery;
        return this;
    }

    public File convert() throws SqlJetException {
        List<String> tables = this.getAllTables();

        //  Remove Excluded Table
        for ( Iterator<String> it = tables.iterator(); it.hasNext(); ) {
            final String tableName = it.next();

            if ( excludeTableRegex != null &&
                    tableName.matches(excludeTableRegex) ) {
                it.remove();
            }
        }

        //  Prepare SQLite File
        try ( final Sqlite sqlite = Sqlite.create(sqliteOutput) ) {

            //  List all Tables
            for ( String tableName : tables ) {


                //  SHOW Create Schema
                final String createSchemaQuery = this.getCreateSchema(tableName);

                //  Create Table in Sqlite
                sqlite.initTableFromMysql(tableName, createSchemaQuery);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        //  Insert Data to Sqlite
        try ( final Sqlite sqlite = Sqlite.open(sqliteOutput) ) {
            for ( String tableName : tables ) {
                if ( excludeDataTableRegex != null &&
                        tableName.matches(excludeDataTableRegex) ) {
                    continue;
                }

                this.appendAllData(sqlite, tableName);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return sqliteOutput;
    }

    private void appendAllData(Sqlite sqlite, String tableName) {

        //  Where Query & Order By
        final SqlQuery sqlQuery = this.additonalWhereQuery.get(tableName);


        String orderBy = this.orderByQuery.get(tableName);
        if ( orderBy == null || orderBy.trim().isEmpty() ) {
            orderBy = defaultOrderByQuery;
        }

        //  Get Data Count
        final int dataCount = this.getTableDataCount(tableName, sqlQuery);

        //  Preparing Batch Process
        final int pageCount = dataCount / READ_BATCH_SIZE + 1;

        //  Register to Thread Executor
        List<Future> futures = new ArrayList<>();
        for ( int i = 0; i < pageCount; i++ ) {
            DataFetchExecutor callable =
                    new DataFetchExecutor(dataSource, tableName, i, sqlite)
                    .withAdditionalWhereQuery(sqlQuery)
                    .withOrderBy(orderBy);

            Future future = executor.submit(callable);
            futures.add(future);
        }

        //  Wait until process is done
        try {
            boolean done;
            do {
                done = true;
                for ( Future future : futures ) {
                    if ( !future.isDone() ) {
                        done = false;
                    }
                }
            } while ( !done );

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private int getTableDataCount(String tableName, SqlQuery sqlQuery) {
        final Sql2o sql2o = new Sql2o(dataSource);

        Object[] params = new Object[]{};
        String additionalWhereQuery = null;
        if ( sqlQuery != null ) {
            additionalWhereQuery = sqlQuery.getQuery();
            params = sqlQuery.getParams();
        }

        //  Resolve Additional Where Query
        String whereQuery = "1=1";
        if ( additionalWhereQuery != null &&
                !additionalWhereQuery.trim().isEmpty() ) {
            whereQuery = " (" + additionalWhereQuery + ") ";
        }


        final String countQ = "select count(1) as \"COUNT\" from " + tableName +
                " WHERE " + whereQuery;

        List<Map<String, Object>> list = executeSql(dataSource, countQ, params);
        int count = 0;
        if ( !list.isEmpty() ) {
            Map<String, Object> map = list.get(0);
            count = Integer.parseInt(map.get("COUNT").toString());
        }

        return count;
    }


    private String getCreateSchema(String tableName) {
        String res = null;
        try ( Connection conn = dataSource.getConnection() ) {

            final String query = String.format("show create table %s", tableName);
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            while ( rs.next() ) {
                res = rs.getString(2);
            }
            st.close();
            rs.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    private List<String> getAllTables() {
        List<String> res = new ArrayList<>();
        try ( Connection conn = dataSource.getConnection() ) {

            final String query = "show tables";

            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            while ( rs.next() ) {
                res.add(rs.getString(1));
            }
            st.close();
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return res;
    }

    private static class DataFetchExecutor implements Runnable {
        static synchronized void insertTransaction(Sqlite sqlite, String tableName, List<Map<String, Object>> values) {
            try {
                sqlite.insertTransactional(tableName, values);
            } catch (SqlJetException e) {
                throw new RuntimeException(e);
            }
        }

        private final DataSource ds;
        private final String tableName;
        private final int index;
        private final Sqlite sqlite;

        private SqlQuery sqlQuery;
        private String orderBy;

        private DataFetchExecutor(DataSource ds, String tableName, int index, Sqlite sqlite) {
            this.ds = ds;
            this.tableName = tableName;
            this.index = index;
            this.sqlite = sqlite;
        }

        private DataFetchExecutor withAdditionalWhereQuery(SqlQuery additionalWhereQuery) {
            this.sqlQuery = additionalWhereQuery;
            return this;
        }

        private DataFetchExecutor withOrderBy(String orderBy) {
            this.orderBy = orderBy;
            return this;
        }

        @Override
        public void run() {
            final Sql2o sql2o = new Sql2o(ds);

            String additionalWhereQuery = null;
            if ( this.sqlQuery != null ) {
                additionalWhereQuery = this.sqlQuery.getQuery();
            }

            //  Resolve Additional Where Query
            String whereQuery = "1=1";
            if ( additionalWhereQuery != null &&
                    !additionalWhereQuery.trim().isEmpty() ) {
                whereQuery = " (" + additionalWhereQuery + ") ";
            }

            //  Resolve Order By
            String orderByQuery = "";
            if ( this.orderBy != null &&
                    !this.orderBy.trim().isEmpty() ) {
                orderByQuery = " ORDER BY " + this.orderBy + " ";
            }

            //  resolve parameters
            Object[] params = new Object[]{};
            if ( this.sqlQuery != null &&
                    this.sqlQuery.getParams() != null ) {
                params = this.sqlQuery.getParams();
            }

            final String query =
                    "select * from " + tableName +
                    " WHERE " + whereQuery +
                    orderByQuery +
                    " LIMIT " + READ_BATCH_SIZE +
                    " OFFSET " + (index * READ_BATCH_SIZE);

            //  Execute & Clean Up
            final List<Map<String, Object>> tmp = executeSql(ds, query, params);
            final List<Map<String, Object>> res = this.cleanUpDataType(tmp);

            //  Masukkan ke Sqlite
            System.out.println("Insert into table: " + tableName + "; Index: " + index);
            insertTransaction(sqlite, tableName, res);
        }

        private List<Map<String, Object>> cleanUpDataType(List<Map<String, Object>> data) {
            List<Map<String, Object>> list = new ArrayList<>(data);

            //  Date Converter
            final String pattern = "yyyy-MM-dd HH:mm:ss";
            SimpleDateFormat sdf = new SimpleDateFormat();
            sdf.applyPattern(pattern);

            //  Convert Date to String
            for ( int i = 0; i < list.size(); i++ ) {
                Map<String, Object> map = list.get(i);

                boolean change = false;
                for ( String key : map.keySet() ) {

                    boolean modify = false;

                    Object value = map.get(key);
                    if ( value instanceof Timestamp ) {
                        value = sdf.format(value);

                        change = true;
                        modify = true;
                    }
                    else if ( value instanceof BigDecimal ) {
                        value = value.toString();

                        change = true;
                        modify = true;
                    }

                    if ( modify ) {
                        map.put(key, value);
                    }
                }

                if ( change ) {
                    list.set(i, map);
                }
            }

            return list;
        }
    }
}
