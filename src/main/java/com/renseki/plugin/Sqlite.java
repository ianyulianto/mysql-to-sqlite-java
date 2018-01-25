package com.renseki.plugin;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.ISqlJetTable;
import org.tmatesoft.sqljet.core.table.SqlJetDb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Sqlite implements Closeable {

    public static Sqlite create(File sqlite) throws SqlJetException {
        //noinspection ResultOfMethodCallIgnored
        sqlite.delete();

        SqlJetDb db = SqlJetDb.open(sqlite, true);
        db.getOptions().setAutovacuum(true);
        db.beginTransaction(SqlJetTransactionMode.WRITE);
        try {
            db.getOptions().setUserVersion(1);
        } finally {
            db.commit();
        }

        return new Sqlite(db);
    }

    public static Sqlite open(File sqlite) throws SqlJetException {
        SqlJetDb db = SqlJetDb.open(sqlite, true);
        return new Sqlite(db);
    }

    private final SqlJetDb db;
    private boolean closed = false;
    private Sqlite(SqlJetDb db) {
        this.db = db;
    }

    public void initTableFromMysql(String tableName, String query) throws SqlJetException {
        if ( closed ) {
            throw new RuntimeException("Sqlite was closed!");
        }

        //  For Leftover indexes
        List<String> indices = new ArrayList<>();

        //  Remove all unnecessary query attribute from MySQL
        //  - Split INDEX creation to 'indices'
        String cleanQuery = cleanMySqlForSqlite(query, tableName, indices);
        cleanQuery += ";\n";

        //  Create Table in Sqlite
        try {
            db.beginTransaction(SqlJetTransactionMode.WRITE);
            db.createTable(cleanQuery);
            if ( !indices.isEmpty() ) {
                for ( String temp : indices ) {
                    db.createIndex(temp);
                }
            }
        }
        finally {
            db.commit();
        }
    }

    public void insertTransactional(String tableName, List<Map<String, Object>> values) throws SqlJetException {
        if ( closed ) {
            throw new RuntimeException("Sqlite was closed!");
        }

        db.beginTransaction(SqlJetTransactionMode.WRITE);
        try {
            ISqlJetTable table = db.getTable(tableName);
            for ( Map<String, Object> mapValue : values ) {
                table.insertByFieldNames(mapValue);
            }
        }
        finally {
            db.commit();
        }
    }


    private static String cleanMySqlForSqlite(String tableQuery, String tableName, List<String> indices) {
        String foreignKey;

        tableQuery = tableQuery.toLowerCase();
        tableQuery = tableQuery.replaceAll(" text", " memo");
        tableQuery = tableQuery.replaceAll("int\\(\\d+\\)", "integer");
        tableQuery = tableQuery.replace("auto_increment", "");
        tableQuery = tableQuery.replaceAll("decimal\\(\\d+,\\d\\)", "real");
        tableQuery = tableQuery.replaceAll("comment '.+'", "");
        tableQuery = tableQuery.replaceAll("comment='.+'", "");
        tableQuery = tableQuery.replaceAll("engine.+latin1", "");
        tableQuery = tableQuery.replaceAll("varchar\\(\\d+\\)", "text(255)");
        tableQuery = tableQuery.replaceAll("using btree,", "");
        tableQuery = tableQuery.replaceAll("using btree", "");

        Pattern pattern = Pattern.compile("unique key .+ \\(.+\\)");
        Matcher matcher = pattern.matcher(tableQuery);
        while (matcher.find()) {
            String result = matcher.group(0).replaceAll("key .+ \\(", "(");
            tableQuery = tableQuery.replace(matcher.group(0), result);
        }

        pattern = Pattern.compile("constraint [a-zA-Z`]+.+,*");
        matcher = pattern.matcher(tableQuery);
        while (matcher.find()) {
            String result = matcher.group(0);
            Pattern patternFK = Pattern.compile("foreign key .+\\)[a-zA-Z ]*,*");
            Matcher matcherFK = patternFK.matcher(result);
            while (matcherFK.find()) {
                foreignKey = matcherFK.group(0);
                String lastChar = foreignKey.substring(foreignKey.length() - 1, foreignKey.length());
                if (lastChar.equalsIgnoreCase(",")) {
                    foreignKey = foreignKey.replaceAll(",", " on update cascade,");
                } else {
                    foreignKey += " on update cascade";
                }

                tableQuery = tableQuery.replace(result, foreignKey);
            }
        }

        pattern = Pattern.compile("key [a-zA-Z`]+.+\\),*");
        matcher = pattern.matcher(tableQuery);
        while (matcher.find()) {
            String result = matcher.group(0);
            result = result.trim();
            String[] results = result.split(" ");

            results[2] = results[2].replace("),", ")");

            String index = "create index " + results[1] + " on " + tableName + " " + results[2];
            indices.add(index);
            tableQuery = tableQuery.replace(result, "");
            tableQuery = tableQuery.replace("  ", "");
        }

        pattern = Pattern.compile("\\n\\n+");
        matcher = pattern.matcher(tableQuery);
        while (matcher.find()) {
            tableQuery = tableQuery.replaceAll("\\n\\n+", "\n");
        }

        pattern = Pattern.compile(",\\s*\\)");
        matcher = pattern.matcher(tableQuery);
        while (matcher.find()) {
            tableQuery = tableQuery.replaceAll(",\\s*\\)", "\\)");
        }

        tableQuery = tableQuery.replaceAll("`", "\"");

        tableQuery = tableQuery.substring(0, tableQuery.lastIndexOf(")") + 1);

        return tableQuery;
    }

    @Override
    public void close() throws IOException {
        try {
            db.close();
            closed = true;
        } catch (SqlJetException ignored) { }
    }
}
