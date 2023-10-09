package org.embulk.output.tsurugidb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.embulk.config.ConfigException;
import org.embulk.exec.PartialExecutionException;
import org.junit.Before;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.exception.UniqueConstraintViolationException;

public class MethodOptionTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name
    private static final int SIZE = 10;

    private static final int FIRST_PK = 1;
    private static final long FIRST_VALUE = 1111;

    @Before
    public void beforeEach() {
        dropTable(TEST);

        String sql = "create table " + TEST //
                + "(" //
                + " pk int primary key," //
                + " value bigint" //
                + ")";
        createTable(sql);

        executeOcc((sqlClient, tx) -> {
            var insertSql = "insert into " + TEST + " values(" + FIRST_PK + ", " + FIRST_VALUE + ")";
            tx.executeStatement(insertSql).await(3, TimeUnit.SECONDS);
        });
    }

    @Test
    public void testInvalidMethodOption() {
        var e = assertThrows(PartialExecutionException.class, () -> {
            test("insert", "hoge");
        });
        var c = (ConfigException) e.getCause();
        assertTrue(c.getMessage().contains("hoge"));
    }

    @Test
    public void testInsertDefault() {
        var e = assertThrows(PartialExecutionException.class, () -> {
            test("insert", null);
        });
        assertInsert(e);
    }

    @Test
    public void testInsertInsert() {
        var e = assertThrows(PartialExecutionException.class, () -> {
            test("insert", "insert");
        });
        assertInsert(e);
    }

    private void assertInsert(PartialExecutionException e) {
        var c = (ServerRuntimeException) e.getCause();
        var u = (UniqueConstraintViolationException) c.getCause();
        assertEquals(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, u.getDiagnosticCode());
    }

    @Test
    public void testInsertReplace() {
        var insertList = test("insert", "insert_or_replace");
        var actualList = selectAll();
        assertOverwrite(insertList, actualList);
    }

    private void assertOverwrite(List<String> insertList, List<String> actualList) {
        assertEquals(insertList.size(), actualList.size());
        int i = 0;
        for (var actual : actualList) {
            var expected = insertList.get(i++);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testInsertIfNotExists() {
        var insertList = test("insert", "insert_if_not_exists");
        var actualList = selectAll();
        assertIfAbsent(insertList, actualList);
    }

    private void assertIfAbsent(List<String> insertList, List<String> actualList) {
        assertEquals(insertList.size(), actualList.size());
        int i = 0;
        for (var actual : actualList) {
            var expected = insertList.get(i++);
            if (expected.startsWith(FIRST_PK + ",")) {
                expected = toCsv(FIRST_PK, FIRST_VALUE);
            }
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testPutDefault() {
        var insertList = test("put", null);
        var actualList = selectAll();
        assertOverwrite(insertList, actualList);
    }

    @Test
    public void testPutOverwrite() {
        var insertList = test("put", "put_overwrite");
        var actualList = selectAll();
        assertOverwrite(insertList, actualList);
    }

    @Test
    public void testPutIfAbsent() {
        var insertList = test("put", "put_if_absent");
        var actualList = selectAll();
        assertIfAbsent(insertList, actualList);
    }

    @Test
    public void testPutIfPresent() {
        test("put", "put_if_present");
        var expectedList = List.of(create(FIRST_PK));
        var actualList = selectAll();
        assertEquals(expectedList.size(), actualList.size());
        int i = 0;
        for (var actual : actualList) {
            var expected = expectedList.get(i++);
            assertEquals(expected, actual);
        }
    }

    private List<String> test(String method, String methodOption) {
        var csvList = new ArrayList<String>();
        for (int i = 0; i < SIZE; i++) {
            String csv = create(i);
            csvList.add(csv);
        }

        try (var tester = new EmbulkPluginTester()) {
            tester.addOutputPlugin(TsurugiOutputPlugin.TYPE, TsurugiOutputPlugin.class);

            var parser = tester.newParserConfig("csv");
            parser.addColumn("pk", "long");
            parser.addColumn("value", "long");

            var out = tester.newConfigSource();
            out.set("type", TsurugiOutputPlugin.TYPE);
            out.set("endpoint", ENDPOINT);
            out.set("table", TEST);
            out.set("tx_type", "OCC");
            if (method != null) {
                out.set("method", method);
            }
            if (methodOption != null) {
                out.set("method_option", methodOption);
            }

            tester.runOutput(csvList, parser, out);
        }

        return csvList;
    }

    private static String create(int pk) {
        return toCsv(pk, 100 - pk);
    }

    private static String toCsv(int pk, long value) {
        return pk + "," + value;
    }

    private List<String> selectAll() {
        var actualList = new ArrayList<String>();
        executeRtx((sqlClient, transaction) -> {
            String sql = "select * from " + TEST + " order by pk";
            try (var rs = executeQuery(transaction, sql)) {
                while (rs.nextRow()) {
                    rs.nextColumn();
                    int pk = rs.fetchInt4Value();
                    rs.nextColumn();
                    long value = rs.fetchInt8Value();
                    String record = toCsv(pk, value);
                    actualList.add(record);
                }
            }
        });
        return actualList;
    }
}
