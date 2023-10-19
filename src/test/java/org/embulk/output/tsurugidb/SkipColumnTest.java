package org.embulk.output.tsurugidb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.embulk.exec.PartialExecutionException;
import org.junit.Before;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.KvsServiceCode;
import com.tsurugidb.tsubakuro.sql.ResultSet;

public class SkipColumnTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name
    private static final int SIZE = 10;

    @Before
    public void beforeEach() {
        dropTable(TEST);

        String sql = "create table " + TEST //
                + "(" //
                + " pk int primary key," //
                + " value1 bigint," //
                + " value2 bigint," //
                + " value4 bigint" //
                + ")";
        createTable(sql);
    }

    @Test
    public void testInsert() {
        var csvList = test("insert");
        assertInsert(csvList);
    }

    private void assertInsert(List<String> csvList) {
        var actualList = selectAll();
        assertEquals(csvList.size(), actualList.size());
        int i = 0;
        for (var actual : actualList) {
            var expected = csvList.get(i++);
            String[] as = actual.split(",", -1);
            String[] es = expected.split(",", -1);
            assertEquals(es[0], as[0]); // pk
            assertEquals("", as[1]); // value1
            assertEquals(es[2], as[2]); // value2
            assertEquals("", as[3]); // value4
        }
    }

    @Test
    public void testPut() {
        var e = assertThrows(PartialExecutionException.class, () -> {
            test("put");
        });
        var c = (ServerRuntimeException) e.getCause();
        assertEquals(KvsServiceCode.INCOMPLETE_COLUMNS, c.getCause().getDiagnosticCode());
    }

    private List<String> test(String method) {
        var csvList = new ArrayList<String>();
        for (int i = 0; i < SIZE; i++) {
            String csv = create(i);
            csv += ",999"; // value3_skip
            csvList.add(csv);
        }

        try (var tester = new EmbulkPluginTester()) {
            tester.addOutputPlugin(TsurugiOutputPlugin.TYPE, TsurugiOutputPlugin.class);

            var parser = tester.newParserConfig("csv");
            parser.addColumn("pk", "long");
            parser.addColumn("value1_skip", "long");
            parser.addColumn("value2", "long");
            parser.addColumn("value3_skip", "long");

            var out = tester.newConfigSource();
            out.set("type", TsurugiOutputPlugin.TYPE);
            out.set("endpoint", ENDPOINT);
            out.set("table", TEST);
            out.set("tx_type", "OCC");
            if (method != null) {
                out.set("method", method);
            }

            tester.runOutput(csvList, parser, out);
        }

        return csvList;
    }

    private static String create(int pk) {
        return toCsv(pk, 100L - pk, 1000 + pk);
    }

    private static String toCsv(int pk, Long value1, long value2) {
        return pk + "," + toString(value1) + "," + value2;
    }

    private static String toString(Long value) {
        return (value != null) ? value.toString() : "";
    }

    private List<String> selectAll() {
        var actualList = new ArrayList<String>();
        executeRtx((sqlClient, transaction) -> {
            String sql = "select * from " + TEST + " order by pk";
            try (var rs = executeQuery(transaction, sql)) {
                while (rs.nextRow()) {
                    int pk = fetchInt4(rs);
                    Long value1 = fetchInt8(rs);
                    Long value2 = fetchInt8(rs);
                    Long value4 = fetchInt8(rs);
                    String record = toCsv(pk, value1, value2) + "," + toString(value4);
                    actualList.add(record);
                }
            }
        });
        return actualList;
    }

    private static Integer fetchInt4(ResultSet rs) throws IOException, ServerException, InterruptedException {
        rs.nextColumn();
        if (rs.isNull()) {
            return null;
        }
        return rs.fetchInt4Value();
    }

    private static Long fetchInt8(ResultSet rs) throws IOException, ServerException, InterruptedException {
        rs.nextColumn();
        if (rs.isNull()) {
            return null;
        }
        return rs.fetchInt8Value();
    }
}
