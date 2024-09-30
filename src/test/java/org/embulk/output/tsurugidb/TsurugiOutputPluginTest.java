package org.embulk.output.tsurugidb;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;

public class TsurugiOutputPluginTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name
    private static final int SIZE = 20000;

    @Before
    public void beforeEach() {
        dropTable(TEST);

        String sql = "create table " + TEST //
                + "(" //
                + " pk int primary key," //
                + " long_value bigint," //
                + " double_value double," //
//                + " decimal_value decimal(4,2)," //
                + " string_value varchar(10)" //
                + ")";
        createTable(sql);
    }

    @Test
    public void testDefault() {
        test(null, null, null, null);
    }

    @Test
    public void testInsertOcc() {
        test("OCC", "insert_direct", "insert", null);
    }

    @Test
    public void testInsertLtx() {
        test("LTX", "insert_direct", "insert", null);
    }

    @Test
    public void testInsertOrReplaceOcc() {
        test("OCC", "insert_direct", "insert", "insert_or_replace");
    }

    @Test
    public void testInsertOrReplaceLtx() {
        test("LTX", "insert_direct", "insert", "insert_or_replace");
    }

    @Test
    public void testInsertWaitOcc() {
        test("OCC", "insert_direct", "insert_wait", null);
    }

    @Test
    public void testInsertWaitLtx() {
        test("LTX", "insert_direct", "insert_wait", null);
    }

    @Test
    public void testInsertInsertMultiValueshOcc() {
        test("OCC", "insert_direct", "insert_multi_values", null);
    }

    @Test
    public void testInsertInsertMultiValuesLtx() {
        test("LTX", "insert_direct", "insert_multi_values", null);
    }

    @Test
    public void testInsertOrReplaceInsertMultiValueshOcc() {
        test("OCC", "insert_direct", "insert_multi_values", "insert_or_replace");
    }

    @Test
    public void testInsertOrReplaceInsertMultiValuesLtx() {
        test("LTX", "insert_direct", "insert_multi_values", "insert_or_replace");
    }

    @Test
    @Ignore // TODO remove Ignore. SQL batch実装待ち
    public void testInsertBatchOcc() {
        test("OCC", "insert_direct", "insert_batch", null);
    }

    @Test
    @Ignore // TODO remove Ignore. SQL batch実装待ち
    public void testInsertBatchLtx() {
        test("LTX", "insert_direct", "insert_batch", null);
    }

    @Test
    public void testPutOcc() {
        test("OCC", "insert_direct", "put", null);
    }

    @Test
    @Ignore // TODO remove Ignore. KVS write preserve実装待ち
    public void testPutLtx() {
        test("LTX", "insert_direct", "put", null);
    }

    @Test
    public void testPutWaitOcc() {
        test("OCC", "insert_direct", "put_wait", null);
    }

    @Test
    @Ignore // TODO remove Ignore. KVS write preserve実装待ち
    public void testPutWaitLtx() {
        test("LTX", "insert_direct", "put_wait", null);
    }

    @Test
    @Ignore // TODO remove Ignore. KVS batch実装待ち
    public void testPutBatchOcc() {
        test("OCC", "insert_direct", "put_batch", null);
    }

    @Test
    @Ignore // TODO remove Ignore. KVS batch実装待ち
    public void testPutBatchLTX() {
        test("LTX", "insert_direct", "put_batch", null);
    }

    private void test(String txType, String mode, String method, String methodOption) {
        var entityList = new ArrayList<TestEntity>();
        for (int i = 0; i < SIZE; i++) {
            var entity = new TestEntity(i, 100 - i, i / 10d, BigDecimal.valueOf(i / 10d), "z" + i);
            entityList.add(entity);
        }

        try (var tester = new EmbulkPluginTester()) {
            tester.addOutputPlugin(TsurugiOutputPlugin.TYPE, TsurugiOutputPlugin.class);

            List<String> csvList = entityList.stream().map(TestEntity::toCsv).collect(Collectors.toList());

            var parser = tester.newParserConfig("csv");
            parser.addColumn("pk", "long");
            parser.addColumn("long_value", "long");
            parser.addColumn("double_value", "double");
//            parser.addColumn("decimal_value", "string");
            parser.addColumn("string_value", "string");

            var out = tester.newConfigSource();
            out.set("type", TsurugiOutputPlugin.TYPE);
            out.set("endpoint", ENDPOINT);
            out.set("table", TEST);
            if (txType != null) {
                out.set("tx_type", txType);
            }
            if (mode != null) {
                out.set("mode", mode);
            }
            if (method != null) {
                out.set("method", method);
            }
            if (methodOption != null) {
                out.set("method_option", methodOption);
            }

            tester.runOutput(csvList, parser, out);
        }

        assertTable(entityList);
    }

    private void assertTable(List<TestEntity> expectedList) {
        var actualList = new ArrayList<TestEntity>();
        executeRtx((sqlClient, transaction) -> {
            actualList.clear();
            String sql = "select * from " + TEST + " order by pk";
            try (var rs = executeQuery(transaction, sql)) {
                while (rs.nextRow()) {
                    rs.nextColumn();
                    int pk = rs.fetchInt4Value();
                    rs.nextColumn();
                    long longValue = rs.fetchInt8Value();
                    rs.nextColumn();
                    double doubleValue = rs.fetchFloat8Value();
                    // double doubleValue = pk / 10d;
//                    rs.nextColumn();
//                    BigDecimal decimalValue = rs.fetchDecimalValue();
                    BigDecimal decimalValue = BigDecimal.valueOf(pk / 10d);
                    rs.nextColumn();
                    String stringValue = rs.fetchCharacterValue();
                    actualList.add(new TestEntity(pk, longValue, doubleValue, decimalValue, stringValue));
                }
            }
        });

        assertEquals(expectedList.size(), actualList.size());
        for (int i = 0; i < actualList.size(); i++) {
            var actual = actualList.get(i);
            var expected = expectedList.get(i);
            assertEquals(expected, actual);
        }
    }
}
