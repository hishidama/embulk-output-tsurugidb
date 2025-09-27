package org.embulk.output.tsurugidb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.function.Consumer;

import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.junit.Before;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.sql.exception.InvalidRuntimeValueException;

public class ValidateNulCharTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name

    @Before
    public void beforeEach() {
        dropTable(TEST);

        String sql = "create table " + TEST //
                + "(" //
                + " pk int primary key," //
                + " string_value varchar(4)" //
                + ")";
        createTable(sql);
    }

    @Test
    public void testInsert() {
        test("OCC", "insert_direct", "insert", null, out -> {
        }, null, null);
    }

    @Test
    public void testInsertValidate() {
        test("OCC", "insert_direct", "insert", null, out -> {
            out.set("validate_nul_char", true);
            out.set("log_level_on_invalid_record", "WARN");
            out.set("stop_on_invalid_record", false);
        }, null, null);
    }

    @Test
    public void testInsertValidateStop() {
        test("OCC", "insert_direct", "insert", null, out -> {
            out.set("validate_nul_char", true);
            out.set("log_level_on_invalid_record", "WARN");
            out.set("stop_on_invalid_record", true);
        }, RuntimeException.class, "nul-char found. column=string_value");
    }

    @Test
    public void testInsertNoValidate() {
        test("OCC", "insert_direct", "insert", null, out -> {
            out.set("validate_nul_char", false);
            out.set("log_level_on_invalid_record", "WARN");
            out.set("stop_on_invalid_record", false);
        }, InvalidRuntimeValueException.class, null);
    }

    @Test
    public void testPut() {
        test("OCC", "insert_direct", "put", null, out -> {
        }, null, null);
    }

    @Test
    public void testPutValidate() {
        test("OCC", "insert_direct", "put", null, out -> {
            out.set("validate_nul_char", true);
            out.set("log_level_on_invalid_record", "WARN");
            out.set("stop_on_invalid_record", false);
        }, null, null);
    }

    @Test
    public void testPutValidateStop() {
        test("OCC", "insert_direct", "put", null, out -> {
            out.set("validate_nul_char", true);
            out.set("log_level_on_invalid_record", "WARN");
            out.set("stop_on_invalid_record", true);
        }, RuntimeException.class, "nul-char found. column=string_value");
    }

    @Test
    public void testPutNoValidate() {
        test("OCC", "insert_direct", "put", null, out -> {
            out.set("validate_nul_char", false);
            out.set("log_level_on_invalid_record", "WARN");
            out.set("stop_on_invalid_record", false);
        }, KvsServiceException.class, null);
    }

    private void test(String txType, String mode, String method, String methodOption, Consumer<ConfigSource> initializer, Class<?> expectedClass, String expectedMessage) {
        try (var tester = new EmbulkPluginTester()) {
            tester.addOutputPlugin(TsurugiOutputPlugin.TYPE, TsurugiOutputPlugin.class);

            var csvList = new ArrayList<String>();
            csvList.add("1,aaaa");
            csvList.add("2,bb\0b");
            csvList.add("3,cccc");

            var parser = tester.newParserConfig("csv");
            parser.addColumn("pk", "long");
            parser.addColumn("string_value", "string");

            var out = tester.newConfigSource();
            out.set("type", TsurugiOutputPlugin.TYPE);
            out.set("endpoint", ENDPOINT);
            setCredential(out);
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
            initializer.accept(out);
            out.set("stop_on_record_error", true);

            try {
                tester.runOutput(csvList, parser, out);
            } catch (PartialExecutionException e) {
                if (expectedClass == null) {
                    throw e;
                }

                var r = e.getCause();
                if (expectedClass == RuntimeException.class) {
                    assertEquals(expectedClass, r.getClass());
                    assertEquals(expectedMessage, r.getMessage());
                    return;
                }

                if (!(r instanceof ServerRuntimeException)) {
                    throw e;
                }
                var c = r.getCause();
                if (c.getClass() != expectedClass) {
                    throw e;
                }

                return;
            }

            if (expectedClass != null) {
                fail("error not occurred");
            }
            assertTable();
        }
    }

    private void assertTable() {
        var actualList = new ArrayList<TestEntity>();
        executeRtx((sqlClient, transaction) -> {
            actualList.clear();
            String sql = "select * from " + TEST + " order by pk";
            try (var rs = executeQuery(transaction, sql)) {
                for (int i = 0; rs.nextRow(); i++) {
                    rs.nextColumn();
                    int pk = rs.fetchInt4Value();
                    rs.nextColumn();
                    String stringValue = rs.fetchCharacterValue();

                    switch (i) {
                    case 0:
                        assertEquals(1, pk);
                        assertEquals("aaaa", stringValue);
                        break;
                    case 1:
                        assertEquals(3, pk);
                        assertEquals("cccc", stringValue);
                        break;
                    default:
                        fail("select size error");
                    }
                }
            }
        });
    }
}
