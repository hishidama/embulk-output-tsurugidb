package org.embulk.output.tsurugidb;

import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.embulk.exec.PartialExecutionException;
import org.junit.Before;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.KvsServiceException;
import com.tsurugidb.tsubakuro.sql.exception.ValueTooLongException;

public class TsurugiOutputPluginErrorTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name
    private static final int SIZE = 2;

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
    public void testInsertOcc() {
        testSql("OCC", "insert_direct", "insert", null);
    }

    @Test
    public void testInsertWaitOcc() {
        testSql("OCC", "insert_direct", "insert_wait", null);
    }

    private void testSql(String txType, String mode, String method, String methodOption) {
        test(txType, mode, method, methodOption, ValueTooLongException.class);
    }

    @Test
    public void testPutOcc() {
        testKvs("OCC", "insert_direct", "put", null);
    }

    @Test
    public void testPutWaitOcc() {
        testKvs("OCC", "insert_direct", "put_wait", null);
    }

    private void testKvs(String txType, String mode, String method, String methodOption) {
        test(txType, mode, method, methodOption, KvsServiceException.class);
    }

    private void test(String txType, String mode, String method, String methodOption, Class<? extends ServerException> expectedClass) {
        try (var tester = new EmbulkPluginTester()) {
            tester.addOutputPlugin(TsurugiOutputPlugin.TYPE, TsurugiOutputPlugin.class);

            var csvList = new ArrayList<String>();
            for (int i = 0; i < SIZE; i++) {
                String csv = i + "," + Integer.toString(i).repeat(5);
                csvList.add(csv);
            }

            var parser = tester.newParserConfig("csv");
            parser.addColumn("pk", "long");
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
            out.set("log_level_on_record_error", "WARN");
            out.set("stop_on_record_error", true);

            try {
                tester.runOutput(csvList, parser, out);
            } catch (PartialExecutionException e) {
                var r = e.getCause();
                if (!(r instanceof ServerRuntimeException)) {
                    throw e;
                }
                var c = r.getCause();
                if (c.getClass() != expectedClass) {
                    throw e;
                }
                return;
            }
            fail("error not occurred");
        }
    }
}
