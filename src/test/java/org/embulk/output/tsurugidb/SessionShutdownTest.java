package org.embulk.output.tsurugidb;

import java.util.ArrayList;

import org.embulk.output.tsurugidb.option.TsurugiSessionShutdownType;
import org.junit.Before;
import org.junit.Test;

import com.hishidama.embulk.tester.EmbulkPluginTester;

public class SessionShutdownTest extends TsurugiTestTool {

    private static final String TEST = "test"; // table name

    @Before
    public void beforeEach() {
        dropTable(TEST);

        String sql = "create table " + TEST //
                + "(" //
                + " pk int primary key," //
                + " string_value varchar(10)" //
                + ")";
        createTable(sql);
    }

    @Test
    public void testNothing() {
        test(TsurugiSessionShutdownType.NOTHING);
    }

    @Test
    public void testGraceful() {
        test(TsurugiSessionShutdownType.GRACEFUL);
    }

    @Test
    public void testForceful() {
        test(TsurugiSessionShutdownType.FORCEFUL);
    }

    private void test(TsurugiSessionShutdownType shutdownType) {
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
            out.set("session_shutdown_type", shutdownType.toString());

            tester.runOutput(csvList, parser, out);
        }
    }
}
