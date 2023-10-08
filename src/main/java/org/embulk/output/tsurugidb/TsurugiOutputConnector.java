package org.embulk.output.tsurugidb;

import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;

import com.tsurugidb.tsubakuro.exception.ServerException;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/AbstractJdbcOutputConnector.java
// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-postgresql/src/main/java/org/embulk/output/postgresql/PostgreSQLOutputConnector.java
public class TsurugiOutputConnector {

    private PluginTask task;

    public TsurugiOutputConnector(PluginTask task) {
        this.task = task;
    }

    public TsurugiOutputConnection connect(boolean autoCommit) throws ServerException {
        return TsurugiOutputConnection.newConnection(task, true, autoCommit);
    }
}
