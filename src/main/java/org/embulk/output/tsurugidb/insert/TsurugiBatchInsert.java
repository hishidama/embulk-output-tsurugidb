package org.embulk.output.tsurugidb.insert;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import org.embulk.output.tsurugidb.TsurugiOutputConnection;
import org.embulk.output.tsurugidb.TsurugiOutputConnector;
import org.embulk.output.tsurugidb.TsurugiTableSchema;
import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.embulk.output.tsurugidb.executor.LogLevelOnError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;

//https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/StandardBatchInsert.java
public abstract class TsurugiBatchInsert implements BatchInsert {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiBatchInsert.class);

    private final PluginTask task;
    private final TsurugiOutputConnector connector;
    protected final Optional<MergeConfig> mergeConfig;
    protected final boolean isValidateNulChar;

    private TsurugiOutputConnection connection;
    private int batchWeight;
    private long totalRows;
    private int[] lastUpdateCounts;

    protected List<String> skipReasonList;

    public TsurugiBatchInsert(PluginTask task, TsurugiOutputConnector connector, Optional<MergeConfig> mergeConfig) {
        this.task = task;
        this.connector = connector;
        this.mergeConfig = mergeConfig;
        this.isValidateNulChar = task.getValidateNulChar();
    }

    @Override
    public final void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema) throws ServerException {
        this.connection = connector.connect(true);
        this.totalRows = 0;
        prepare(loadTable, insertSchema, connection);
        clearRecordList();
        this.skipReasonList = null;
    }

    protected abstract void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema, TsurugiOutputConnection connection) throws ServerException;

    @Override
    public int getBatchWeight() {
        return batchWeight;
    }

    protected void checkNulChar(String bindName, String value) {
        if (!this.isValidateNulChar) {
            return;
        }

        if (value == null) {
            return;
        }
        if (value.indexOf('\0') >= 0) {
            String r = value.replace("\0", "\\0");
            processInvalidData("nul-char found", bindName, r);
        }
    }

    protected void processInvalidData(String reason, String bindName, Object value) {
        BiConsumer<String, Object[]> logMethod;
        boolean withStackTrace = false;
        LogLevelOnError logLevel = task.getLogLevelOnInvalidRecord();
        switch (logLevel) {
        case NONE:
            logMethod = (f, args) -> {
            };
            break;
        case DEBUG:
            logMethod = logger::debug;
            break;
        case DEBUG_STACKTRACE:
            logMethod = logger::debug;
            withStackTrace = true;
            break;
        case INFO:
            logMethod = logger::info;
            break;
        case WARN:
            logMethod = logger::warn;
            break;
        case ERROR:
            logMethod = logger::error;
            break;
        default:
            throw new AssertionError("invalid log_level_on_invalid_record=" + logLevel);
        }

        boolean isStop = task.getStopOnInvalidRecord();
        RuntimeException e = null;
        if (isStop || withStackTrace) {
            e = new RuntimeException(MessageFormat.format("{0}. column={1}", reason, bindName));
        }

        if (withStackTrace) {
            logMethod.accept("invalid data. reason={}, name={}, value={}", new Object[] { reason, bindName, value, e });
        } else {
            logMethod.accept("invalid data. reason={}, name={}, value={}", new Object[] { reason, bindName, value });
        }

        if (skipReasonList == null) {
            this.skipReasonList = new ArrayList<>();
        }
        skipReasonList.add(String.format("%s: %s", bindName, reason));
        if (isStop) {
            throw e;
        }
    }

    @Override
    public final void add() {
        if (this.skipReasonList != null) {
            logger.warn("Skipped line. reason={}", skipReasonList);

            this.skipReasonList = null;
        } else {
            addRecord();
            batchWeight += 32; // add weight as overhead of each rows
        }
    }

    protected abstract void addRecord();

    protected abstract int recordListSize();

    protected abstract void clearRecordList();

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws ServerException {
        try (var c = connection) {
            // close only
        }
    }

    @Override
    public void flush() throws IOException, ServerException {
        lastUpdateCounts = new int[] {};

        if (batchWeight == 0) {
            return;
        }

        int batchRows = recordListSize();
        logger.info(String.format("Loading %,d rows", batchRows));

        long startTime = System.currentTimeMillis();
        try {
            lastUpdateCounts = executeBatch();
        } catch (ServerException e) {
            // will be used for retry
            throw e;

        } finally {
            // clear for retry
            clearRecordList();
            batchWeight = 0;
        }
        long endTime = System.currentTimeMillis();

        double seconds = (endTime - startTime) / 1000.0;
        int successRows = 0;
        for (int count : lastUpdateCounts) {
            successRows += count;
        }
        if (successRows != batchRows) {
            logger.info(String.format("Loaded  %,d rows", successRows));
        }
        totalRows += successRows;
        logger.info(String.format("> %.2f seconds (loaded %,d rows in total)", seconds, totalRows));
    }

    protected abstract int[] executeBatch() throws IOException, ServerException;

    @Override
    public int[] getLastUpdateCounts() {
        return lastUpdateCounts;
    }

    protected void nextColumn(int weight) {
        batchWeight += weight + 4; // add weight as overhead of each columns
    }
}
