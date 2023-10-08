package org.embulk.output.tsurugidb.insert;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import org.embulk.output.tsurugidb.TsurugiOutputConnection;
import org.embulk.output.tsurugidb.TsurugiOutputConnector;
import org.embulk.output.tsurugidb.TsurugiTableSchema;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.tsubakuro.exception.ServerException;

//https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/StandardBatchInsert.java
public abstract class TsurugiBatchInsert implements BatchInsert {
    private static final Logger logger = LoggerFactory.getLogger(TsurugiBatchInsert.class);

    private final TsurugiOutputConnector connector;
    protected final Optional<MergeConfig> mergeConfig;

    private TsurugiOutputConnection connection;
    private int batchWeight;
    private long totalRows;
    private int[] lastUpdateCounts;

    public TsurugiBatchInsert(TsurugiOutputConnector connector, Optional<MergeConfig> mergeConfig) {
        this.connector = connector;
        this.mergeConfig = mergeConfig;
    }

    @Override
    public final void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema) throws ServerException {
        this.connection = connector.connect(true);
        this.totalRows = 0;
        prepare(loadTable, insertSchema, connection);
        clearRecordList();
    }

    protected abstract void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema, TsurugiOutputConnection connection) throws ServerException;

    @Override
    public int getBatchWeight() {
        return batchWeight;
    }

    @Override
    public final void add() {
        addRecord();
        batchWeight += 32; // add weight as overhead of each rows
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
        totalRows += batchRows;
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
