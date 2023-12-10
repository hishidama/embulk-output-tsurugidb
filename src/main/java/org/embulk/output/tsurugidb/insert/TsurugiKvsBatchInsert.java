package org.embulk.output.tsurugidb.insert;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.embulk.output.tsurugidb.TsurugiOutputConnection;
import org.embulk.output.tsurugidb.TsurugiOutputConnector;
import org.embulk.output.tsurugidb.TsurugiTableSchema;
import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.embulk.output.tsurugidb.executor.TsurugiKvsExecutor;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;

public abstract class TsurugiKvsBatchInsert extends TsurugiBatchInsert {

    protected String tableName;
    protected TsurugiKvsExecutor executor;
    private RecordBuffer record;
    protected final List<RecordBuffer> recordList = new ArrayList<>(1024);

    public TsurugiKvsBatchInsert(PluginTask task, TsurugiOutputConnector connector, Optional<MergeConfig> mergeConfig) {
        super(task, connector, mergeConfig);
    }

    @Override
    public void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema, TsurugiOutputConnection connection) throws ServerException {
        this.tableName = loadTable.getTableName();
        this.executor = connection.getKvsExecutor();
    }

    @Override
    public void beforeAdd() {
        record = new RecordBuffer();
    }

    @Override
    public void addRecord() {
        recordList.add(record);
    }

    @Override
    protected int recordListSize() {
        return recordList.size();
    }

    @Override
    protected void clearRecordList() {
        record = null;
        recordList.clear();
    }

    @Override
    protected abstract int[] executeBatch() throws IOException, ServerException;

    @Override
    public void finish() throws IOException, ServerException {
        if (executor.existsTransaction()) {
            executor.commit();
        }
    }

    @Override
    public void setNull(String bindName) {
        record.addNull(bindName);
        nextColumn(0);
    }

    @Override
    public void setBoolean(String bindName, boolean v) {
        record.add(bindName, v);
        nextColumn(1);
    }

    @Override
    public void setByte(String bindName, byte v) {
        record.add(bindName, v);
        nextColumn(1);
    }

    @Override
    public void setShort(String bindName, short v) {
        record.add(bindName, v);
        nextColumn(2);
    }

    @Override
    public void setInt(String bindName, int v) {
        record.add(bindName, v);
        nextColumn(4);
    }

    @Override
    public void setLong(String bindName, long v) {
        record.add(bindName, v);
        nextColumn(8);
    }

    @Override
    public void setFloat(String bindName, float v) {
        record.add(bindName, v);
        nextColumn(4);
    }

    @Override
    public void setDouble(String bindName, double v) {
        record.add(bindName, v);
        nextColumn(8);
    }

    @Override
    public void setBigDecimal(String bindName, BigDecimal v) {
        // use estimated number of necessary bytes + 8 byte for the weight
        // assuming one place needs 4 bits. ceil(v.precision() / 2.0) + 8
        record.add(bindName, v);
        nextColumn((v.precision() & ~2) / 2 + 8);
    }

    @Override
    public void setString(String bindName, String v) {
        checkNulChar(bindName, v);
        record.add(bindName, v);
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    @Override
    public void setNString(String bindName, String v) {
        checkNulChar(bindName, v);
        record.add(bindName, v);
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    @Override
    public void setBytes(String bindName, byte[] v) {
        record.add(bindName, v);
        nextColumn(v.length + 4);
    }

    @Override
    public void setDate(String bindName, final Instant v, final ZoneId zoneId) {
        // JavaDoc of java.sql.Time says:
        // >> To conform with the definition of SQL DATE, the millisecond values wrapped by a java.sql.Date instance must be 'normalized' by setting the hours, minutes, seconds, and milliseconds to
        // zero in the particular time zone with which the instance is associated.
        var date = LocalDate.ofInstant(v, zoneId);
        record.add(bindName, date);
        nextColumn(32);
    }

    @Override
    public void setTime(String bindName, final Instant v, final ZoneId zoneId) {
        var time = LocalTime.ofInstant(v, zoneId);
        record.add(bindName, time);
        nextColumn(32);
    }

    @Override
    public void setDateTime(String bindName, final Instant v, final ZoneId zoneId) {
        var dateTime = LocalDateTime.ofInstant(v, zoneId);
        record.add(bindName, dateTime);
        nextColumn(32);
    }

    @Override
    public void setOffsetTime(String bindName, final Instant v, final ZoneId zoneId) {
        var time = OffsetTime.ofInstant(v, zoneId);
        record.add(bindName, time);
        nextColumn(32);
    }

    @Override
    public void setOffsetDateTime(String bindName, final Instant v, final ZoneId zoneId) {
        var dateTime = OffsetDateTime.ofInstant(v, zoneId);
        record.add(bindName, dateTime);
        nextColumn(32);
    }
}
