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

import javax.annotation.OverridingMethodsMustInvokeSuper;

import org.embulk.output.tsurugidb.TsurugiOutputConnection;
import org.embulk.output.tsurugidb.TsurugiOutputConnector;
import org.embulk.output.tsurugidb.TsurugiOutputPlugin.PluginTask;
import org.embulk.output.tsurugidb.TsurugiTableSchema;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.embulk.output.tsurugidb.executor.TsurugiSqlExecutor;
import org.embulk.output.tsurugidb.executor.TsurugiSqlExecutor.MultiValues;

import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;

public class TsurugiBatchInsertInsertMultiValues extends TsurugiBatchInsert {

    private static final int PARAMETER_MAX = 5000;

    protected TsurugiSqlExecutor executor;
    private TableIdentifier loadTable;
    private TsurugiTableSchema insertSchema;
    private int columnSize;

    private final List<MultiValues> valuesList = new ArrayList<>();
    private MultiValues values = null;
    private int recordCount = 0;
    private int bindNameIndex = 0;

    public TsurugiBatchInsertInsertMultiValues(PluginTask task, TsurugiOutputConnector connector, Optional<MergeConfig> mergeConfig) {
        super(task, connector, mergeConfig);
    }

    @Override
    public void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema, TsurugiOutputConnection connection) throws ServerException {
        this.executor = connection.getSqlExecutor();
        this.loadTable = loadTable;
        this.insertSchema = insertSchema;
        this.columnSize = insertSchema.getCount();
    }

    @Override
    public void beforeAdd() {
        if (values == null) {
            values = new MultiValues(PARAMETER_MAX / columnSize);
            valuesList.add(values);
            bindNameIndex = 0;
        }
    }

    @Override
    public void addRecord() {
        recordCount++;
        bindNameIndex++;
        values.incRecordCount();

        if (columnSize * bindNameIndex > PARAMETER_MAX) {
            values = null;
        }
    }

    @Override
    protected int recordListSize() {
        return recordCount;
    }

    @Override
    protected void clearRecordList() {
        recordCount = 0;
        valuesList.clear();
        values = null;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() throws ServerException {
        super.close();
    }

    @Override
    protected int[] executeBatch() throws IOException, ServerException {
        return executor.executeInsertMutliValues(loadTable, insertSchema, mergeConfig, valuesList, recordCount);
    }

    @Override
    public void finish() throws IOException, ServerException {
        if (executor.existsTransaction()) {
            executor.commit();
        }
    }

    @Override
    public void setNull(String bindName) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.ofNull(bindName));
        nextColumn(0);
    }

    @Override
    public void setBoolean(String bindName, boolean v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(1);
    }

    @Override
    public void setByte(String bindName, byte v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(1);
    }

    @Override
    public void setShort(String bindName, short v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(2);
    }

    @Override
    public void setInt(String bindName, int v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(4);
    }

    @Override
    public void setLong(String bindName, long v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(8);
    }

    @Override
    public void setFloat(String bindName, float v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(4);
    }

    @Override
    public void setDouble(String bindName, double v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(8);
    }

    @Override
    public void setBigDecimal(String bindName, BigDecimal v) {
        bindName += "_" + bindNameIndex;
        // use estimated number of necessary bytes + 8 byte for the weight
        // assuming one place needs 4 bits. ceil(v.precision() / 2.0) + 8
        values.add(Parameters.of(bindName, v));
        nextColumn((v.precision() & ~2) / 2 + 8);
    }

    @Override
    public void setString(String bindName, String v) {
        bindName += "_" + bindNameIndex;
        checkNulChar(bindName, v);
        values.add(Parameters.of(bindName, v));
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    @Override
    public void setNString(String bindName, String v) {
        bindName += "_" + bindNameIndex;
        checkNulChar(bindName, v);
        values.add(Parameters.of(bindName, v));
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    @Override
    public void setBytes(String bindName, byte[] v) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, v));
        nextColumn(v.length + 4);
    }

    @Override
    public void setDate(String bindName, final Instant v, final ZoneId zoneId) {
        bindName += "_" + bindNameIndex;
        // JavaDoc of java.sql.Time says:
        // >> To conform with the definition of SQL DATE, the millisecond values wrapped by a java.sql.Date instance must be 'normalized' by setting the hours, minutes, seconds, and milliseconds to
        // zero in the particular time zone with which the instance is associated.
        values.add(Parameters.of(bindName, LocalDate.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setTime(String bindName, final Instant v, final ZoneId zoneId) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, LocalTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setDateTime(String bindName, final Instant v, final ZoneId zoneId) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, LocalDateTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setOffsetTime(String bindName, final Instant v, final ZoneId zoneId) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, OffsetTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setOffsetDateTime(String bindName, final Instant v, final ZoneId zoneId) {
        bindName += "_" + bindNameIndex;
        values.add(Parameters.of(bindName, OffsetDateTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }
}
