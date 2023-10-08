package org.embulk.output.tsurugidb.insert;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.embulk.output.tsurugidb.TsurugiTableSchema;
import org.embulk.output.tsurugidb.common.MergeConfig;
import org.embulk.output.tsurugidb.common.TableIdentifier;
import org.embulk.output.tsurugidb.executor.TsurugiSqlExecutor;

import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.Parameters;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;

//https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/StandardBatchInsert.java
public abstract class TsurugiSqlBatchInsert extends TsurugiBatchInsert {

    protected TsurugiSqlExecutor executor;
    protected PreparedStatement batch;
    private List<Parameter> record;
    protected final List<List<Parameter>> recordList = new ArrayList<>(1024);

    public TsurugiSqlBatchInsert(TsurugiOutputConnector connector, Optional<MergeConfig> mergeConfig) {
        super(connector, mergeConfig);
    }

    @Override
    public void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema, TsurugiOutputConnection connection) throws ServerException {
        this.executor = connection.getSqlExecutor();
        this.batch = prepareStatement(loadTable, insertSchema);
    }

    protected PreparedStatement prepareStatement(TableIdentifier loadTable, TsurugiTableSchema insertSchema) throws ServerException {
        return executor.prepareBatchInsertStatement(loadTable, insertSchema, mergeConfig);
    }

    @Override
    public void beforeAdd() {
        record = new ArrayList<>();
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
    @OverridingMethodsMustInvokeSuper
    public void close() throws ServerException {
        try (var ps = batch) {
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            super.close();
        }
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
        record.add(Parameters.ofNull(bindName));
        nextColumn(0);
    }

    @Override
    public void setBoolean(String bindName, boolean v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(1);
    }

    @Override
    public void setByte(String bindName, byte v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(1);
    }

    @Override
    public void setShort(String bindName, short v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(2);
    }

    @Override
    public void setInt(String bindName, int v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(4);
    }

    @Override
    public void setLong(String bindName, long v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(8);
    }

    @Override
    public void setFloat(String bindName, float v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(4);
    }

    @Override
    public void setDouble(String bindName, double v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(8);
    }

    @Override
    public void setBigDecimal(String bindName, BigDecimal v) {
        // use estimated number of necessary bytes + 8 byte for the weight
        // assuming one place needs 4 bits. ceil(v.precision() / 2.0) + 8
        record.add(Parameters.of(bindName, v));
        nextColumn((v.precision() & ~2) / 2 + 8);
    }

    @Override
    public void setString(String bindName, String v) {
        record.add(Parameters.of(bindName, v));
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    @Override
    public void setNString(String bindName, String v) {
        record.add(Parameters.of(bindName, v));
        // estimate all chracters use 2 bytes; almost enough for the worst case
        nextColumn(v.length() * 2 + 4);
    }

    @Override
    public void setBytes(String bindName, byte[] v) {
        record.add(Parameters.of(bindName, v));
        nextColumn(v.length + 4);
    }

    @Override
    public void setDate(String bindName, final Instant v, final ZoneId zoneId) {
        // JavaDoc of java.sql.Time says:
        // >> To conform with the definition of SQL DATE, the millisecond values wrapped by a java.sql.Date instance must be 'normalized' by setting the hours, minutes, seconds, and milliseconds to
        // zero in the particular time zone with which the instance is associated.
        record.add(Parameters.of(bindName, LocalDate.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setTime(String bindName, final Instant v, final ZoneId zoneId) {
        record.add(Parameters.of(bindName, LocalTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setDateTime(String bindName, final Instant v, final ZoneId zoneId) {
        record.add(Parameters.of(bindName, LocalDateTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setOffsetTime(String bindName, final Instant v, final ZoneId zoneId) {
        record.add(Parameters.of(bindName, OffsetTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }

    @Override
    public void setOffsetDateTime(String bindName, final Instant v, final ZoneId zoneId) {
        record.add(Parameters.of(bindName, OffsetDateTime.ofInstant(v, zoneId)));
        nextColumn(32);
    }
}
