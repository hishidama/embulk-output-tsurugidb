package org.embulk.output.tsurugidb.insert;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;

import org.embulk.output.tsurugidb.TsurugiTableSchema;
import org.embulk.output.tsurugidb.common.TableIdentifier;

import com.tsurugidb.tsubakuro.exception.ServerException;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/BatchInsert.java
public interface BatchInsert {
    public void prepare(TableIdentifier loadTable, TsurugiTableSchema insertSchema) throws ServerException;

    public int getBatchWeight();

    public void beforeAdd();

    public void add() throws IOException, ServerException;

    public void close() throws IOException, ServerException;

    public void flush() throws IOException, ServerException;

    // should be implemented for retry
    public int[] getLastUpdateCounts();

    public void finish() throws IOException, ServerException;

    public void setNull(String bindName);

    public void setBoolean(String bindName, boolean v);

    public void setByte(String bindName, byte v);

    public void setShort(String bindName, short v);

    public void setInt(String bindName, int v);

    public void setLong(String bindName, long v);

    public void setFloat(String bindName, float v);

    public void setDouble(String bindName, double v);

    public void setBigDecimal(String bindName, BigDecimal v);

    public void setString(String bindName, String v);

    public void setNString(String bindName, String v);

    public void setBytes(String bindName, byte[] v);

    public void setDate(String bindName, Instant v, ZoneId zoneId);

    public void setTime(String bindName, Instant v, ZoneId zoneId);

    public void setDateTime(String bindName, Instant v, ZoneId zoneId);

    public void setOffsetTime(String bindName, Instant v, ZoneId zoneId);

    public void setOffsetDateTime(String bindName, Instant v, ZoneId zoneId);
}
