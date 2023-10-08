package org.embulk.output.tsurugidb.setter;

import java.time.Instant;
import java.time.ZoneId;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/PassThroughColumnSetter.java
public class PassThroughColumnSetter extends AbstractTimestampColumnSetter {

    public PassThroughColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue, ZoneId zoneId) {
        super(bindName, batch, column, defaultValue, zoneId);
    }

    @Override
    public void nullValue() {
        batch.setNull(bindName);
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setBoolean(bindName, v);
    }

    @Override
    public void longValue(long v) {
        batch.setLong(bindName, v);
    }

    @Override
    public void doubleValue(double v) {
        batch.setDouble(bindName, v);
    }

    @Override
    public void stringValue(String v) {
        batch.setString(bindName, v);
    }

    @Override
    public void timestampValue(final Instant v) {
        batch.setDateTime(bindName, v, zoneId);
    }

    @Override
    public void jsonValue(Value v) {
        batch.setString(bindName, v.toJson());
    }
}
