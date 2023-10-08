package org.embulk.output.tsurugidb.setter;

import java.time.Instant;
import java.time.ZoneId;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/SqlTimestampColumnSetter.java
public class DateTimeColumnSetter extends AbstractTimestampColumnSetter {

    public DateTimeColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue, ZoneId zoneId) {
        super(bindName, batch, column, defaultValue, zoneId);
    }

    @Override
    public void nullValue() {
        defaultValue.setDateTime();
    }

    @Override
    public void booleanValue(boolean v) {
        defaultValue.setDateTime();
    }

    @Override
    public void longValue(long v) {
        defaultValue.setDateTime();
    }

    @Override
    public void doubleValue(double v) {
        defaultValue.setDateTime();
    }

    @Override
    public void stringValue(String v) {
        defaultValue.setDateTime();
    }

    @Override
    public void timestampValue(final Instant v) {
        batch.setDateTime(bindName, v, zoneId);
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setDateTime();
    }
}
