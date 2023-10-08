package org.embulk.output.tsurugidb.setter;

import java.time.Instant;
import java.time.ZoneId;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/SqlTimeColumnSetter.java
public class OffsetTimeColumnSetter extends AbstractTimestampColumnSetter {

    public OffsetTimeColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue, ZoneId zoneId) {
        super(bindName, batch, column, defaultValue, zoneId);
    }

    @Override
    public void nullValue() {
        defaultValue.setOffsetTime();
    }

    @Override
    public void booleanValue(boolean v) {
        defaultValue.setOffsetTime();
    }

    @Override
    public void longValue(long v) {
        defaultValue.setOffsetTime();
    }

    @Override
    public void doubleValue(double v) {
        defaultValue.setOffsetTime();
    }

    @Override
    public void stringValue(String v) {
        defaultValue.setOffsetTime();
    }

    @Override
    public void timestampValue(final Instant v) {
        batch.setTime(bindName, v, zoneId);
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setOffsetTime();
    }
}
