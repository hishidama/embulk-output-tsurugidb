package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/NStringColumnSetter.java
public class NStringColumnSetter extends ColumnSetter {
    private final TimestampFormatter timestampFormatter;

    public NStringColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue, TimestampFormatter timestampFormatter) {
        super(bindName, batch, column, defaultValue);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void nullValue() {
        defaultValue.setNString();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setNString(bindName, Boolean.toString(v));
    }

    @Override
    public void longValue(long v) {
        batch.setNString(bindName, Long.toString(v));
    }

    @Override
    public void doubleValue(double v) {
        batch.setNString(bindName, Double.toString(v));
    }

    @Override
    public void stringValue(String v) {
        batch.setNString(bindName, v);
    }

    @Override
    public void timestampValue(final Instant v) {
        batch.setNString(bindName, timestampFormatter.format(v));
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setNString();
    }
}
