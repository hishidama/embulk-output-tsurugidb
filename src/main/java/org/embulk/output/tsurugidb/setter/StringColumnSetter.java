package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/StringColumnSetter.java
public class StringColumnSetter extends ColumnSetter {
    private final TimestampFormatter timestampFormatter;

    public StringColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue, TimestampFormatter timestampFormatter) {
        super(bindName, batch, column, defaultValue);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void nullValue() {
        defaultValue.setString();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setString(bindName, Boolean.toString(v));
    }

    @Override
    public void longValue(long v) {
        batch.setString(bindName, Long.toString(v));
    }

    @Override
    public void doubleValue(double v) {
        batch.setString(bindName, Double.toString(v));
    }

    @Override
    public void stringValue(String v) {
        batch.setString(bindName, v);
    }

    @Override
    public void timestampValue(final Instant v) {
        batch.setString(bindName, timestampFormatter.format(v));
    }

    @Override
    public void jsonValue(Value v) {
        batch.setString(bindName, v.toJson());
    }
}
