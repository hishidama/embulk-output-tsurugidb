package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

public class ClobColumnSetter extends ColumnSetter {
    private final TimestampFormatter timestampFormatter;

    public ClobColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue, TimestampFormatter timestampFormatter) {
        super(bindName, batch, column, defaultValue);
        this.timestampFormatter = timestampFormatter;
    }

    @Override
    public void nullValue() {
        defaultValue.setString();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setClob(bindName, Boolean.toString(v));
    }

    @Override
    public void longValue(long v) {
        batch.setClob(bindName, Long.toString(v));
    }

    @Override
    public void doubleValue(double v) {
        batch.setClob(bindName, Double.toString(v));
    }

    @Override
    public void stringValue(String v) {
        batch.setClob(bindName, v);
    }

    @Override
    public void timestampValue(final Instant v) {
        batch.setClob(bindName, timestampFormatter.format(v));
    }

    @Override
    public void jsonValue(Value v) {
        batch.setClob(bindName, v.toJson());
    }
}
