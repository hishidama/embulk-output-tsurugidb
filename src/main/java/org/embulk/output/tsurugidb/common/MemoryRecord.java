package org.embulk.output.tsurugidb.common;

import java.time.Instant;

import org.embulk.output.tsurugidb.setter.Record;
import org.embulk.spi.Column;
import org.msgpack.value.Value;

public class MemoryRecord implements Record {
    private final Object[] values;

    public MemoryRecord(int columnCount) {
        values = new Object[columnCount];
    }

    @Override
    public boolean isNull(Column column) {
        return getValue(column) == null;
    }

    @Override
    public boolean getBoolean(Column column) {
        return (Boolean) getValue(column);
    }

    @Override
    public long getLong(Column column) {
        return (Long) getValue(column);
    }

    @Override
    public double getDouble(Column column) {
        return (Double) getValue(column);
    }

    @Override
    public String getString(Column column) {
        return (String) getValue(column);
    }

    @Override
    public Instant getTimestamp(Column column) {
        return (Instant) getValue(column);
    }

    @Override
    public Value getJson(Column column) {
        return (Value) getValue(column);
    }

    private Object getValue(Column column) {
        return values[column.getIndex()];
    }

    public void setValue(Column column, Object value) {
        values[column.getIndex()] = value;
    }
}
