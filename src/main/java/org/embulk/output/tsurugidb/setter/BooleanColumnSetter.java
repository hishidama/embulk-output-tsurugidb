package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/BooleanColumnSetter.java
public class BooleanColumnSetter extends ColumnSetter {
    public BooleanColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setBoolean();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setBoolean(bindName, v);
    }

    @Override
    public void longValue(long v) {
        batch.setBoolean(bindName, v > 0);
    }

    @Override
    public void doubleValue(double v) {
        batch.setBoolean(bindName, v > 0.0);
    }

    @Override
    public void stringValue(String v) {
        defaultValue.setBoolean();
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setBoolean();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setBoolean();
    }
}
