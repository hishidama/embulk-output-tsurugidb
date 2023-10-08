package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/NullColumnSetter.java
public class NullColumnSetter extends ColumnSetter {
    public NullColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void booleanValue(boolean v) {
        defaultValue.setNull();
    }

    @Override
    public void longValue(long v) {
        defaultValue.setNull();
    }

    @Override
    public void doubleValue(double v) {
        defaultValue.setNull();
    }

    @Override
    public void stringValue(String v) {
        defaultValue.setNull();
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setNull();
    }

    @Override
    public void nullValue() {
        defaultValue.setNull();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setNull();
    }
}
