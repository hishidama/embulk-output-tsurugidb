package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/DoubleColumnSetter.java
public class DoubleColumnSetter extends ColumnSetter {
    public DoubleColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setDouble();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setDouble(bindName, v ? 1.0 : 0.0);
    }

    @Override
    public void longValue(long v) {
        batch.setDouble(bindName, v);
    }

    @Override
    public void doubleValue(double v) {
        batch.setDouble(bindName, v);
    }

    @Override
    public void stringValue(String v) {
        double dv;
        try {
            dv = Double.parseDouble(v);
        } catch (NumberFormatException e) {
            defaultValue.setDouble();
            return;
        }
        batch.setDouble(bindName, dv);
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setDouble();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setDouble();
    }
}
