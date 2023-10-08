package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/FloatColumnSetter.java
public class FloatColumnSetter extends ColumnSetter {
    public FloatColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setFloat();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setFloat(bindName, v ? 1f : 0f);
    }

    @Override
    public void longValue(long v) {
        batch.setFloat(bindName, v);
    }

    @Override
    public void doubleValue(double v) {
        batch.setFloat(bindName, (float) v);
    }

    @Override
    public void stringValue(String v) {
        float fv;
        try {
            fv = Float.parseFloat(v);
        } catch (NumberFormatException e) {
            defaultValue.setFloat();
            return;
        }
        batch.setFloat(bindName, fv);
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setFloat();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setFloat();
    }
}
