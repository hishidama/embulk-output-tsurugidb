package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/LongColumnSetter.java
public class LongColumnSetter extends ColumnSetter {
    public LongColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setLong();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setLong(bindName, v ? 1L : 0L);
    }

    @Override
    public void longValue(long v) {
        batch.setLong(bindName, v);
    }

    @Override
    public void doubleValue(double v) {
        long lv;
        try {
            // TODO configurable rounding mode
            lv = roundDoubleToLong(v);
        } catch (ArithmeticException ex) {
            // NaN / Infinite / -Infinite
            defaultValue.setLong();
            return;
        }
        batch.setLong(bindName, lv);
    }

    @Override
    public void stringValue(String v) {
        long lv;
        try {
            lv = Long.parseLong(v);
        } catch (NumberFormatException e) {
            defaultValue.setLong();
            return;
        }
        batch.setLong(bindName, lv);
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setLong();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setLong();
    }
}
