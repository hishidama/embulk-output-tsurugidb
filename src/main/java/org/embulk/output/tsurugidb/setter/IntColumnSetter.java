package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/IntColumnSetter.java
public class IntColumnSetter extends ColumnSetter {
    public IntColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setInt();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setInt(bindName, v ? 1 : 0);
    }

    @Override
    public void longValue(long v) {
        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
            defaultValue.setInt();
        } else {
            batch.setInt(bindName, (int) v);
        }
    }

    @Override
    public void doubleValue(double v) {
        long lv;
        try {
            // TODO configurable rounding mode
            lv = roundDoubleToLong(v);
        } catch (ArithmeticException ex) {
            // NaN / Infinite / -Infinite
            defaultValue.setInt();
            return;
        }
        longValue(lv);
    }

    @Override
    public void stringValue(String v) {
        int iv;
        try {
            iv = Integer.parseInt(v);
        } catch (NumberFormatException e) {
            defaultValue.setInt();
            return;
        }
        batch.setInt(bindName, iv);
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setInt();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setInt();
    }
}
