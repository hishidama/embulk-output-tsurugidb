package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/ShortColumnSetter.java
public class ShortColumnSetter extends ColumnSetter {
    public ShortColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setShort();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setShort(bindName, v ? (short) 1 : (short) 0);
    }

    @Override
    public void longValue(long v) {
        if (v > Short.MAX_VALUE || v < Short.MIN_VALUE) {
            defaultValue.setShort();
        } else {
            batch.setShort(bindName, (short) v);
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
            defaultValue.setShort();
            return;
        }
        longValue(lv);
    }

    @Override
    public void stringValue(String v) {
        short sv;
        try {
            sv = Short.parseShort(v);
        } catch (NumberFormatException e) {
            defaultValue.setShort();
            return;
        }
        batch.setShort(bindName, sv);
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setShort();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setShort();
    }
}
