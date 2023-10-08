package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/ByteColumnSetter.java
public class ByteColumnSetter extends ColumnSetter {
    public ByteColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setByte();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setByte(bindName, v ? (byte) 1 : (byte) 0);
    }

    @Override
    public void longValue(long v) {
        if (v > Byte.MAX_VALUE || v < Byte.MIN_VALUE) {
            defaultValue.setByte();
        } else {
            batch.setByte(bindName, (byte) v);
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
            defaultValue.setByte();
            return;
        }
        longValue(lv);
    }

    @Override
    public void stringValue(String v) {
        byte sv;
        try {
            sv = Byte.parseByte(v);
        } catch (NumberFormatException e) {
            defaultValue.setByte();
            return;
        }
        batch.setByte(bindName, sv);
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setByte();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setByte();
    }
}
