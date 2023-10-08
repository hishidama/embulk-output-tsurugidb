package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/ColumnSetter.java
public abstract class ColumnSetter {

    protected final String bindName;
    protected final BatchInsert batch;
    protected final TsurugiColumn column;
    protected final DefaultValueSetter defaultValue;

    public ColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        this.bindName = bindName;
        this.batch = batch;
        this.column = column;
        this.defaultValue = defaultValue;
    }

    public TsurugiColumn getColumn() {
        return column;
    }

    public AtomType getSqlType() {
        return column.getSqlType();
    }

    public abstract void nullValue();

    public abstract void booleanValue(boolean v);

    public abstract void longValue(long v);

    public abstract void doubleValue(double v);

    public abstract void stringValue(String v);

    public abstract void timestampValue(final Instant v);

    public abstract void jsonValue(Value v);

    final long roundDoubleToLong(final double v) {
        if (Math.getExponent(v) > Double.MAX_EXPONENT) {
            throw new ArithmeticException("input is infinite or NaN");
        }
        final double z = Math.rint(v);
        final double result;
        if (Math.abs(v - z) == 0.5) {
            result = v + Math.copySign(0.5, v);
        } else {
            result = z;
        }

        // What we want to confirm here is, in essense :
        //
        // result > ((double) Long.MIN_VALUE) - 1.0 && result < ((double) Long.MAX_VALUE) + 1.0
        //
        // However, |Long.MAX_VALUE| cannot be stored as a double without losing precision.
        // Instead, |Long.MAX_VALUE + 1| can be stored as a double.
        //
        // Then, we use |-0x1p63| for |Long.MIN_VALUE|, and |0x1p63| for |Long.MAX_VALUE + 1|, such as :
        //
        // result > ((double) -0x1p63) - 1.0 && result < ((double) 0x1p63)
        //
        // Then, the inequation is transformed into :
        //
        // result - ((double) -0x1p63) > -1.0 && result < ((double) 0x1p63)
        //
        // And then :
        //
        // ((double) -0x1p63) - result < 1.0 && result < ((double) 0x1p63)
        if (!((-0x1p63) - result < 1.0 && result < (0x1p63))) {
            throw new ArithmeticException("not in range");
        }

        return (long) result;
    }
}
