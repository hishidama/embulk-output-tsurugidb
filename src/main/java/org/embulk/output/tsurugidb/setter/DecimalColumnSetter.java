package org.embulk.output.tsurugidb.setter;

import java.math.BigDecimal;
import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/BigDecimalColumnSetter.java
public class DecimalColumnSetter extends ColumnSetter {

    public DecimalColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setBigDecimal();
    }

    @Override
    public void booleanValue(boolean v) {
        batch.setBigDecimal(bindName, v ? BigDecimal.ONE : BigDecimal.ZERO);
    }

    @Override
    public void longValue(long v) {
        batch.setBigDecimal(bindName, BigDecimal.valueOf(v));
    }

    @Override
    public void doubleValue(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            defaultValue.setBigDecimal();
        } else {
            batch.setBigDecimal(bindName, BigDecimal.valueOf(v));
        }
    }

    @Override
    public void stringValue(String v) {
        BigDecimal dv;
        try {
            dv = new BigDecimal(v);
        } catch (NumberFormatException ex) {
            defaultValue.setBigDecimal();
            return;
        }
        batch.setBigDecimal(bindName, dv);
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setBigDecimal();
    }

    @Override
    public void jsonValue(Value v) {
        defaultValue.setBigDecimal();
    }
}
