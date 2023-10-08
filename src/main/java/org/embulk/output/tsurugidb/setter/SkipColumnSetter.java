package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/SkipColumnSetter.java
public class SkipColumnSetter extends ColumnSetter {
    public SkipColumnSetter(String bindName, BatchInsert batch) {
        super(bindName, batch, null, null);
    }

    @Override
    public void booleanValue(boolean v) {
    }

    @Override
    public void longValue(long v) {
    }

    @Override
    public void doubleValue(double v) {
    }

    @Override
    public void stringValue(String v) {
    }

    @Override
    public void timestampValue(final Instant v) {
    }

    @Override
    public void nullValue() {
    }

    @Override
    public void jsonValue(Value v) {
    }
}
