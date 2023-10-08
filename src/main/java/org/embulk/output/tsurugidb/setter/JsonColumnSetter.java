package org.embulk.output.tsurugidb.setter;

import java.time.Instant;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.msgpack.value.Value;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/JsonColumnSetter.java
public class JsonColumnSetter extends ColumnSetter {
    public JsonColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue) {
        super(bindName, batch, column, defaultValue);
    }

    @Override
    public void nullValue() {
        defaultValue.setJson();
    }

    @Override
    public void booleanValue(boolean v) {
        defaultValue.setJson();
    }

    @Override
    public void longValue(long v) {
        defaultValue.setJson();
    }

    @Override
    public void doubleValue(double v) {
        defaultValue.setJson();
    }

    @Override
    public void stringValue(String v) {
        defaultValue.setJson();
    }

    @Override
    public void timestampValue(final Instant v) {
        defaultValue.setJson();
    }

    @Override
    public void jsonValue(Value v) {
        batch.setString(bindName, v.toJson());
    }
}
