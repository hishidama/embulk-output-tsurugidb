package org.embulk.output.tsurugidb.setter;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/NullDefaultValueSetter.java
public class NullDefaultValueSetter extends DefaultValueSetter {
    public NullDefaultValueSetter(String bindName, BatchInsert batch, TsurugiColumn column) {
        super(bindName, batch, column);
    }

    @Override
    public void setNull() {
        batch.setNull(bindName);
    }

    @Override
    public void setBoolean() {
        batch.setNull(bindName);
    }

    @Override
    public void setByte() {
        batch.setNull(bindName);
    }

    @Override
    public void setShort() {
        batch.setNull(bindName);
    }

    @Override
    public void setInt() {
        batch.setNull(bindName);
    }

    @Override
    public void setLong() {
        batch.setNull(bindName);
    }

    @Override
    public void setFloat() {
        batch.setNull(bindName);
    }

    @Override
    public void setDouble() {
        batch.setNull(bindName);
    }

    @Override
    public void setBigDecimal() {
        batch.setNull(bindName);
    }

    @Override
    public void setString() {
        batch.setNull(bindName);
    }

    @Override
    public void setNString() {
        batch.setNull(bindName);
    }

    @Override
    public void setBytes() {
        batch.setNull(bindName);
    }

    @Override
    public void setDate() {
        batch.setNull(bindName);
    }

    @Override
    public void setTime() {
        batch.setNull(bindName);
    }

    @Override
    public void setDateTime() {
        batch.setNull(bindName);
    }

    @Override
    public void setOffsetTime() {
        batch.setNull(bindName);
    }

    @Override
    public void setOffsetDateTime() {
        batch.setNull(bindName);
    }

    @Override
    public void setJson() {
        batch.setNull(bindName);
    }
}
