package org.embulk.output.tsurugidb.setter;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/DefaultValueSetter.java
public abstract class DefaultValueSetter {
    protected final String bindName;
    protected final BatchInsert batch;
    protected final TsurugiColumn column;

    public DefaultValueSetter(String bindName, BatchInsert batch, TsurugiColumn column) {
        this.bindName = bindName;
        this.batch = batch;
        this.column = column;
    }

    public abstract void setNull();

    public abstract void setBoolean();

    public abstract void setByte();

    public abstract void setShort();

    public abstract void setInt();

    public abstract void setLong();

    public abstract void setFloat();

    public abstract void setDouble();

    public abstract void setBigDecimal();

    public abstract void setString();

    public abstract void setNString();

    public abstract void setBytes();

    public abstract void setDate();

    public abstract void setTime();

    public abstract void setDateTime();

    public abstract void setOffsetTime();

    public abstract void setOffsetDateTime();

    public abstract void setJson();
}
