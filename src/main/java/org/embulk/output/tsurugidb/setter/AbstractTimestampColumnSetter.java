package org.embulk.output.tsurugidb.setter;

import java.time.ZoneId;

import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.insert.BatchInsert;

public abstract class AbstractTimestampColumnSetter extends ColumnSetter {

    protected final ZoneId zoneId;

    public AbstractTimestampColumnSetter(String bindName, BatchInsert batch, TsurugiColumn column, DefaultValueSetter defaultValue, ZoneId zoneId) {
        super(bindName, batch, column, defaultValue);
        this.zoneId = zoneId;
    }
}
