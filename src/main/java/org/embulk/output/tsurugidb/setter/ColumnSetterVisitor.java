package org.embulk.output.tsurugidb.setter;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/ColumnSetterVisitor.java
public class ColumnSetterVisitor implements ColumnVisitor {
    private final Record record;
    private final ColumnSetter setter;

    public ColumnSetterVisitor(Record record, ColumnSetter setter) {
        this.record = record;
        this.setter = setter;
    }

    @Override
    public void booleanColumn(Column column) {
        if (record.isNull(column)) {
            setter.nullValue();
        } else {
            setter.booleanValue(record.getBoolean(column));
        }
    }

    @Override
    public void longColumn(Column column) {
        if (record.isNull(column)) {
            setter.nullValue();
        } else {
            setter.longValue(record.getLong(column));
        }
    }

    @Override
    public void doubleColumn(Column column) {
        if (record.isNull(column)) {
            setter.nullValue();
        } else {
            setter.doubleValue(record.getDouble(column));
        }
    }

    @Override
    public void stringColumn(Column column) {
        if (record.isNull(column)) {
            setter.nullValue();
        } else {
            setter.stringValue(record.getString(column));
        }
    }

    @Override
    public void jsonColumn(Column column) {
        if (record.isNull(column)) {
            setter.nullValue();
        } else {
            setter.jsonValue(record.getJson(column));
        }
    }

    @Override
    public void timestampColumn(Column column) {
        if (record.isNull(column)) {
            setter.nullValue();
        } else {
            setter.timestampValue(record.getTimestamp(column));
        }
    }
}
