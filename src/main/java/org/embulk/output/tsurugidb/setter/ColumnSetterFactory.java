package org.embulk.output.tsurugidb.setter;

import java.time.ZoneId;

import org.embulk.config.ConfigException;
import org.embulk.output.tsurugidb.TsurugiColumn;
import org.embulk.output.tsurugidb.common.DbColumnOption;
import org.embulk.output.tsurugidb.insert.BatchInsert;
import org.embulk.util.timestamp.TimestampFormatter;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/setter/ColumnSetterFactory.java
public class ColumnSetterFactory {
    protected final BatchInsert batch;
    protected final ZoneId defaultTimeZone;

    public ColumnSetterFactory(final BatchInsert batch, final ZoneId defaultTimeZone) {
        this.batch = batch;
        this.defaultTimeZone = defaultTimeZone;
    }

    public SkipColumnSetter newSkipColumnSetter(String bindName) {
        return new SkipColumnSetter(bindName, batch);
    }

    public DefaultValueSetter newDefaultValueSetter(String bindName, TsurugiColumn column, DbColumnOption option) {
        return new NullDefaultValueSetter(bindName, batch, column);
    }

    public ColumnSetter newColumnSetter(String bindName, TsurugiColumn column, DbColumnOption option) {
        switch (option.getValueType()) {
        case "coerce":
            return newCoalesceColumnSetter(bindName, column, option);
        case "byte":
            return new ByteColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "short":
            return new ShortColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "int":
            return new IntColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "long":
            return new LongColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "double":
            return new DoubleColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "float":
            return new FloatColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "boolean":
            return new BooleanColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "string":
            return new StringColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), newTimestampFormatter(option));
        case "nstring":
            return new NStringColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), newTimestampFormatter(option));
        case "date":
            return new DateColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        case "time":
            return new TimeColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        case "timestamp":
            return new DateTimeColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        case "decimal":
            return new DecimalColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "json":
            return new JsonColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "null":
            return new NullColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case "pass":
            return new PassThroughColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        default:
            throw new ConfigException(String.format("Unknown value_type '%s' for column '%s'", option.getValueType(), column.getName()));
        }
    }

    protected TimestampFormatter newTimestampFormatter(DbColumnOption option) {
        final String format = option.getTimestampFormat();
        final ZoneId timezone = getTimeZone(option);
        return TimestampFormatter.builder(format, true).setDefaultZoneId(timezone).build();
    }

    protected ZoneId getTimeZone(DbColumnOption option) {
        return option.getTimeZone().orElse(this.defaultTimeZone);
    }

    public ColumnSetter newCoalesceColumnSetter(String bindName, TsurugiColumn column, DbColumnOption option) {
        switch (column.getSqlType()) {
        case BOOLEAN:
            return new BooleanColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case INT4:
            return new IntColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case INT8:
            return new LongColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case FLOAT4:
            return new FloatColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case FLOAT8:
            return new DoubleColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case DECIMAL:
            return new DecimalColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option));
        case CHARACTER:
            return new StringColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), newTimestampFormatter(option));
        case DATE:
            return new DateColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        case TIME_OF_DAY:
            return new TimeColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        case TIME_POINT:
            return new DateTimeColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return new OffsetTimeColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        case TIME_POINT_WITH_TIME_ZONE:
            return new OffsetDateTimeColumnSetter(bindName, batch, column, newDefaultValueSetter(bindName, column, option), getTimeZone(option));
        default:
            throw unsupportedOperationException(column);
        }
    }

    // private static String[] UNSUPPORTED = new String[] {
    // "ARRAY",
    // "STRUCT",
    // "REF",
    // "DATALINK",
    // "SQLXML",
    // "ROWID",
    // "DISTINCT",
    // "OTHER",
    // };

    private static UnsupportedOperationException unsupportedOperationException(TsurugiColumn column) {
        throw new UnsupportedOperationException(String.format("Unsupported type %s (sqlType=%d, size=%d, scale=%d)", column.getDeclaredType().orElse(column.getSimpleTypeName()), column.getSqlType(),
                column.getSizeTypeParameter(), column.getScaleTypeParameter()));
    }
}
