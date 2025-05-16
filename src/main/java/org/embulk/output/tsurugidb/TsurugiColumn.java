package org.embulk.output.tsurugidb;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

// https://github.com/embulk/embulk-output-jdbc/blob/master/embulk-output-jdbc/src/main/java/org/embulk/output/jdbc/TsurugiColumn.java
public class TsurugiColumn {

    private final int index;
    private final String name;
    private final String simpleTypeName;
    private final AtomType sqlType;
    private final int sizeTypeParameter;
    private final int scaleTypeParameter;
    private final int dataLength;
    private final Optional<String> declaredType;
    private final boolean isNotNull;
    private final boolean isUniqueKey;

    @JsonCreator
    public TsurugiColumn( //
            @JsonProperty("index") int index, //
            @JsonProperty("name") String name, //
            @JsonProperty("sqlType") AtomType sqlType, //
            @JsonProperty("simpleTypeName") String simpleTypeName, //
            @JsonProperty("sizeTypeParameter") int sizeTypeParameter, //
            @JsonProperty("scaleTypeParameter") int scaleTypeParameter, //
            @JsonProperty("dataLength") int dataLength, //
            @JsonProperty("declaredType") Optional<String> declaredType, //
            @JsonProperty("notNull") boolean isNotNull, //
            @JsonProperty("uniqueKey") boolean isUniqueKey) {
        this.index = index;
        this.name = name;
        this.simpleTypeName = simpleTypeName;
        this.sqlType = sqlType;
        this.sizeTypeParameter = sizeTypeParameter;
        this.scaleTypeParameter = scaleTypeParameter;
        this.dataLength = dataLength;
        this.declaredType = declaredType;
        this.isNotNull = isNotNull;
        this.isUniqueKey = isUniqueKey;
    }

    public static TsurugiColumn newGenericTypeColumn(int index, String name, AtomType sqlType, String simpleTypeName, int sizeTypeParameter, int scaleTypeParameter, int dataLength, boolean isNotNull,
            boolean isUniqueKey) {
        Objects.requireNonNull(name, "name==null");
        return new TsurugiColumn(index, name, sqlType, simpleTypeName, sizeTypeParameter, scaleTypeParameter, dataLength, Optional.<String>empty(), isNotNull, isUniqueKey);
    }

    public static TsurugiColumn newGenericTypeColumn(int index, String name, AtomType sqlType, String simpleTypeName, int sizeTypeParameter, int scaleTypeParameter, boolean isNotNull,
            boolean isUniqueKey) {
        Objects.requireNonNull(name, "name==null");
        return new TsurugiColumn(index, name, sqlType, simpleTypeName, sizeTypeParameter, scaleTypeParameter, sizeTypeParameter, Optional.<String>empty(), isNotNull, isUniqueKey);
    }

    public static TsurugiColumn newTypeDeclaredColumn(int index, String name, AtomType sqlType, String declaredType, boolean isNotNull, boolean isUniqueKey) {
        Objects.requireNonNull(name, "name==null");
        return new TsurugiColumn(index, name, sqlType, declaredType, 0, 0, 0, Optional.of(declaredType), isNotNull, isUniqueKey);
    }

    @JsonIgnore
    public static TsurugiColumn skipColumn() {
        return new TsurugiColumn(-1, null, AtomType.UNKNOWN, null, 0, 0, 0, Optional.<String>empty(), false, false);
    }

    @JsonIgnore
    public boolean isSkipColumn() {
        return name == null;
    }

    @JsonProperty("index")
    public int getIndex() {
        return index;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("sqlType")
    public AtomType getSqlType() {
        return sqlType;
    }

    @JsonProperty("simpleTypeName")
    public String getSimpleTypeName() {
        return simpleTypeName;
    }

    @JsonProperty("sizeTypeParameter")
    public int getSizeTypeParameter() {
        return sizeTypeParameter;
    }

    @JsonProperty("scaleTypeParameter")
    public int getScaleTypeParameter() {
        return scaleTypeParameter;
    }

    @JsonProperty("dataLength")
    public int getDataLength() {
        return dataLength;
    }

    @JsonProperty("declaredType")
    public Optional<String> getDeclaredType() {
        return declaredType;
    }

    @JsonProperty("notNull")
    public boolean isNotNull() {
        return isNotNull;
    }

    @JsonProperty("uniqueKey")
    public boolean isUniqueKey() {
        return isUniqueKey;
    }

    public static String getSimpleTypeName(SqlCommon.Column column) {
        var atomType = column.getAtomType();
        switch (atomType) {
        case INT4:
            return "INT";
        case INT8:
            return "BIGINT";
        case FLOAT4:
            return "REAL";
        case FLOAT8:
            return "DOUBLE";
        case DECIMAL:
            return "DECIMAL";
        case CHARACTER:
            return getSimpleTypeNameVar("CHAR", column);
        case OCTET:
            return getSimpleTypeNameVar("BINARY", column);
        case DATE:
            return "DATE";
        case TIME_OF_DAY:
            return "TIME";
        case TIME_POINT:
            return "TIMESTAMP";
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return "TIME WITH TIME ZONE";
        case TIME_POINT_WITH_TIME_ZONE:
            return "TIMESTAMP WITH TIME ZONE";
        default:
            return atomType.name();
        }
    }

    private static String getSimpleTypeNameVar(String baseName, SqlCommon.Column column) {
        var varying = column.getVaryingOptCase();
        if (varying == null) {
            return column.getAtomType().name();
        }
        switch (varying) {
        case VARYINGOPT_NOT_SET:
            return baseName;
        case VARYING:
            return "VAR" + baseName;
        default:
            return column.getAtomType().name();
        }
    }

    public static int getColumnSize(SqlCommon.Column column) {
        var atomType = column.getAtomType();
        switch (atomType) {
        case BOOLEAN:
            return 1;
        case INT4:
            return 10;
        case INT8:
            return 19;
        case FLOAT4:
            return 7;
        case FLOAT8:
            return 15;
        case DECIMAL:
            return getPrecision(column);
        case CHARACTER:
            return getLength(column);
        case OCTET:
            return getLength(column);
        case DATE:
            return 10;
        case TIME_OF_DAY:
            return 8 + 6;
        case TIME_POINT:
            return 19 + 6;
        case TIME_OF_DAY_WITH_TIME_ZONE:
            return 8 + 6 + 6;
        case TIME_POINT_WITH_TIME_ZONE:
            return 19 + 6 + 6;
        default:
            return 0;
        }
    }

    public static int getPrecision(SqlCommon.Column column) {
        var precision = column.getPrecisionOptCase();
        if (precision == null) {
            return 0;
        }
        switch (precision) {
        case PRECISION:
            return column.getPrecision();
        case ARBITRARY_PRECISION:
            return 38;
        case PRECISIONOPT_NOT_SET:
        default:
            return 0;
        }
    }

    public static int getScale(SqlCommon.Column column) {
        var scale = column.getScaleOptCase();
        if (scale == null) {
            return -1;
        }
        switch (scale) {
        case SCALE:
            return column.getScale();
        case ARBITRARY_SCALE:
            return 38;
        case SCALEOPT_NOT_SET:
        default:
            return -1;
        }
    }

    public static int getLength(SqlCommon.Column column) {
        var length = column.getLengthOptCase();
        if (length == null) {
            return 0;
        }
        switch (length) {
        case LENGTH:
            return column.getLength();
        case ARBITRARY_LENGTH:
            return 2097132;
        case LENGTHOPT_NOT_SET:
        default:
            return 0;
        }
    }

    public static boolean isNotNull(SqlCommon.Column column) {
        var nullable = column.getNullableOptCase();
        if (nullable == null) {
            return false;
        }
        switch (nullable) {
        case NULLABLE:
            return false;
        case NULLABLEOPT_NOT_SET:
            return true;
        default:
            return false;
        }
    }
}
