package org.embulk.output.tsurugidb;

import java.math.BigDecimal;
import java.util.Objects;

public class TestEntity {

    private final int pk;
    private final long longValue;
    private final double doubleValue;
    private final BigDecimal decimalValue;
    private final String stringValue;

    public TestEntity(int pk, long longValue, double doubleValue, BigDecimal decimalValue, String stringValue) {
        this.pk = pk;
        this.longValue = longValue;
        this.doubleValue = doubleValue;
        this.decimalValue = decimalValue;
        this.stringValue = stringValue;
    }

    public int getPk() {
        return pk;
    }

    public long getLongValue() {
        return longValue;
    }

    public double getDoubleValue() {
        return doubleValue;
    }

    public BigDecimal getDecimalValue() {
        return decimalValue;
    }

    public String getStringValue() {
        return stringValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(decimalValue, doubleValue, longValue, pk, stringValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TestEntity other = (TestEntity) obj;
        return Objects.equals(decimalValue, other.decimalValue) && Double.doubleToLongBits(doubleValue) == Double.doubleToLongBits(other.doubleValue) && longValue == other.longValue && pk == other.pk
                && Objects.equals(stringValue, other.stringValue);
    }

    public String toCsv() {
        return pk //
                + "," + longValue //
                + "," + doubleValue //
//                + "," + decimalValue //
                + "," + stringValue;
    }

    @Override
    public String toString() {
        return "TestEntity{pk=" + pk + ", longValue=" + longValue + ", doubleValue=" + doubleValue + ", decimalValue=" + decimalValue + ", stringValue=" + stringValue + "}";
    }
}
