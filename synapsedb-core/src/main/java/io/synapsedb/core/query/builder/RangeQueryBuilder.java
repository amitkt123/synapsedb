package io.synapsedb.core.query.builder;

import io.synapsedb.core.query.QueryBuilder;
import io.synapsedb.core.query.validation.ValidationResult;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;

/**
 * Builder for range queries on numeric fields.
 * Used for filtering by numeric ranges like price, age, date, etc.
 *
 * Examples:
 * - Price between $100 and $500: range("price").gte(100).lte(500)
 * - Age greater than 18: range("age").gt(18)
 * - Temperature less than or equal to 100: range("temp").lte(100)
 *
 * Supports: int, long, double (dates can be represented as long timestamps)
 *
 * @author Amit Tiwari
 */
public class RangeQueryBuilder extends QueryBuilder {

    private final String field;
    private RangeType type;

    // Bounds
    private Number lowerBound;
    private Number upperBound;
    private boolean includeLower = true;
    private boolean includeUpper = true;

    /**
     * Supported numeric types for range queries
     */
    public enum RangeType {
        INT,
        LONG,
        DOUBLE
    }

    public RangeQueryBuilder(String field) {
        this.field = field;
    }

    /**
     * Set the range type explicitly.
     * Auto-detected from first bound if not set.
     */
    public RangeQueryBuilder type(RangeType type) {
        this.type = type;
        return this;
    }

    /**
     * Greater than (exclusive lower bound)
     */
    public RangeQueryBuilder gt(Number value) {
        this.lowerBound = value;
        this.includeLower = false;
        detectType(value);
        return this;
    }

    /**
     * Greater than or equal (inclusive lower bound)
     */
    public RangeQueryBuilder gte(Number value) {
        this.lowerBound = value;
        this.includeLower = true;
        detectType(value);
        return this;
    }

    /**
     * Less than (exclusive upper bound)
     */
    public RangeQueryBuilder lt(Number value) {
        this.upperBound = value;
        this.includeUpper = false;
        detectType(value);
        return this;
    }

    /**
     * Less than or equal (inclusive upper bound)
     */
    public RangeQueryBuilder lte(Number value) {
        this.upperBound = value;
        this.includeUpper = true;
        detectType(value);
        return this;
    }

    /**
     * Set both bounds at once (inclusive)
     */
    public RangeQueryBuilder from(Number lower, Number upper) {
        this.lowerBound = lower;
        this.upperBound = upper;
        this.includeLower = true;
        this.includeUpper = true;
        detectType(lower);
        return this;
    }

    private void detectType(Number value) {
        if (type != null) return; // Already set explicitly

        if (value instanceof Integer) {
            type = RangeType.INT;
        } else if (value instanceof Long) {
            type = RangeType.LONG;
        } else if (value instanceof Double || value instanceof Float) {
            type = RangeType.DOUBLE;
        }
    }

    @Override
    public Query toLuceneQuery() {
        // Convert bounds based on type
        switch (type) {
            case INT:
                int intMin = lowerBound != null ? lowerBound.intValue() : Integer.MIN_VALUE;
                int intMax = upperBound != null ? upperBound.intValue() : Integer.MAX_VALUE;

                // Adjust for exclusive bounds
                if (lowerBound != null && !includeLower) intMin++;
                if (upperBound != null && !includeUpper) intMax--;

                return IntPoint.newRangeQuery(field, intMin, intMax);

            case LONG:
                long longMin = lowerBound != null ? lowerBound.longValue() : Long.MIN_VALUE;
                long longMax = upperBound != null ? upperBound.longValue() : Long.MAX_VALUE;

                if (lowerBound != null && !includeLower) longMin++;
                if (upperBound != null && !includeUpper) longMax--;

                return LongPoint.newRangeQuery(field, longMin, longMax);

            case DOUBLE:
                double doubleMin = lowerBound != null ? lowerBound.doubleValue() : Double.NEGATIVE_INFINITY;
                double doubleMax = upperBound != null ? upperBound.doubleValue() : Double.POSITIVE_INFINITY;

                if (lowerBound != null && !includeLower) {
                    doubleMin = Math.nextUp(doubleMin);
                }
                if (upperBound != null && !includeUpper) {
                    doubleMax = Math.nextDown(doubleMax);
                }

                return DoublePoint.newRangeQuery(field, doubleMin, doubleMax);

            default:
                throw new IllegalStateException("Unknown range type: " + type);
        }
    }

    @Override
    public ValidationResult validate() {
        ValidationResult.Builder builder = new ValidationResult.Builder();

        if (field == null || field.trim().isEmpty()) {
            builder.addError("field", "Field name is required for range query");
        }

        if (type == null) {
            builder.addError("type", "Range type not set. Use gte/lte/gt/lt or set type() explicitly");
        }

        if (lowerBound == null && upperBound == null) {
            builder.addError("bounds", "At least one bound (lower or upper) must be specified");
        }

        if (lowerBound != null && upperBound != null) {
            double lower = lowerBound.doubleValue();
            double upper = upperBound.doubleValue();

            if (lower > upper) {
                builder.addError("bounds",
                    String.format("Lower bound (%.2f) is greater than upper bound (%.2f)", lower, upper));
            }

            if (lower == upper && (!includeLower || !includeUpper)) {
                builder.addWarning("bounds",
                    "Lower and upper bounds are equal but one is exclusive - this will match no documents");
            }
        }

        return builder.build();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("RangeQuery{field='").append(field).append("'");

        if (lowerBound != null) {
            sb.append(", ").append(includeLower ? ">=" : ">").append(lowerBound);
        }
        if (upperBound != null) {
            sb.append(", ").append(includeUpper ? "<=" : "<").append(upperBound);
        }
        sb.append(", type=").append(type);
        sb.append("}");

        return sb.toString();
    }

    @Override
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"range\",\"field\":\"").append(field).append("\"");

        if (lowerBound != null) {
            sb.append(",\"").append(includeLower ? "gte" : "gt").append("\":").append(lowerBound);
        }
        if (upperBound != null) {
            sb.append(",\"").append(includeUpper ? "lte" : "lt").append("\":").append(upperBound);
        }
        sb.append(",\"dataType\":\"").append(type).append("\"}");

        return sb.toString();
    }

    @Override
    public String getQueryType() {
        return "range";
    }

    // Getters

    public String getField() {
        return field;
    }

    public Number getLowerBound() {
        return lowerBound;
    }

    public Number getUpperBound() {
        return upperBound;
    }

    public boolean isIncludeLower() {
        return includeLower;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    public RangeType getType() {
        return type;
    }
}

