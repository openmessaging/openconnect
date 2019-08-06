package io.openmessaging.connector.api.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.text.CharacterIterator;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Data with meta information.
 * Provide a variety of data formatting methods. Easy access to desired data types.
 * Provide static methods for data type validation.
 * Provide the function of decompiling data strings to MetaAndData.
 *
 * @author liuboyan
 */
public class MetaAndData {

    private final Meta meta;
    private Object data;

    public MetaAndData(Meta meta) {
        this.meta = meta;
        initData();
    }

    public MetaAndData(Meta meta, Object data) {
        this.meta = meta;
        if (data != null) {
            this.data = data;
        } else {
            initData();
        }
    }

    public Meta getMeta() {
        return meta;
    }

    public MetaAndData setData(Object data) {
        this.data = data;
        return this;
    }

    public Object getData() {
        return data;
    }

    private void initData() {
        if (this.meta == null || this.meta.getType() == null) {
            return;
        }
        if (this.data == null) {
            switch (meta.getType()) {
                case STRUCT:
                    this.data = new Struct(meta);
                    break;
                case ARRAY:
                    this.data = new ArrayList<>();
                    break;
                case MAP:
                    this.data = new HashMap<>();
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetaAndData that = (MetaAndData)o;
        return Objects.equals(meta, that.meta) &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(meta, data);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MetaAndData{").append("meta=");
        if (meta != null) {
            sb.append(meta);
        }
        sb.append(", ").append("data=");
        if (data != null) {
            sb.append(data);
        }
        sb.append("}");
        return sb.toString();
    }

    /******************************CONSTRUCT********************************/

    public static MetaAndData getMetaDataFromString(String value) {
        return new MetaAndData(Meta.STRING_META).parseString(value);
    }

    // base

    public MetaAndData putData(Object data) {
        if (this.meta != null) {
            if (this.meta.type == Type.ARRAY) {
                this.meta.getValueMeta().validateValue(data);
                ((List)this.data).add(data);
            } else {
                this.meta.validateValue(data);
                this.data = data;
            }
        } else {
            this.data = data;
        }
        return this;
    }

    // map {"a":15}

    public MetaAndData putData(Object key, Object value) {
        if (key == null) {
            throw new RuntimeException("key should not be null.");
        }
        if (this.meta != null) {
            this.meta.getKeyMeta().validateValue(key);
            this.meta.getValueMeta().validateValue(value);
            ((Map)this.data).put(key, value);
        } else {
            ((Map)this.data).put(key, value);
        }
        return this;
    }

    public MetaAndData putData(Map map) {
        if (map == null) {
            return this;
        }
        if (this.meta != null) {
            map.forEach((key, value) -> {
                this.meta.getKeyMeta().validateValue(key);
                this.meta.getValueMeta().validateValue(value);
                ((Map)this.data).put(key, value);
            });
        } else {
            ((Map)this.data).putAll(map);
        }
        return this;
    }

    // array

    public MetaAndData putData(List elements) {
        if (elements == null) {
            return this;
        }
        if (this.meta != null) {
            elements.forEach(element -> {
                this.meta.getValueMeta().validateValue(element);
                ((List)this.data).add(element);
            });
        } else {
            ((List)this.data).addAll(elements);
        }
        return this;
    }

    // struct

    public MetaAndData putData(String fieldName, Object value) {
        ((Struct)this.data).put(fieldName, value);
        return this;
    }

    public MetaAndData putData(Field field, Object value) {
        ((Struct)this.data).put(field, value);
        return this;
    }

    /******************************VALUES********************************/

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final MetaAndData TRUE_META_AND_VALUE = new MetaAndData(Meta.BOOLEAN_META, Boolean.TRUE);
    private static final MetaAndData FALSE_META_AND_VALUE = new MetaAndData(Meta.BOOLEAN_META, Boolean.FALSE);
    private static final Meta ARRAY_SELECTOR_META = MetaBuilder.array(Meta.STRING_META).build();
    private static final Meta MAP_SELECTOR_META = MetaBuilder.map(Meta.STRING_META, Meta.STRING_META).build();
    private static final Meta STRUCT_SELECTOR_META = MetaBuilder.struct().build();
    private static final String TRUE_LITERAL = Boolean.TRUE.toString();
    private static final String FALSE_LITERAL = Boolean.FALSE.toString();
    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final String NULL_VALUE = "null";
    private static final String ISO_8601_DATE_FORMAT_PATTERN = "yyyy-MM-dd";
    private static final String ISO_8601_TIME_FORMAT_PATTERN = "HH:mm:ss.SSS'Z'";
    private static final String ISO_8601_TIMESTAMP_FORMAT_PATTERN = ISO_8601_DATE_FORMAT_PATTERN + "'T'"
        + ISO_8601_TIME_FORMAT_PATTERN;

    private static final String QUOTE_DELIMITER = "\"";
    private static final String COMMA_DELIMITER = ",";
    private static final String ENTRY_DELIMITER = ":";
    private static final String ARRAY_BEGIN_DELIMITER = "[";
    private static final String ARRAY_END_DELIMITER = "]";
    private static final String MAP_BEGIN_DELIMITER = "{";
    private static final String MAP_END_DELIMITER = "}";
    private static final int ISO_8601_DATE_LENGTH = ISO_8601_DATE_FORMAT_PATTERN.length();
    /**
     * subtract single quotes
     */
    private static final int ISO_8601_TIME_LENGTH = ISO_8601_TIME_FORMAT_PATTERN.length() - 2;
    private static final int ISO_8601_TIMESTAMP_LENGTH = ISO_8601_TIMESTAMP_FORMAT_PATTERN.length() - 4;

    private static final Pattern TWO_BACKSLASHES = Pattern.compile("\\\\");

    private static final Pattern DOUBLEQOUTE = Pattern.compile("\"");

    public Boolean convertToBoolean() throws RuntimeException {
        return (Boolean)convertTo(this.meta, Meta.BOOLEAN_META, this.data);
    }

    public Byte convertToByte() throws RuntimeException {
        return (Byte)convertTo(this.meta, Meta.INT8_META, this.data);
    }

    public Short convertToShort() throws RuntimeException {
        return (Short)convertTo(this.meta, Meta.INT16_META, this.data);
    }

    public Integer convertToInteger() throws RuntimeException {
        return (Integer)convertTo(this.meta, Meta.INT32_META, this.data);
    }

    public Long convertToLong() throws RuntimeException {
        return (Long)convertTo(this.meta, Meta.INT64_META, this.data);
    }

    public Float convertToFloat() throws RuntimeException {
        return (Float)convertTo(this.meta, Meta.FLOAT32_META, this.data);
    }

    public Double convertToDouble() throws RuntimeException {
        return (Double)convertTo(this.meta, Meta.FLOAT64_META, this.data);
    }

    public String convertToString() {
        return (String)convertTo(this.meta, Meta.STRING_META, this.data);
    }

    public List<?> convertToList() {
        return (List<?>)convertTo(this.meta, ARRAY_SELECTOR_META, this.data);
    }

    public Map<?, ?> convertToMap() {
        return (Map<?, ?>)convertTo(this.meta, MAP_SELECTOR_META, this.data);
    }

    public Struct convertToStruct() {
        return (Struct)convertTo(this.meta, STRUCT_SELECTOR_META, this.data);
    }

    public java.util.Date convertToTime() {
        return (java.util.Date)convertTo(this.meta, Time.META, this.data);
    }

    public java.util.Date convertToDate() {
        return (java.util.Date)convertTo(this.meta, Date.META, this.data);
    }

    public java.util.Date convertToTimestamp() {
        return (java.util.Date)convertTo(this.meta, Timestamp.META, this.data);
    }

    public BigDecimal convertToDecimal(int scale) {
        return (BigDecimal)convertTo(this.meta, Decimal.meta(scale), this.data);
    }

    public Meta inferMeta() {
        if (this.data == null) {
            return null;
        }
        if (this.data instanceof String) {
            return Meta.STRING_META;
        }
        if (this.data instanceof Boolean) {
            return Meta.BOOLEAN_META;
        }
        if (this.data instanceof Byte) {
            return Meta.INT8_META;
        }
        if (this.data instanceof Short) {
            return Meta.INT16_META;
        }
        if (this.data instanceof Integer) {
            return Meta.INT32_META;
        }
        if (this.data instanceof Long) {
            return Meta.INT64_META;
        }
        if (this.data instanceof Float) {
            return Meta.FLOAT32_META;
        }
        if (this.data instanceof Double) {
            return Meta.FLOAT64_META;
        }
        if (this.data instanceof byte[] || this.data instanceof ByteBuffer) {
            return Meta.BYTES_META;
        }
        if (this.data instanceof List) {
            List<?> list = (List<?>)this.data;
            if (list.isEmpty()) {
                return null;
            }
            MetaDetector detector = new MetaDetector();
            for (Object element : list) {
                if (!detector.canDetect(element)) {
                    return null;
                }
            }
            return MetaBuilder.array(detector.meta()).build();
        }
        if (this.data instanceof Map) {
            Map<?, ?> map = (Map<?, ?>)this.data;
            if (map.isEmpty()) {
                return null;
            }
            MetaDetector keyDetector = new MetaDetector();
            MetaDetector valueDetector = new MetaDetector();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!keyDetector.canDetect(entry.getKey()) || !valueDetector.canDetect(entry.getValue())) {
                    return null;
                }
            }
            return MetaBuilder.map(keyDetector.meta(), valueDetector.meta()).build();
        }
        if (this.data instanceof Struct) {
            return ((Struct)this.data).meta();
        }
        return null;
    }

    /************************************VALUES_PROTECTED*************************************/

    /**
     * Convert the value to the desired type.
     *
     * @param fromMeta the meta for the desired type; may not be null
     * @param toMeta   the meta for the supplied value; may be null if not known
     * @return the converted value; never null
     * @throws RuntimeException if the value could not be converted to the desired type
     */
    protected Object convertTo(Meta fromMeta, Meta toMeta, Object value) throws RuntimeException {
        if (value == null) {
            throw new RuntimeException("Unable to convert a null value to a meta that requires a value");
        }
        if (toMeta == null) {
            throw new RuntimeException("Unable to convert a value to a null meta");
        }
        switch (toMeta.getType()) {
            case BYTES:
                if (Decimal.LOGICAL_NAME.equals(toMeta.getName())) {
                    if (value instanceof ByteBuffer) {
                        value = toArray((ByteBuffer)value);
                    }
                    if (value instanceof byte[]) {
                        return Decimal.toLogical(toMeta, (byte[])value);
                    }
                    if (value instanceof BigDecimal) {
                        return value;
                    }
                    if (value instanceof Number) {
                        // Not already a decimal, so treat it as a double ...
                        double converted = ((Number)value).doubleValue();
                        return new BigDecimal(converted);
                    }
                    if (value instanceof String) {
                        return new BigDecimal(value.toString()).doubleValue();
                    }
                }
                if (value instanceof ByteBuffer) {
                    return toArray((ByteBuffer)value);
                }
                if (value instanceof byte[]) {
                    return value;
                }
                if (value instanceof BigDecimal) {
                    return Decimal.fromLogical(toMeta, (BigDecimal)value);
                }
                break;
            case STRING:
                StringBuilder sb = new StringBuilder();
                append(sb, value, false);
                return sb.toString();
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                }
                if (value instanceof String) {
                    MetaAndData parsed = parseString(value.toString());
                    if (parsed.getData() instanceof Boolean) {
                        return parsed.getData();
                    }
                }
                return asLong(value, fromMeta, null) == 0L ? Boolean.FALSE : Boolean.TRUE;
            case INT8:
                if (value instanceof Byte) {
                    return value;
                }
                return (byte)asLong(value, fromMeta, null);
            case INT16:
                if (value instanceof Short) {
                    return value;
                }
                return (short)asLong(value, fromMeta, null);
            case INT32:
                if (Date.LOGICAL_NAME.equals(toMeta.getName())) {
                    if (value instanceof String) {
                        MetaAndData parsed = parseString(value.toString());
                        value = parsed.getData();
                    }
                    if (value instanceof java.util.Date) {
                        if (fromMeta != null) {
                            String fromMetaName = fromMeta.getName();
                            if (Date.LOGICAL_NAME.equals(fromMetaName)) {
                                return value;
                            }
                            if (Timestamp.LOGICAL_NAME.equals(fromMetaName)) {
                                // Just get the number of days from this timestamp
                                long millis = ((java.util.Date)value).getTime();
                                int days = (int)(millis / MILLIS_PER_DAY); // truncates
                                return Date.toLogical(toMeta, days);
                            }
                        }
                    }
                    long numeric = asLong(value, fromMeta, null);
                    return Date.toLogical(toMeta, (int)numeric);
                }
                if (Time.LOGICAL_NAME.equals(toMeta.getName())) {
                    if (value instanceof String) {
                        MetaAndData parsed = parseString(value.toString());
                        value = parsed.getData();
                    }
                    if (value instanceof java.util.Date) {
                        if (fromMeta != null) {
                            String fromMetaName = fromMeta.getName();
                            if (Time.LOGICAL_NAME.equals(fromMetaName)) {
                                return value;
                            }
                            if (Timestamp.LOGICAL_NAME.equals(fromMetaName)) {
                                // Just get the time portion of this timestamp
                                Calendar calendar = Calendar.getInstance(UTC);
                                calendar.setTime((java.util.Date)value);
                                calendar.set(Calendar.YEAR, 1970);
                                calendar.set(Calendar.MONTH, 0); // Months are zero-based
                                calendar.set(Calendar.DAY_OF_MONTH, 1);
                                return Time.toLogical(toMeta, (int)calendar.getTimeInMillis());
                            }
                        }
                    }
                    long numeric = asLong(value, fromMeta, null);
                    return Time.toLogical(toMeta, (int)numeric);
                }
                if (value instanceof Integer) {
                    return value;
                }
                return (int)asLong(value, fromMeta, null);
            case INT64:
                if (Timestamp.LOGICAL_NAME.equals(toMeta.getName())) {
                    if (value instanceof String) {
                        MetaAndData parsed = parseString(value.toString());
                        value = parsed.getData();
                    }
                    if (value instanceof java.util.Date) {
                        java.util.Date date = (java.util.Date)value;
                        if (fromMeta != null) {
                            String fromMetaName = fromMeta.getName();
                            if (Date.LOGICAL_NAME.equals(fromMetaName)) {
                                int days = Date.fromLogical(fromMeta, date);
                                long millis = days * MILLIS_PER_DAY;
                                return Timestamp.toLogical(toMeta, millis);
                            }
                            if (Time.LOGICAL_NAME.equals(fromMetaName)) {
                                long millis = Time.fromLogical(fromMeta, date);
                                return Timestamp.toLogical(toMeta, millis);
                            }
                            if (Timestamp.LOGICAL_NAME.equals(fromMetaName)) {
                                return value;
                            }
                        }
                    }
                    long numeric = asLong(value, fromMeta, null);
                    return Timestamp.toLogical(toMeta, numeric);
                }
                if (value instanceof Long) {
                    return value;
                }
                return asLong(value, fromMeta, null);
            case FLOAT32:
                if (value instanceof Float) {
                    return value;
                }
                return (float)asDouble(value, fromMeta, null);
            case FLOAT64:
                if (value instanceof Double) {
                    return value;
                }
                return asDouble(value, fromMeta, null);
            case ARRAY:
                if (value instanceof String) {
                    MetaAndData metaAndData = parseString(value.toString());
                    value = metaAndData.getData();
                }
                if (value instanceof List) {
                    return value;
                }
                break;
            case MAP:
                if (value instanceof String) {
                    MetaAndData metaAndData = parseString(value.toString());
                    value = metaAndData.getData();
                }
                if (value instanceof Map) {
                    return value;
                }
                break;
            case STRUCT:
                if (value instanceof Struct) {
                    Struct struct = (Struct)value;
                    return struct;
                }
                if (value instanceof Map) {
                    Map mapData = (Map)value;
                    Set<Map.Entry> entries = mapData.entrySet();
                    List<Meta> fieldMetas = new LinkedList<>();
                    List<String> fieldNames = new LinkedList<>();
                    List fieldDatas = new LinkedList();
                    for (Map.Entry entry : entries) {
                        String fieldName = entry.getKey().toString();
                        MetaAndData valueMeta = parseString(
                            convertTo(null, Meta.STRING_META, entry.getValue()).toString());
                        if (valueMeta.getMeta() == null && valueMeta.getData() instanceof Map) {
                            valueMeta = new MetaAndData(valueMeta.convertToStruct().meta(),
                                valueMeta.convertToStruct());
                        }
                        fieldMetas.add(valueMeta.getMeta());
                        fieldNames.add(fieldName);
                        fieldDatas.add(valueMeta.getData());
                    }
                    MetaBuilder structMetaBuilder = MetaBuilder.struct();
                    for (int i = 0; i < fieldMetas.size(); i++) {
                        structMetaBuilder.field(fieldNames.get(i), fieldMetas.get(i));
                    }
                    Meta structMeta = structMetaBuilder.build();
                    Struct result = new Struct(structMeta);
                    for (int i = 0; i < fieldMetas.size(); i++) {
                        result.put(fieldNames.get(i), fieldDatas.get(i));
                    }
                    return result;
                }
        }
        throw new RuntimeException("Unable to convert " + value + " (" + value.getClass() + ") to " + toMeta);
    }

    public MetaAndData parseString(String value) {
        if (value == null) {
            return null;
        }
        if (value.isEmpty()) {
            return new MetaAndData(Meta.STRING_META, value);
        }
        Parser parser = new Parser(value);
        return parse(parser, false);
    }

    protected MetaAndData parse(Parser parser, boolean embedded) throws NoSuchElementException {
        if (!parser.hasNext()) {
            return null;
        }
        if (embedded) {
            if (parser.canConsume(NULL_VALUE)) {
                return null;
            }
            if (parser.canConsume(QUOTE_DELIMITER)) {
                StringBuilder sb = new StringBuilder();
                while (parser.hasNext()) {
                    if (parser.canConsume(QUOTE_DELIMITER)) {
                        break;
                    }
                    sb.append(parser.next());
                }
                return new MetaAndData(Meta.STRING_META, sb.toString());
            }
        }
        if (parser.canConsume(TRUE_LITERAL)) {
            return TRUE_META_AND_VALUE;
        }
        if (parser.canConsume(FALSE_LITERAL)) {
            return FALSE_META_AND_VALUE;
        }
        int startPosition = parser.mark();
        try {
            if (parser.canConsume(ARRAY_BEGIN_DELIMITER)) {
                List<Object> result = new ArrayList<>();
                Meta elementMeta = null;
                while (parser.hasNext()) {
                    if (parser.canConsume(ARRAY_END_DELIMITER)) {
                        Meta listMeta = null;
                        if (elementMeta != null) {
                            listMeta = MetaBuilder.array(elementMeta).meta();
                        }
                        result = alignListEntriesWithMeta(listMeta, result);
                        return new MetaAndData(listMeta, result);
                    }
                    if (parser.canConsume(COMMA_DELIMITER)) {
                        throw new RuntimeException("Unable to parse an empty array element: " + parser.original());
                    }
                    MetaAndData element = parse(parser, true);
                    elementMeta = commonMetaFor(elementMeta, element);
                    result.add(element.getData());
                    parser.canConsume(COMMA_DELIMITER);
                }
                // Missing either a comma or an end delimiter
                if (COMMA_DELIMITER.equals(parser.previous())) {
                    throw new RuntimeException("Array is missing element after ',': " + parser.original());
                }
                throw new RuntimeException("Array is missing terminating ']': " + parser.original());
            }

            if (parser.canConsume(MAP_BEGIN_DELIMITER)) {
                Map<Object, Object> result = new LinkedHashMap<>();
                Meta keyMeta = null;
                Meta valueMeta = null;
                boolean objFlag = false;
                while (parser.hasNext()) {
                    if (parser.canConsume(MAP_END_DELIMITER)) {
                        Meta mapMeta = null;
                        if (!objFlag) {
                            mapMeta = MetaBuilder.map(keyMeta, valueMeta).meta();
                        }
                        result = alignMapKeysAndValuesWithMeta(mapMeta, result);
                        return new MetaAndData(mapMeta, result);
                    }
                    if (parser.canConsume(COMMA_DELIMITER)) {
                        throw new RuntimeException(
                            "Unable to parse a map entry has no key or value: " + parser.original());
                    }
                    MetaAndData key = parse(parser, true);
                    if (key == null || key.getData() == null) {
                        throw new RuntimeException("Map entry may not have a null key: " + parser.original());
                    }
                    if (!parser.canConsume(ENTRY_DELIMITER)) {
                        throw new RuntimeException("Map entry is missing '=': " + parser.original());
                    }
                    MetaAndData value = parse(parser, true);
                    Object entryValue = value != null ? value.getData() : null;
                    result.put(key.getData(), entryValue);
                    parser.canConsume(COMMA_DELIMITER);
                    keyMeta = commonMetaFor(keyMeta, key);
                    valueMeta = commonMetaFor(valueMeta, value);
                    if (!objFlag && (keyMeta == null || valueMeta == null)) {
                        objFlag = true;
                    }
                }
                // Missing either a comma or an end delimiter
                if (COMMA_DELIMITER.equals(parser.previous())) {
                    throw new RuntimeException("Map is missing element after ',': " + parser.original());
                }
                throw new RuntimeException("Map is missing terminating ']': " + parser.original());
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
            parser.rewindTo(startPosition);
        }
        String token = parser.next().trim();
        assert !token
            .isEmpty(); // original can be empty string but is handled right away; no way for token to be empty here
        char firstChar = token.charAt(0);
        boolean firstCharIsDigit = Character.isDigit(firstChar);
        if (firstCharIsDigit || firstChar == '+' || firstChar == '-') {
            try {
                // Try to parse as a number ...
                BigDecimal decimal = new BigDecimal(token);
                try {
                    return new MetaAndData(Meta.INT8_META, decimal.byteValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new MetaAndData(Meta.INT16_META, decimal.shortValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new MetaAndData(Meta.INT32_META, decimal.intValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                try {
                    return new MetaAndData(Meta.INT64_META, decimal.longValueExact());
                } catch (ArithmeticException e) {
                    // continue
                }
                double dValue = decimal.doubleValue();
                if (dValue != Double.NEGATIVE_INFINITY && dValue != Double.POSITIVE_INFINITY) {
                    return new MetaAndData(Meta.FLOAT64_META, dValue);
                }
                Meta meta = Decimal.meta(decimal.scale());
                return new MetaAndData(meta, decimal);
            } catch (NumberFormatException e) {
                // can't parse as a number
            }
        }
        if (firstCharIsDigit) {
            // Check for a date, time, or timestamp ...
            int tokenLength = token.length();
            if (tokenLength == ISO_8601_DATE_LENGTH) {
                try {
                    return new MetaAndData(Date.META, new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN).parse(token));
                } catch (ParseException e) {
                    // not a valid date
                }
            } else if (tokenLength == ISO_8601_TIME_LENGTH) {
                try {
                    return new MetaAndData(Time.META, new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN).parse(token));
                } catch (ParseException e) {
                    // not a valid date
                }
            } else if (tokenLength == ISO_8601_TIMESTAMP_LENGTH) {
                try {
                    return new MetaAndData(Timestamp.META,
                        new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(token));
                } catch (ParseException e) {
                    // not a valid date
                }
            }
        }
        // At this point, the only thing this can be is a string. Embedded strings were processed above,
        // so this is not embedded and we can use the original string...
        return new MetaAndData(Meta.STRING_META, parser.original());
    }

    protected List<Object> alignListEntriesWithMeta(Meta meta, List<Object> input) {
        if (meta == null) {
            return input;
        }
        Meta valueMeta = meta.getValueMeta();
        List<Object> result = new ArrayList<>();
        for (Object value : input) {
            Object newValue = convertTo(null, valueMeta, value);
            result.add(newValue);
        }
        return result;
    }

    protected Map<Object, Object> alignMapKeysAndValuesWithMeta(Meta mapMeta, Map<Object, Object> input) {
        if (mapMeta == null) {
            return input;
        }
        Meta keyMeta = mapMeta.getKeyMeta();
        Meta valueMeta = mapMeta.getValueMeta();
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : input.entrySet()) {
            Object newKey = convertTo(keyMeta, null, entry.getKey());
            Object newValue = convertTo(valueMeta, null, entry.getValue());
            result.put(newKey, newValue);
        }
        return result;
    }

    protected Meta commonMetaFor(Meta previous, MetaAndData latest) {
        if (latest == null) {
            return previous;
        }
        if (previous == null) {
            return latest.getMeta();
        }
        Meta newMeta = latest.getMeta();
        Type previousType = previous.getType();
        Type newType = newMeta.getType();
        if (previousType != newType) {
            switch (previous.getType()) {
                case INT8:
                    if (newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64
                        || newType == Type.FLOAT32 || newType ==
                        Type.FLOAT64) {
                        return newMeta;
                    }
                    break;
                case INT16:
                    if (newType == Type.INT8) {
                        return previous;
                    }
                    if (newType == Type.INT32 || newType == Type.INT64 || newType == Type.FLOAT32
                        || newType == Type.FLOAT64) {
                        return newMeta;
                    }
                    break;
                case INT32:
                    if (newType == Type.INT8 || newType == Type.INT16) {
                        return previous;
                    }
                    if (newType == Type.INT64 || newType == Type.FLOAT32 || newType == Type.FLOAT64) {
                        return newMeta;
                    }
                    break;
                case INT64:
                    if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32) {
                        return previous;
                    }
                    if (newType == Type.FLOAT32 || newType == Type.FLOAT64) {
                        return newMeta;
                    }
                    break;
                case FLOAT32:
                    if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32
                        || newType == Type.INT64) {
                        return previous;
                    }
                    if (newType == Type.FLOAT64) {
                        return newMeta;
                    }
                    break;
                case FLOAT64:
                    if (newType == Type.INT8 || newType == Type.INT16 || newType == Type.INT32 || newType == Type.INT64
                        || newType ==
                        Type.FLOAT32) {
                        return previous;
                    }
                    break;
            }
            return null;
        }
        if (!previous.equals(newMeta)) {
            return null;
        }
        return previous;
    }

    protected void append(StringBuilder sb, Object value, boolean embedded) {
        if (value == null) {
            sb.append(NULL_VALUE);
        } else if (value instanceof Number) {
            sb.append(value);
        } else if (value instanceof Boolean) {
            sb.append(value);
        } else if (value instanceof String) {
            if (embedded) {
                String escaped = escape((String)value);
                sb.append('"').append(escaped).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof byte[]) {
            value = Base64.getEncoder().encodeToString((byte[])value);
            if (embedded) {
                sb.append('"').append(value).append('"');
            } else {
                sb.append(value);
            }
        } else if (value instanceof ByteBuffer) {
            byte[] bytes = readBytes((ByteBuffer)value);
            append(sb, bytes, embedded);
        } else if (value instanceof List) {
            List<?> list = (List<?>)value;
            sb.append('[');
            appendIterable(sb, list.iterator());
            sb.append(']');
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>)value;
            sb.append('{');
            appendIterable(sb, map.entrySet().iterator());
            sb.append('}');
        } else if (value instanceof Struct) {
            Struct struct = (Struct)value;
            Meta meta = struct.meta();
            boolean first = true;
            sb.append('{');
            for (Field field : meta.getFields()) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }
                append(sb, field.name(), true);
                sb.append(':');
                append(sb, struct.get(field), true);
            }
            sb.append('}');
        } else if (value instanceof Map.Entry) {
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>)value;
            append(sb, entry.getKey(), true);
            sb.append(':');
            append(sb, entry.getValue(), true);
        } else if (value instanceof java.util.Date) {
            java.util.Date dateValue = (java.util.Date)value;
            String formatted = dateFormatFor(dateValue).format(dateValue);
            sb.append(formatted);
        } else {
            throw new RuntimeException(
                "Failed to serialize unexpected value type " + value.getClass().getName() + ": " + value);
        }
    }

    protected String escape(String value) {
        String replace1 = TWO_BACKSLASHES.matcher(value).replaceAll("\\\\\\\\");
        return DOUBLEQOUTE.matcher(replace1).replaceAll("\\\\\"");
    }

    protected void appendIterable(StringBuilder sb, Iterator<?> iter) {
        if (iter.hasNext()) {
            append(sb, iter.next(), true);
            while (iter.hasNext()) {
                sb.append(',');
                append(sb, iter.next(), true);
            }
        }
    }

    protected double asDouble(Object value, Meta meta, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number)value;
                return number.doubleValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).doubleValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        return asLong(value, meta, error);
    }

    protected long asLong(Object value, Meta fromMeta, Throwable error) {
        try {
            if (value instanceof Number) {
                Number number = (Number)value;
                return number.longValue();
            }
            if (value instanceof String) {
                return new BigDecimal(value.toString()).longValue();
            }
        } catch (NumberFormatException e) {
            error = e;
            // fall through
        }
        if (fromMeta != null) {
            String metaName = fromMeta.getName();
            if (value instanceof java.util.Date) {
                if (Date.LOGICAL_NAME.equals(metaName)) {
                    return Date.fromLogical(fromMeta, (java.util.Date)value);
                }
                if (Time.LOGICAL_NAME.equals(metaName)) {
                    return Time.fromLogical(fromMeta, (java.util.Date)value);
                }
                if (Timestamp.LOGICAL_NAME.equals(metaName)) {
                    return Timestamp.fromLogical(fromMeta, (java.util.Date)value);
                }
            }
            throw new RuntimeException("Unable to convert " + value + " (" + value.getClass() + ") to " + fromMeta,
                error);
        }
        throw new RuntimeException("Unable to convert " + value + " (" + value.getClass() + ") to a number", error);
    }

    /**************************UTILS****************************/

    private DateFormat dateFormatFor(java.util.Date value) {
        if (value.getTime() < MILLIS_PER_DAY) {
            return new SimpleDateFormat(ISO_8601_TIME_FORMAT_PATTERN);
        }
        if (value.getTime() % MILLIS_PER_DAY == 0) {
            return new SimpleDateFormat(ISO_8601_DATE_FORMAT_PATTERN);
        }
        return new SimpleDateFormat(ISO_8601_TIMESTAMP_FORMAT_PATTERN);
    }

    private byte[] toArray(ByteBuffer buffer) {
        return toArray(buffer, 0, buffer.remaining());
    }

    private byte[] toArray(ByteBuffer buffer, int offset, int size) {
        byte[] dest = new byte[size];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, dest, 0, size);
        } else {
            int pos = buffer.position();
            buffer.position(pos + offset);
            buffer.get(dest);
            buffer.position(pos);
        }
        return dest;
    }

    /**
     * Read a buffer into a Byte array for the given offset and length
     */
    private byte[] readBytes(ByteBuffer buffer, int offset, int length) {
        byte[] dest = new byte[length];
        if (buffer.hasArray()) {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, length);
        } else {
            buffer.mark();
            buffer.position(offset);
            buffer.get(dest, 0, length);
            buffer.reset();
        }
        return dest;
    }

    /**
     * Read the given byte buffer into a Byte array
     */
    private byte[] readBytes(ByteBuffer buffer) {
        return readBytes(buffer, 0, buffer.limit());
    }

    protected class MetaDetector {
        private Type knownType = null;

        public MetaDetector() {
        }

        public boolean canDetect(Object value) {
            if (value == null) {
                return true;
            }
            Meta schema = inferMeta();
            if (schema == null) {
                return false;
            }
            if (knownType == null) {
                knownType = schema.getType();
            } else if (knownType != schema.getType()) {
                return false;
            }
            return true;
        }

        public Meta meta() {
            return MetaBuilder.type(knownType).meta();
        }
    }

    protected class Parser {
        private final String original;
        private final CharacterIterator iter;
        private String nextToken = null;
        private String previousToken = null;

        public Parser(String original) {
            this.original = original;
            this.iter = new StringCharacterIterator(this.original);
        }

        public int position() {
            return iter.getIndex();
        }

        public int mark() {
            return iter.getIndex() - (nextToken != null ? nextToken.length() : 0);
        }

        public void rewindTo(int position) {
            iter.setIndex(position);
            nextToken = null;
        }

        public String original() {
            return original;
        }

        public boolean hasNext() {
            return nextToken != null || canConsumeNextToken();
        }

        protected boolean canConsumeNextToken() {
            return iter.getEndIndex() > iter.getIndex();
        }

        public String next() {
            if (nextToken != null) {
                previousToken = nextToken;
                nextToken = null;
            } else {
                previousToken = consumeNextToken();
            }
            return previousToken;
        }

        private String consumeNextToken() throws NoSuchElementException {
            boolean escaped = false;
            int start = iter.getIndex();
            char c = iter.current();
            while (c != CharacterIterator.DONE) {
                switch (c) {
                    case '\\':
                        escaped = !escaped;
                        break;
                    case ':':
                    case ',':
                    case '{':
                    case '}':
                    case '[':
                    case ']':
                    case '\"':
                        if (!escaped) {
                            if (start < iter.getIndex()) {
                                // Return the previous token
                                return original.substring(start, iter.getIndex());
                            }
                            // Consume and return this delimiter as a token
                            iter.next();
                            return original.substring(start, start + 1);
                        }
                        // escaped, so continue
                        escaped = false;
                        break;
                    default:
                        // If escaped, then we don't care what was escaped
                        escaped = false;
                        break;
                }
                c = iter.next();
            }
            return original.substring(start, iter.getIndex());
        }

        public String previous() {
            return previousToken;
        }

        public boolean canConsume(String expected) {
            return canConsume(expected, true);
        }

        public boolean canConsume(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
            if (isNext(expected, ignoreLeadingAndTrailingWhitespace)) {
                // consume this token ...
                nextToken = null;
                return true;
            }
            return false;
        }

        protected boolean isNext(String expected, boolean ignoreLeadingAndTrailingWhitespace) {
            if (nextToken == null) {
                if (!hasNext()) {
                    return false;
                }
                // There's another token, so consume it
                nextToken = consumeNextToken();
            }
            if (ignoreLeadingAndTrailingWhitespace) {
                nextToken = nextToken.trim();
                while (nextToken.isEmpty() && canConsumeNextToken()) {
                    nextToken = consumeNextToken().trim();
                }
            }
            return nextToken.equals(expected);
        }
    }
}
