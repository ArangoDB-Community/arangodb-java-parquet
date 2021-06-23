package com.arangodb.parquet.serde;


import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Utility Class For Proper Avro To JSON Conversion.
 * From https://stackoverflow.com/a/63742827
 */
public class GenericRecordJsonEncoder {

    Map<LogicalType, Function<Object, Object>> logicalTypesConverters = new HashMap<>();

    public void registerLogicalTypeConverter(LogicalType logicalType, Function<Object, Object> converter) {
        this.logicalTypesConverters.put(logicalType, converter);
    }

    public Function<Object, Object> getLogicalTypeConverter(Schema.Field field) {
        Schema fieldSchema = field.schema();
        LogicalType logicalType = fieldSchema.getLogicalType();
        return getLogicalTypeConverter(logicalType);
    }

    public Function<Object, Object> getLogicalTypeConverter(LogicalType logicalType) {
        if (logicalType == null) {
            return Function.identity();
        }

        return logicalTypesConverters.getOrDefault(logicalType, Function.identity());
    }

    public String serialize(GenericRecord value) {
        StringBuilder buffer = new StringBuilder();
        serialize(value, buffer, new IdentityHashMap<>(128) );
        String result = buffer.toString();
        return result;
    }

    private static final String TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT =
            " \">>> CIRCULAR REFERENCE CANNOT BE PUT IN JSON STRING, ABORTING RECURSION <<<\" ";

    /** Renders a Java datum as <a href="http://www.json.org/">JSON</a>. */
    private void serialize(final Object datum, final StringBuilder buffer, final IdentityHashMap<Object, Object> seenObjects) {
        if (isRecord(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            buffer.append("{");
            int count = 0;
            Schema schema = getRecordSchema(datum);
            for (Schema.Field f : schema.getFields()) {
                serialize(f.name(), buffer, seenObjects);
                buffer.append(": ");
                Function<Object, Object> logicalTypeConverter = getLogicalTypeConverter(f);
                serialize(logicalTypeConverter.apply(getField(datum, f.name(), f.pos())), buffer, seenObjects);
                if (++count < schema.getFields().size())
                    buffer.append(", ");
            }
            buffer.append("}");
            seenObjects.remove(datum);
        } else if (isArray(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            Collection<?> array = getArrayAsCollection(datum);
            buffer.append("[");
            long last = array.size()-1;
            int i = 0;
            for (Object element : array) {
                serialize(element, buffer, seenObjects);
                if (i++ < last)
                    buffer.append(", ");
            }
            buffer.append("]");
            seenObjects.remove(datum);
        } else if (isMap(datum)) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            buffer.append("{");
            int count = 0;
            @SuppressWarnings(value="unchecked")
            Map<Object,Object> map = (Map<Object,Object>)datum;
            for (Map.Entry<Object,Object> entry : map.entrySet()) {
                serialize(entry.getKey(), buffer, seenObjects);
                buffer.append(": ");
                serialize(entry.getValue(), buffer, seenObjects);
                if (++count < map.size())
                    buffer.append(", ");
            }
            buffer.append("}");
            seenObjects.remove(datum);
        } else if (isString(datum)|| isEnum(datum)) {
            buffer.append("\"");
            writeEscapedString(datum.toString(), buffer);
            buffer.append("\"");
        } else if (isBytes(datum)) {
            buffer.append("{\"bytes\": \"");
            ByteBuffer bytes = ((ByteBuffer) datum).duplicate();
            writeEscapedString(StandardCharsets.ISO_8859_1.decode(bytes), buffer);
            buffer.append("\"}");
        } else if (((datum instanceof Float) &&       // quote Nan & Infinity
                (((Float)datum).isInfinite() || ((Float)datum).isNaN()))
                || ((datum instanceof Double) &&
                (((Double)datum).isInfinite() || ((Double)datum).isNaN()))) {
            buffer.append("\"");
            buffer.append(datum);
            buffer.append("\"");
        } else if (datum instanceof GenericData) {
            if (seenObjects.containsKey(datum)) {
                buffer.append(TOSTRING_CIRCULAR_REFERENCE_ERROR_TEXT);
                return;
            }
            seenObjects.put(datum, datum);
            serialize(datum, buffer, seenObjects);
            seenObjects.remove(datum);
        } else {
            // This fallback is the reason why GenericRecord toString does not
            // generate a valid JSON representation
            buffer.append(datum);
        }
    }

    // All these methods are also copied from the GenericData class source

    private boolean isRecord(Object datum) {
        return datum instanceof IndexedRecord;
    }

    private Schema getRecordSchema(Object record) {
        return ((GenericContainer)record).getSchema();
    }

    private Object getField(Object record, String name, int position) {
        return ((IndexedRecord)record).get(position);
    }

    private boolean isArray(Object datum) {
        return datum instanceof Collection;
    }

    private Collection getArrayAsCollection(Object datum) {
        return (Collection)datum;
    }

    private boolean isEnum(Object datum) {
        return datum instanceof GenericEnumSymbol;
    }

    private boolean isMap(Object datum) {
        return datum instanceof Map;
    }

    private boolean isString(Object datum) {
        return datum instanceof CharSequence;
    }

    private boolean isBytes(Object datum) {
        return datum instanceof ByteBuffer;
    }

    private void writeEscapedString(CharSequence string, StringBuilder builder) {
        for(int i = 0; i < string.length(); i++){
            char ch = string.charAt(i);
            switch(ch){
                case '"':
                    builder.append("\\\"");
                    break;
                case '\\':
                    builder.append("\\\\");
                    break;
                case '\b':
                    builder.append("\\b");
                    break;
                case '\f':
                    builder.append("\\f");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    // Reference: http://www.unicode.org/versions/Unicode5.1.0/
                    if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
                        String hex = Integer.toHexString(ch);
                        builder.append("\\u");
                        for(int j = 0; j < 4 - hex.length(); j++)
                            builder.append('0');
                        builder.append(hex.toUpperCase());
                    } else {
                        builder.append(ch);
                    }
            }
        }
    }
}