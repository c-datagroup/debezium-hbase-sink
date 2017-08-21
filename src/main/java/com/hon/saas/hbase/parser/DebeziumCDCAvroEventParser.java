package com.hon.saas.hbase.parser;

import com.google.common.base.Preconditions;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by zhengwx on 8/17/17.
 */
public class DebeziumCDCAvroEventParser implements EventParser{
    private static Logger logger = LoggerFactory.getLogger(DebeziumCDCAvroEventParser.class);

    private final static AvroData avroData = new AvroData(100);
    private final Map<String, byte[]> EMPTY_MAP = Collections.emptyMap();
    private final String AFTER_KEY = "after";
    private final List<String> VALUE_FILTER_KEYS = Arrays.asList("before", "source", "ts_ms");
    private final List<String> VALUE_NESTED_KEYS = Arrays.asList(AFTER_KEY);

    public DebeziumCDCAvroEventParser(){
    }

    @Override
    public Map<String, byte[]> parseKey(SinkRecord sr) throws EventParsingException {
        return parse(sr.keySchema(), sr.key(), true);
    }

    @Override
    public Map<String, byte[]> parseValue(SinkRecord sr) throws EventParsingException {
        return parse(sr.valueSchema(), sr.value(), false);
    }

    /**
     * parses the value.
     * @param schema
     * @param value
     * @return
     */
    private Map<String, byte[]> parse(final Schema schema, final Object value, boolean isKey) {
        final Map<String, byte[]> values = new LinkedHashMap<>();
        try {
            Object data = avroData.fromConnectData(schema, value);
            if (data == null || !(data instanceof GenericRecord)) {
                return EMPTY_MAP;
            }
            final GenericRecord record = (GenericRecord) data;
            final List<Field> fields = schema.fields();
            for (Field field : fields) {
                if(isFilteredOff(field.name(), isKey)){
                    continue;
                }
                else if(isNestedKey(field.name())){
                    Object nestedData = record.get(field.name());
                    if(nestedData == null || !(nestedData instanceof  GenericRecord)){
                        continue;
                    }
                   parseNestedRecord(field.schema(), (GenericRecord)nestedData, values);
                }
                else {
                    final byte[] fieldValue = toValue(record, field);
                    if (fieldValue == null) {
                        continue;
                    }
                    values.put(field.name(), fieldValue);
                }
            }
            return values;
        } catch (Exception ex) {
            final String errorMsg = String.format("Failed to parse the schema [%s] , value [%s] with ex [%s]" ,
                    schema, value, ex.getMessage());
            logger.error(errorMsg);
            throw new EventParsingException(errorMsg, ex);
        }
    }

    private void parseNestedRecord(Schema schema, final GenericRecord record, Map<String, byte[]> values){
        final List<Field> fields = schema.fields();
        for (Field field : fields) {
            final byte[] fieldValue = toValue(record, field);
            if (fieldValue != null) {
                values.put(field.name(), fieldValue);
            }
        }
    }

    private boolean isFilteredOff(String fieldName, boolean isKey){
        if(!isKey){
            return VALUE_FILTER_KEYS.contains(fieldName);
        }
        return false;
    }

    private boolean isNestedKey(String fieldName){
        return VALUE_NESTED_KEYS.contains(fieldName);
    }

    private byte[] toValue(final GenericRecord record, final Field field) {
        Preconditions.checkNotNull(field);
        final Schema.Type type = field.schema().type();
        final String fieldName = field.name();
        final Object fieldValue = record.get(fieldName);
        if (fieldValue != null) {
            switch (type) {
                case STRING:
                    return Bytes.toBytes((String) fieldValue);
                case BOOLEAN:
                    return Bytes.toBytes((Boolean) fieldValue ? "1" : "0");
                case BYTES:
                    return Bytes.toBytes((ByteBuffer) fieldValue);
                case FLOAT32:
                    return Bytes.toBytes(((Float) fieldValue).toString());
                case FLOAT64:
                    return Bytes.toBytes(((Double) fieldValue).toString());
                case INT8:
                    return Bytes.toBytes(((Byte) fieldValue).toString());
                case INT16:
                    return Bytes.toBytes(((Integer) fieldValue).toString());
                case INT32:
                    return Bytes.toBytes(((Integer) fieldValue).toString());
                case INT64:
                    return Bytes.toBytes(((Long) fieldValue).toString());
                default:
                    return null;
            }
        }
        return null;
    }

}
