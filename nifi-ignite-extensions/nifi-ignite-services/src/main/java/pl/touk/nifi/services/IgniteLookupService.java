package pl.touk.nifi.services;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteLookupService extends AbstractIgniteCache<String, BinaryObject> implements RecordLookupService {

    public static final String KEY_KEY = "key";
    private static final String IGNITE_THIS_FIELD_NAME = "this$0";
    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY_KEY).collect(Collectors.toSet()));

    /*
    TODO:
    1. validate keys to be string
    2. handle all ignite types
    3. select fields
    */
    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException, RuntimeException {
        String key = (String)coordinates.get(KEY_KEY);

        BinaryObject binaryObject = getCache().get(key);
        if (binaryObject == null) {
            return Optional.empty();
        }

        BinaryType igniteType = binaryObject.type();
        Collection<String> fieldNames = igniteType
                .fieldNames().stream()
                .filter(name -> !name.equals(IGNITE_THIS_FIELD_NAME))
                .collect(Collectors.toList());

        Map<String, Object> record = new HashMap<>();
        fieldNames.forEach(fieldName ->
                record.put(fieldName, binaryObject.field(fieldName))
        );

        RecordSchema recordSchema = getRecordSchema(igniteType, fieldNames);
        return Optional.of(new MapRecord(recordSchema, record));
    }

    private RecordSchema getRecordSchema(BinaryType igniteType, Collection<String> fieldNames) throws RuntimeException {
        List<RecordField> recordFields = new ArrayList<>();
        fieldNames.forEach(fieldName -> {
                try {
                    recordFields.add(new RecordField(fieldName, getRecordType(igniteType, fieldName)));
                } catch (LookupFailureException e) {
                    throw new RuntimeException(e.getMessage());
                }
            }
        );
        return new SimpleRecordSchema(recordFields);
    }

    private DataType getRecordType(BinaryType igniteType, String fieldName) throws LookupFailureException {
        String igniteTypeName = igniteType.fieldTypeName(fieldName);
        if (igniteTypeName.equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.STRING))) {
           return RecordFieldType.STRING.getDataType();
        } else if (igniteTypeName.equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.LONG))) {
            return RecordFieldType.LONG.getDataType();
        } else if (igniteTypeName.equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.INT))) {
            return RecordFieldType.INT.getDataType();
        } else if (igniteTypeName.equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.BOOLEAN))) {
            return RecordFieldType.BOOLEAN.getDataType();
        } else if (igniteTypeName.equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.DOUBLE))) {
            return RecordFieldType.DOUBLE.getDataType();
        } else if (igniteTypeName.equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.TIMESTAMP))) {
            return RecordFieldType.TIMESTAMP.getDataType();
        } else if (igniteTypeName.equals(BinaryUtils.fieldTypeName(GridBinaryMarshaller.BYTE))) {
            return RecordFieldType.BYTE.getDataType();
        } else {
            throw new LookupFailureException("Unsupported Ignite type " + igniteTypeName);
        }
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}
