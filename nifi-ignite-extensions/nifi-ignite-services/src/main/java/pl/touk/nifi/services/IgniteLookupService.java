package pl.touk.nifi.services;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteLookupService extends AbstractIgniteCacheService implements RecordLookupService {

    public static final String KEY_KEY = "key";

    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY_KEY).collect(Collectors.toSet()));
    /*
    TODO:
    1. validate keys to be string
    2. types other than string
    */

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        String key = (String)coordinates.get(KEY_KEY);
        List<RecordField> recordFields = new ArrayList<>();
        BinaryObject binaryObject = getIgniteCache().get(key);
        if (binaryObject == null) {
            return Optional.empty();
        }
        BinaryType binaryType = binaryObject.type();
        Collection<String> fieldNames = binaryType.fieldNames();
        fieldNames.forEach(fieldName -> recordFields.add(new RecordField(fieldName, RecordFieldType.STRING.getDataType())));
        RecordSchema lookupRecordSchema = new SimpleRecordSchema(recordFields);
        Map<String, Object> record = new HashMap<>();
        fieldNames.forEach(fieldName -> record.put(fieldName, binaryObject.field(fieldName)));
        return Optional.of(
                new MapRecord(lookupRecordSchema, record)
        );
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}
