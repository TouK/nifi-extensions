package pl.touk.nifi.services;

import org.apache.ignite.binary.BinaryObject;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.serialization.record.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
    TODO:
    1. handle all ignite types
    2. more test cases
*/
public class IgniteRecordLookupService extends AbstractIgniteRecordLookup implements RecordLookupService {

    public static final String KEY_KEY = "key";
    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY_KEY).collect(Collectors.toSet()));

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException, RuntimeException {
        String key = (String)coordinates.get(KEY_KEY);
        BinaryObject binaryObject = getCache().get(key);

        if (binaryObject == null) {
            return Optional.empty();
        } else {
            return Optional.of(buildRecord(binaryObject));
        }
    }
}
