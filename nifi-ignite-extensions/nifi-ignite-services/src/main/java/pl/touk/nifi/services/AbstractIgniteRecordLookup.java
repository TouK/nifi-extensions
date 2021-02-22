package pl.touk.nifi.services;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractIgniteRecordLookup extends AbstractIgniteCache<String, BinaryObject> implements RecordLookupService {

    private static final String IGNITE_THIS_FIELD_NAME = "this$0";

    protected static final PropertyDescriptor FIELDS_TO_RETURN = new PropertyDescriptor.Builder()
            .name("cache-field-names")
            .displayName("Fields to return")
            .description("Comma separated list of fields to be returned")
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_EMPTY_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private volatile String lastUUID;
    private volatile Optional<RecordSchema> recordSchemaCache = Optional.empty();
    private volatile List<String> fieldsToReturn = null;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.concat(
                super.getSupportedPropertyDescriptors().stream(),
                Stream.of(FIELDS_TO_RETURN).collect(Collectors.toList()).stream()
        ).collect(Collectors.toList());
    }

    @Override @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        String fieldsToReturnStr = context.getProperty(FIELDS_TO_RETURN)
                .evaluateAttributeExpressions().getValue();
        if (fieldsToReturnStr != null) {
            fieldsToReturn = Arrays.asList(fieldsToReturnStr.split(","));
        }
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates, Map<String, String> flowFileAttrs) throws LookupFailureException {
        String currentUUID = flowFileAttrs.get(CoreAttributes.UUID.name());
        if (lastUUID == null) {
            lastUUID = currentUUID;
        } else if (!currentUUID.equals(lastUUID)) {
            recordSchemaCache = Optional.empty();
        }

        return lookup(coordinates);
    }

    protected Record buildRecord(BinaryObject binaryObject) {
        RecordSchema recordSchema = recordSchemaCache.orElse(calculateRecordSchema(binaryObject));
        Map<String, Object> record = new HashMap<>();
        recordSchema.getFieldNames().forEach(fieldName ->
                record.put(fieldName, binaryObject.field(fieldName))
        );
        return new MapRecord(recordSchema, record);
    }

    private RecordSchema calculateRecordSchema(BinaryObject binaryObject) throws RuntimeException {
        BinaryType igniteType = binaryObject.type();
        List<String> fieldNames = igniteType
                .fieldNames().stream()
                .filter(name -> !name.equals(IGNITE_THIS_FIELD_NAME))
                .filter(name -> (fieldsToReturn == null) || fieldsToReturn.contains(name))
                .collect(Collectors.toList());
        List<RecordField> recordFields = new ArrayList<>();
        fieldNames.forEach(fieldName -> {
                    try {
                        recordFields.add(new RecordField(fieldName, getRecordType(igniteType, fieldName)));
                    } catch (LookupFailureException e) {
                        throw new RuntimeException(e.getMessage());
                    }
                }
        );
        RecordSchema schema = new SimpleRecordSchema(recordFields);
        recordSchemaCache = Optional.of(schema);
        return schema;
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
}
