package pl.touk.nifi.services;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.Tuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class IgniteRecordSqlLookupService extends AbstractIgniteRecordLookup<Object> implements RecordLookupService  {

    protected static final String ARG_PREFIX = "arg";
    protected static final String ARGS_PATTERN = "^arg[0-9]+$";

    protected static final PropertyDescriptor KEY_COLUMN = new PropertyDescriptor.Builder()
            .name("key-column")
            .displayName("Key column")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected static final PropertyDescriptor WHERE_CLAUSE = new PropertyDescriptor.Builder()
            .name("where-clause")
            .displayName("Where")
            .description("Where clause, eg. age > ?")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private volatile String keyColumnName;
    private volatile String whereClauseStr;

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.concat(
                super.getSupportedPropertyDescriptors().stream(),
                Stream.of(KEY_COLUMN, WHERE_CLAUSE).collect(Collectors.toList()).stream()
        ).collect(Collectors.toList());
    }

    @Override @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        keyColumnName = context.getProperty(KEY_COLUMN)
                .evaluateAttributeExpressions().getValue().toUpperCase();
        whereClauseStr = context.getProperty(WHERE_CLAUSE)
                .evaluateAttributeExpressions().getValue();
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        FieldsQueryCursor<List<?>> queryCursor = getCache().query(getQuery(coordinates));
        Optional<Record> recordOption = getFirst(queryCursor);
        queryCursor.close();
        return recordOption;
    }

    private SqlFieldsQuery getQuery(Map<String, Object> coordinates) {
        return new SqlFieldsQuery(
                "SELECT " + keyColumnName + " FROM " + cacheName + " WHERE " + whereClauseStr + ";"
        ).setArgs(getSortedArgs(coordinates));
    }

    private Optional<Record> getFirst(FieldsQueryCursor<List<?>> queryCursor) {
        Iterator<List<?>> queryIterator = queryCursor.iterator();
        if (queryIterator.hasNext()) {
            List<?> firstFound = queryIterator.next();
            Object keyValue = firstFound.get(0);
            BinaryObject binaryObject = getCache().get(keyValue);
            return Optional.of(buildRecord(binaryObject));
        } else {
            return Optional.empty();
        }
    }

    private Object[] getSortedArgs(Map<String, Object> coordinates) {
        return coordinates.entrySet().stream()
                .filter(entry -> entry.getKey().matches(ARGS_PATTERN))
                .map(entry -> {
                    Integer argIx = Integer.valueOf((entry.getKey()).replaceFirst(ARG_PREFIX, ""));
                    return new Tuple<Integer, Object>(argIx, entry.getValue());
                })
                .sorted(Comparator.comparing(Tuple::getKey))
                .map(Tuple::getValue).toArray(Object[]::new);
    }
}