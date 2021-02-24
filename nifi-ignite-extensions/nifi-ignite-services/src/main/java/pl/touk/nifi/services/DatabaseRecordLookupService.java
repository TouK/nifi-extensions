package pl.touk.nifi.services;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.Tuple;
import pl.touk.nifi.common.ThrowingConsumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"lookup", "cache", "enrich", "join", "rdbms", "database", "reloadable", "key", "value", "record"})
@CapabilityDescription("A relational-database-based lookup service. When the lookup key is found in the database, "
        + "the specified columns (or all if Lookup Value Columns are not specified) are returned as a Record. Only one row "
        + "will be returned for each lookup, duplicate database entries are ignored.")
public class DatabaseRecordLookupService extends AbstractDatabaseLookupService implements RecordLookupService {

    private volatile Cache<Tuple<String, Object>, Record> cache;

    protected static final String ARG_PREFIX = "arg";

    protected static final String ARGS_PATTERN = "^arg[0-9]+$";

    static final PropertyDescriptor LOOKUP_VALUE_COLUMNS = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-value-columns")
            .displayName("Lookup Value Columns")
            .description("A comma-delimited list of columns in the table that will be returned when the lookup key matches. Note that this may be case-sensitive depending on the database.")
            .required(false)
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

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DBCP_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(LOOKUP_VALUE_COLUMNS);
        properties.add(CACHE_SIZE);
        properties.add(CLEAR_CACHE_ON_ENABLED);
        properties.add(CACHE_EXPIRATION);
        properties.add(WHERE_CLAUSE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        final boolean clearCache = context.getProperty(CLEAR_CACHE_ON_ENABLED).asBoolean();
        final long durationNanos = context.getProperty(CACHE_EXPIRATION).isSet() ? context.getProperty(CACHE_EXPIRATION).evaluateAttributeExpressions().asTimePeriod(TimeUnit.NANOSECONDS) : 0L;
        if (this.cache == null || (cacheSize > 0 && clearCache)) {
            if (durationNanos > 0) {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfter(new Expiry<Tuple<String, Object>, Record>() {
                            @Override
                            public long expireAfterCreate(Tuple<String, Object> stringObjectTuple, Record record, long currentTime) {
                                return durationNanos;
                            }

                            @Override
                            public long expireAfterUpdate(Tuple<String, Object> stringObjectTuple, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }

                            @Override
                            public long expireAfterRead(Tuple<String, Object> stringObjectTuple, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }
                        })
                        .build();
            } else {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .build();
            }
        }
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        return lookup(coordinates, null);
    }

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        // TODO
        final Object key = coordinates.get("TODO");
        if (StringUtils.isBlank(key.toString())) {
            return Optional.empty();
        }

        final String tableName = getProperty(TABLE_NAME).evaluateAttributeExpressions(context).getValue();
        final String lookupValueColumnsList = getProperty(LOOKUP_VALUE_COLUMNS).evaluateAttributeExpressions(context).getValue();
        final String whereClause = getProperty(WHERE_CLAUSE).evaluateAttributeExpressions(context).getValue();

        Set<String> lookupValueColumnsSet = new LinkedHashSet<>();
        if (lookupValueColumnsList != null) {
            Stream.of(lookupValueColumnsList)
                    .flatMap(path -> Arrays.stream(path.split(",")))
                    .filter(DatabaseRecordLookupService::isNotBlank)
                    .map(String::trim)
                    .forEach(lookupValueColumnsSet::add);
        }

        final String lookupValueColumns = lookupValueColumnsSet.isEmpty() ? "*" : String.join(",", lookupValueColumnsSet);

        Tuple<String, Object> cacheLookupKey = new Tuple<>(tableName, key);

        // Not using the function param of cache.get so we can catch and handle the checked exceptions
        Record foundRecord = cache.get(cacheLookupKey, k -> null);

        if (foundRecord == null) {
            final String selectQuery = "SELECT " + lookupValueColumns + " FROM " + tableName + " WHERE " + whereClause;

            try (final Connection con = dbcpService.getConnection(context);
                 final PreparedStatement st = con.prepareStatement(selectQuery)) {

                getQueryArgs(coordinates).forEach(
                        consumeWithSqlExceptionHandle(arg ->
                                st.setObject(arg.getKey(), arg.getValue())
                        )
                );
                ResultSet resultSet = st.executeQuery();
                ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, null);
                foundRecord = resultSetRecordSet.next();

                // Populate the cache if the record is present
                if (foundRecord != null) {
                    cache.put(cacheLookupKey, foundRecord);
                }

            } catch (SQLException se) {
                throw new LookupFailureException("Error executing SQL statement: " + selectQuery + "for value " + key.toString()
                        + " : " + (se.getCause() == null ? se.getMessage() : se.getCause().getMessage()), se);
            } catch (IOException ioe) {
                throw new LookupFailureException("Error retrieving result set for SQL statement: " + selectQuery + "for value " + key.toString()
                        + " : " + (ioe.getCause() == null ? ioe.getMessage() : ioe.getCause().getMessage()), ioe);
            }
        }

        return Optional.ofNullable(foundRecord);
    }

    private static boolean isNotBlank(final String value) {
        return value != null && !value.trim().isEmpty();
    }

    private static List<Tuple<Integer, Object>> getQueryArgs(Map<String, Object> coordinates) {
        return coordinates.entrySet().stream()
                .filter(entry -> entry.getKey().matches(ARGS_PATTERN))
                .map(entry -> {
                    Integer argIx = Integer.valueOf((entry.getKey()).replaceFirst(ARG_PREFIX, ""));
                    return new Tuple<Integer, Object>(argIx, entry.getValue());
                })
                .collect(Collectors.toList());
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

    static <A> Consumer<A> consumeWithSqlExceptionHandle(ThrowingConsumer<A, SQLException> consumer) {
        return a -> {
            try {
                consumer.accept(a);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}
