package pl.touk.nifi.services;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class AbstractDatabaseLookupService extends AbstractControllerService {

    static final Set<String> REQUIRED_KEYS = Collections.emptySet();

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-dbcp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-table-name")
            .displayName("Table Name")
            .description("The name of the database table to be queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-cache-size")
            .displayName("Cache Size")
            .description("Specifies how many lookup values/records should be cached. The cache is shared for all tables and keeps a map of lookup values to records. "
                    + "Setting this property to zero means no caching will be done and the table will be queried for each lookup value in each record. If the lookup "
                    + "table changes often or the most recent data must be retrieved, do not use the cache.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    static final PropertyDescriptor CLEAR_CACHE_ON_ENABLED = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-clear-cache-on-enabled")
            .displayName("Clear Cache on Enabled")
            .description("Whether to clear the cache when this service is enabled. If the Cache Size is zero then this property is ignored. Clearing the cache when the "
                    + "service is enabled ensures that the service will first go to the database to get the most recent data.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Cache Expiration")
            .description("Time interval to clear all cache entries. If the Cache Size is zero then this property is ignored.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected List<PropertyDescriptor> properties;

    DBCPService dbcpService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
