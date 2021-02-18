package pl.touk.nifi.services;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public abstract class AbstractIgniteCacheService extends AbstractIgniteService {

    protected static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Name")
            .name("ignite-cache-name")
            .description("The name of the ignite cache")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private String cacheName;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.concat(
                super.getSupportedPropertyDescriptors().stream(),
                Stream.of(CACHE_NAME).collect(Collectors.toList()).stream()
        ).collect(Collectors.toList());
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        initializeIgniteCache(context);
    }

    @OnDisabled
    public void onDisabled() {
        closeIgniteCache();
    }

    protected IgniteCache<String, BinaryObject> getIgniteCache() {
        if ( getIgnite() == null )
            return null;
        else
            return getIgnite().getOrCreateCache(cacheName).withKeepBinary();
    }

    public void initializeIgniteCache(ConfigurationContext context) throws InitializationException {
        getLogger().info("Initializing Ignite cache");
        try {
            if ( getIgnite() == null ) {
                getLogger().info("Initializing ignite as client");
                super.initializeIgnite(context);
            }
            cacheName = context.getProperty(CACHE_NAME).getValue();
        } catch (Exception e) {
            getLogger().error("Failed to initialize ignite cache due to {}", new Object[] { e }, e);
            throw new InitializationException(e);
        }
    }

    public void closeIgniteCache() {
        if (getIgniteCache() != null) {
            getLogger().info("Closing ignite cache");
            getIgniteCache().close();
        }
        super.closeIgnite();
    }
}
