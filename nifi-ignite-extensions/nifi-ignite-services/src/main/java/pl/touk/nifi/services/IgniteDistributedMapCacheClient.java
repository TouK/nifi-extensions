package pl.touk.nifi.services;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.*;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
TODO:
1. Implement AtomicDistributedMapCacheClient<byte[]>
2. Add support for DistributedMapCacheClient#removeByPattern
 */
public class IgniteDistributedMapCacheClient extends AbstractControllerService implements DistributedMapCacheClient {

    private volatile IgniteClient client;

    private volatile ClientCache<byte[], byte[]> cache;

    public static final PropertyDescriptor SERVER_ADDRESSES = new PropertyDescriptor.Builder()
            .name("ignite-addresses")
            .displayName("Ignite addresses")
            .description("Comma-separated list of Ignite host addresses for thin client, eg. host1:10800,host2:10800")
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_EMPTY_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    public static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .name("ignite-cache-name")
            .displayName("Ignite cache name. If cache is non-existing for provided name, it will be created on service enable")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SERVER_ADDRESSES);
        props.add(CACHE_NAME);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        String[] hostAddresses = context.getProperty(SERVER_ADDRESSES).getValue().split(",");
        PropertyValue cacheName = context.getProperty(CACHE_NAME);
        ClientConfiguration cfg = new ClientConfiguration().setAddresses(hostAddresses);
        getLogger().info("Initializing Ignite thin client for " + context.getName() + "with cache " + cacheName.getValue());
        client = Ignition.startClient(cfg);
        cache = client.getOrCreateCache(cacheName.getValue());
    }

    @OnDisabled
    public void onDisabled() throws Exception {
        getLogger().info("Closing Ignite thin client for DistributedMapCacheClient");
        client.close();
    }


    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        return cache.putIfAbsent(kv.getKey(), kv.getValue());
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        final Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        if (cache.putIfAbsent(kv.getKey(), kv.getValue())) {
            return value;
        } else {
            return null;
        }
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        return cache.containsKey(keyBytes);
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final Tuple<byte[],byte[]> kv = serialize(key, value, keySerializer, valueSerializer);
        cache.put(kv.getKey(), kv.getValue());
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        byte[] valueBytes = cache.get(keyBytes);
        return valueBytes == null ? null : valueDeserializer.deserialize(valueBytes);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public <K> boolean remove(K key, Serializer<K> keySerializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        return cache.remove(keyBytes);
    }

    @Override
    public long removeByPattern(String s) {
        throw new UnsupportedOperationException("Remove by pattern is not supported");
    }

    private <K, V> Tuple<byte[],byte[]> serialize(final K key, final V value, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        keySerializer.serialize(key, out);
        final byte[] k = out.toByteArray();
        out.reset();
        valueSerializer.serialize(value, out);
        final byte[] v = out.toByteArray();
        return new Tuple<>(k, v);
    }

    private <K> byte[] serialize(final K key, final Serializer<K> keySerializer) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        keySerializer.serialize(key, out);
        return out.toByteArray();
    }
}
