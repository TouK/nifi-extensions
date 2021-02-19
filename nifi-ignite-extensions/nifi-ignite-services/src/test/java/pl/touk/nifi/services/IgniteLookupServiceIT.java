package pl.touk.nifi.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pl.touk.nifi.utils.PortFinder;
import pl.touk.nifi.utils.IgniteTestUtil;

import java.io.IOException;
import java.util.*;

public class IgniteLookupServiceIT {

    private final static String CACHE_NAME = "my-cache";

    private Ignite igniteServer;
    private Ignite igniteClient;

    private TestRunner runner;
    private IgniteLookupService service;

    @Before
    public void before() throws IOException, InitializationException {
        final int ignitePort = PortFinder.getAvailablePort();
        final int clientConnectorPort = PortFinder.getAvailablePort();
        ClientConnectorConfiguration clientConfiguration = new ClientConnectorConfiguration().setPort(clientConnectorPort);
        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConfiguration);
        igniteClient = IgniteTestUtil.startClient(ignitePort);
        igniteClient.createCache(CACHE_NAME);

        runner = TestRunners.newTestRunner(TestLookupProcessor.class);
        service = new IgniteLookupService();
        runner.addControllerService("ignite-lookup-service", service);
        runner.setProperty(service, IgniteDistributedMapCacheClient.SERVER_ADDRESSES, "localhost:" + clientConnectorPort);
        runner.setProperty(service, IgniteLookupService.CACHE_NAME, CACHE_NAME);
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @After
    public void after() throws Exception {
        service.onDisabled();
        igniteClient.close();
        igniteServer.close();
    }

    @Test
    public void testServiceLookup() throws LookupFailureException {
        MyRecord myRecord = new MyRecord();
        igniteClient.cache(CACHE_NAME).put(myRecord.name, myRecord);
        IgniteCache<String, BinaryObject> binaryCache = igniteClient
                .cache(CACHE_NAME).withKeepBinary();
        BinaryObject binaryObject = binaryCache.get(myRecord.name);
        Assert.assertEquals(myRecord.name, binaryObject.field("name"));

        Map<String, Object> coordinates = new HashMap<>();
        coordinates.put("key", myRecord.name);
        Optional<Record> optionalRecord = service.lookup(coordinates);
        Assert.assertEquals(myRecord.name, optionalRecord.get().toMap().get("name"));
        Assert.assertEquals(myRecord.age, optionalRecord.get().toMap().get("age"));
        Assert.assertEquals(myRecord.active, optionalRecord.get().toMap().get("active"));
    }

    private class MyRecord {
        public String name;
        private int age;
        private boolean active;
        MyRecord() {
            this.name = "my-record";
            this.age = 42;
            this.active = true;
        }
    }
}
