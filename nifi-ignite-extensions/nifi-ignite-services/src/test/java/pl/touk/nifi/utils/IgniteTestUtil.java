package pl.touk.nifi.utils;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import java.util.Collections;

public class IgniteTestUtil {

    public static Ignite startServer(int port) {
        return Ignition.start(getServerConfig(port));
    }

    public static Ignite startServer(int port, ClientConnectorConfiguration clientConnectorConfig) {
        IgniteConfiguration serverConfig = getServerConfig(port)
                .setClientConnectorConfiguration(clientConnectorConfig);

        return Ignition.start(serverConfig);
    }

    public static Ignite startClient(int port) {
        IgniteConfiguration clientConfig = new IgniteConfiguration()
                .setIgniteInstanceName("my-client")
                .setDiscoverySpi(getDiscoverSpi(port))
                .setClientMode(true);

        return Ignition.start(clientConfig);
    }

    private static IgniteConfiguration getServerConfig(int port) {
        return new IgniteConfiguration()
                .setIgniteInstanceName("my-server")
                .setDiscoverySpi(getDiscoverSpi(port));
    }

    private static DiscoverySpi getDiscoverSpi(int port) {
        return new TcpDiscoverySpi()
                .setIpFinder(
                        new TcpDiscoveryMulticastIpFinder()
                                .setAddresses(Collections.singleton("127.0.0.1:" + port))
                );
    }
}
