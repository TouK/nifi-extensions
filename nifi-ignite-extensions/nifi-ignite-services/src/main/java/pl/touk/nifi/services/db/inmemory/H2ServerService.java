package pl.touk.nifi.services.db.inmemory;

import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.h2.tools.Server;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class H2ServerService extends AbstractControllerService {

    private Server server;

    protected List<PropertyDescriptor> properties;

    static final String DB_NAME = "db";

    static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("h2-server-port")
            .displayName("Port")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor INIT_DB_DDL = new PropertyDescriptor.Builder()
            .name("h2-server-init-ddl")
            .displayName("Init DB DDL")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PORT);
        properties.add(INIT_DB_DDL);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void createDb(final ConfigurationContext context) throws SQLException {
        String port = context.getProperty(PORT).getValue();
        String initDdl = context.getProperty(INIT_DB_DDL).getValue();
        server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers", "-ifNotExists");
        server.start();

        String connUrl = "jdbc:h2:tcp://localhost:" + port + "/mem:" + DB_NAME + ";DB_CLOSE_DELAY=-1";
        try (Connection conn = DriverManager.getConnection(connUrl)) {
            if (initDdl != null) {
                PreparedStatement initDllStatement = conn.prepareStatement(initDdl);
                initDllStatement.execute();
            }
        }
    }

    @OnDisabled
    public void dropDb() {
        if (server != null) {
            server.shutdown();
        }
    }
}
