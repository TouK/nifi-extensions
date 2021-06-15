package pl.touk.nifi.services.db.inmemory;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import pl.touk.nifi.services.TestProcessor;
import pl.touk.nifi.utils.PortFinder;
import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.*;

public class H2ServerServiceTest {

    @Test
    public void testCreateAndDrop() throws InitializationException, IOException, SQLException {
        String initDdl = "create table public.t1(id int); insert into public.t1 (id) values (1);";

        H2ServerService h2ServerService = new H2ServerService();
        TestRunner testRunner = TestRunners.newTestRunner(TestProcessor.class);
        testRunner.addControllerService("h2Server", h2ServerService);
        String port = Integer.toString(PortFinder.getAvailablePort());
        testRunner.setProperty(h2ServerService, H2ServerService.PORT, port);
        testRunner.setProperty(h2ServerService, H2ServerService.INIT_DB_DDL, initDdl);
        testRunner.enableControllerService(h2ServerService);

        String connUrl = "jdbc:h2:tcp://localhost:" + port + "/mem:" + H2ServerService.DB_NAME;
        Connection conn = DriverManager.getConnection(connUrl);
        PreparedStatement statement = conn.prepareStatement("select id from public.t1 where id = 1");
        ResultSet resultSet = statement.executeQuery();

        testRunner.disableControllerService(h2ServerService);

        assert resultSet.next();
        assertEquals(1, resultSet.getInt(1));
    }
}
