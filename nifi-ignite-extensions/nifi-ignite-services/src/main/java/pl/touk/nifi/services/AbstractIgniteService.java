package pl.touk.nifi.services;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractIgniteService extends AbstractControllerService {

    public static final PropertyDescriptor IGNITE_CONFIGURATION_FILE = new PropertyDescriptor.Builder()
            .displayName("Ignite Spring Properties Xml File")
            .name("ignite-spring-properties-xml-file")
            .description("Ignite spring configuration file, <path>/<ignite-configuration>.xml. If the " +
                    "configuration file is not provided, default Ignite configuration " +
                    "configuration is used which binds to 127.0.0.1:47500..47509")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.of(IGNITE_CONFIGURATION_FILE).collect(Collectors.toList());
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
    }

    private transient Ignite ignite;

    protected Ignite getIgnite() {
        return ignite;
    }

    public void closeIgnite() {
        if (ignite != null) {
            getLogger().info("Closing ignite client");
            ignite.close();
            ignite = null;
        }
    }

    public void initializeIgnite(ConfigurationContext context) {
        if (getIgnite() != null) {
            getLogger().info("Ignite already initialized");
            return;
        }
        synchronized (Ignition.class) {
            List<Ignite> grids = Ignition.allGrids();

            if (grids.size() == 1) {
                getLogger().info("Ignite grid already available");
                ignite = grids.get(0);
                return;
            }
            Ignition.setClientMode(true);
            String configuration = context.getProperty(IGNITE_CONFIGURATION_FILE).getValue();
            getLogger().info("Initializing ignite with configuration {} ", new Object[]{configuration});
            if (StringUtils.isEmpty(configuration)) {
                ignite = Ignition.start();
            } else {
                ignite = Ignition.start(configuration);
            }
        }
    }
}
