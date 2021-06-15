package pl.touk.nifi.services;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.List;

public class TestProcessor extends AbstractProcessor {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(new PropertyDescriptor.Builder()
                .name("Test processor")
                .identifiesControllerService(AbstractControllerService.class)
                .required(true)
                .build());
        return propDescs;
    }
}

