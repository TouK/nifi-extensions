package pl.touk.nifi.services;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReaderFactory;

@Tags({"example"})
@CapabilityDescription("Example Service API.")
public interface MyService extends RecordReaderFactory {

    public void execute()  throws ProcessException;

}