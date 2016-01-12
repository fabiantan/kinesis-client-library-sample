package com.amazonaws.samples;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
* Used to create new record processors.
*/
public class SampleRecordProcessorFactory implements IRecordProcessorFactory {
    
    /**
* Constructor.
*/
    public SampleRecordProcessorFactory() {
        super();
    }

    /**
* {@inheritDoc}
*/
    @Override
    public IRecordProcessor createProcessor() {
        return new SampleRecordProcessor();
    }

}
