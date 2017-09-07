package com.as.kinezis.withWorker;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class AmazonKinesisApplicationRecordProcessorFactory implements IRecordProcessorFactory {

	public IRecordProcessor createProcessor() {
		return new AmazonKinesisApplicationSampleRecordProcessor();
	}

	
	
	
}
