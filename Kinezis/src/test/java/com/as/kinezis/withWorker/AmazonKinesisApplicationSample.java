package com.as.kinezis.withWorker;

/*
 * Copyright 2012-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.net.InetAddress;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * Sample Amazon Kinesis Application.
 */
public final class AmazonKinesisApplicationSample {

	/*
	 * Before running the code: Fill in your AWS access credentials in the provided
	 * credentials file template, and be sure to move the file to the default
	 * location (~/.aws/credentials) where the sample code will load the credentials
	 * from. https://console.aws.amazon.com/iam/home?#security_credential
	 *
	 * WARNING: To avoid accidental leakage of your credentials, DO NOT keep the
	 * credentials file in your source directory.
	 */

	public static final String SAMPLE_APPLICATION_STREAM_NAME = "scg-attendance";

	private static final String SAMPLE_APPLICATION_NAME = "Ticketek";

	// Initial position in the stream when the application starts up for the first
	// time.
	// Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest
	// available data)
	private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM = InitialPositionInStream.TRIM_HORIZON;

	private static AWSCredentialsProvider credentialsProvider;

	private static void init() {
		// Ensure the JVM will refresh the cached IP values of AWS resources (e.g.
		// service endpoints).
		// java.security.Security.setProperty("networkaddress.cache.ttl", "60");

		/*
		 * The ProfileCredentialsProvider will return your [default] credential profile
		 * by reading from the credentials file located at (~/.aws/credentials).
		 */
		credentialsProvider = new ProfileCredentialsProvider();
		try {
			credentialsProvider.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
	}

	public static void main(String[] args) throws Exception {
		init();

		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
				SAMPLE_APPLICATION_NAME, SAMPLE_APPLICATION_STREAM_NAME, credentialsProvider, workerId);
		kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);
		

		// TODO seted region name
		kinesisClientLibConfiguration.withRegionName("ap-southeast-2");
		System.out.println("region " + kinesisClientLibConfiguration.getRegionName());
		System.out.println("key_id : "
				+ kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSAccessKeyId());
		System.out.println("access_key_id : "
				+ kinesisClientLibConfiguration.getKinesisCredentialsProvider().getCredentials().getAWSSecretKey());

		IRecordProcessorFactory recordProcessorFactory = new AmazonKinesisApplicationRecordProcessorFactory();
		Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

		System.out.printf("Running %s to process stream %s as worker %s...\n", SAMPLE_APPLICATION_NAME,
				SAMPLE_APPLICATION_STREAM_NAME, workerId);

		int exitCode = 0;
		try {
			worker.run();
		} catch (Throwable t) {
			System.err.println("Caught throwable while processing data.");
			t.printStackTrace();
			exitCode = 1;
		}
		System.exit(exitCode);
	}

}
