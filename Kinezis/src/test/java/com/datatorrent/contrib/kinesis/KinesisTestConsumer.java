/**
 * Copyright (C) 2015 DataTorrent, Inc. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */
package com.datatorrent.contrib.kinesis;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * A Kinesis consumer for testing, does not work, use it for idea or info purpose 
 */
public class KinesisTestConsumer implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(KinesisConsumer.class);
	protected String streamName;
	protected transient AmazonKinesisClient client = null;

	protected static final int BUFFER_SIZE_DEFAULT = 1024;
	public transient ArrayBlockingQueue<Record> holdingBuffer = new ArrayBlockingQueue<Record>(BUFFER_SIZE_DEFAULT);

	private volatile boolean isAlive = true;
	private int receiveCount = 0;

	private CountDownLatch doneLatch;

	protected static final int MAX_TRY_TIMES = 30;

	private void createClient() {
		AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();

		System.out.println("key_id : " + credentials.getCredentials().getAWSAccessKeyId());
		System.out.println("access_key_id : " + credentials.getCredentials().getAWSSecretKey());

		client = new AmazonKinesisClient(credentials);
	}

	public int getReceiveCount() {
		return receiveCount;
	}

	public void setReceiveCount(int receiveCount) {
		this.receiveCount = receiveCount;
	}

	public void setIsAlive(boolean isAlive) {
		this.isAlive = isAlive;
	}

	public KinesisTestConsumer(String topic) {
		createClient();
		this.streamName = topic;
	}

	public String getData(Record rc) {
		ByteBuffer buffer = rc.getData();
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return new String(bytes);
	}

	@Override
	public void run() {
		String iterator = prepareIterator();

		while (isAlive) {
			iterator = processNextIterator(iterator);

			// sleep at least 1 second to avoid exceeding the limit on getRecords frequency
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		logger.debug("DONE consuming");
	}

	public String prepareIterator() {
		DescribeStreamRequest describeRequest = new DescribeStreamRequest();
		describeRequest.setStreamName(streamName);
		client.describeStream(describeRequest);
		// brisi
		GetShardIteratorRequest request = null;
		GetShardIteratorResult shardIterator = null;
		for (int i = 0; i < MAX_TRY_TIMES; ++i) {

			request = new GetShardIteratorRequest();
			request.setStreamName("scg-attendance");
			request.setShardIteratorType(ShardIteratorType.LATEST);
			shardIterator = client.getShardIterator(request);

			try {
				Thread.sleep(1000);
			} catch (Exception e) {
			}
		}

		;
		GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
		iteratorRequest.setStreamName(streamName);
		iteratorRequest.setShardId(shardIterator.getShardIterator());

		iteratorRequest.setShardIteratorType("TRIM_HORIZON");
		GetShardIteratorResult iteratorResponse = client.getShardIterator(iteratorRequest);

		return iteratorResponse.getShardIterator();
	}

	public String processNextIterator(String iterator) {
		GetRecordsRequest getRequest = new GetRecordsRequest();
		getRequest.setLimit(1000);

		getRequest.setShardIterator(iterator);
		// call "get" operation and get everything in this shard range
		GetRecordsResult getResponse = client.getRecords(getRequest);

		iterator = getResponse.getNextShardIterator();

		List<Record> records = getResponse.getRecords();
		processResponseRecords(records);

		return iterator;
	}

	protected boolean shouldProcessRecord = true;

	protected void processResponseRecords(List<Record> records) {
		if (records == null || records.isEmpty())
			return;
		receiveCount += records.size();
		logger.debug("ReceiveCount= {}", receiveCount);

		for (Record record : records) {
			holdingBuffer.add(record);
			if (shouldProcessRecord) {
				processRecord(record);
			}

			if (doneLatch != null)
				doneLatch.countDown();
		}

	}

	protected void processRecord(Record record) {

	}

	public void close() {
		isAlive = false;
		holdingBuffer.clear();
	}

	public void setDoneLatch(CountDownLatch produceLatch) {
		this.doneLatch = produceLatch;
	}

	public static void main(String[] args) {
		KinesisTestConsumer consumer = new KinesisTestConsumer("scg-attendance");
		Thread t = new Thread(consumer);
		t.start();
	}

}