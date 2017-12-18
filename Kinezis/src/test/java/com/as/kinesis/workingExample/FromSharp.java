package com.as.kinesis.workingExample;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

public class FromSharp {

	private static int count = 0;
	private static int backoff = 0;
	// define the checkpoint file
	private static final String SEQUNECE_CHECKPOINT_FILE = "C:/Users/User/.aws/sequenceNumber.txt";

	public static void main(String[] args) {
		System.out.println("Read Records: Kinesis");
		readKinesisRecords();

		System.out.println("Press any key to continue...");
	}

	/**
	 * Read records from the Kinesis stream
	 */
	public static void readKinesisRecords() {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential profile
		 * by reading from the credentials file located at (~/.aws/credentials).
		 */
		AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();
		AmazonKinesis client = new AmazonKinesisClient(credentials);
		// region must be set
		Region region = RegionUtils.getRegion("ap-southeast-2");
		client.setRegion(region);

		// Init request with stream name
		DescribeStreamRequest describeRequest = new DescribeStreamRequest();
		describeRequest.setStreamName("!!!");

		// Get list of shards on the stream
		DescribeStreamResult describeResponse = client.describeStream(describeRequest);
		List<Shard> shards = describeResponse.getStreamDescription().getShards();

		// assume single shard stream - multiple shards can be iterated with above list
		String primaryShard = shards.get(0).getShardId();

		// Init shard iterator with stream name and shard id
		GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
		iteratorRequest.setStreamName(describeRequest.getStreamName());
		iteratorRequest.setShardId(primaryShard);

		// if we have a sequence checkpoint, assign appropriate iterator type
		String sequenceNumber = getSequenceCheckpoint();
		if (sequenceNumber == null) {
			iteratorRequest.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
		} else {
			iteratorRequest.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
			iteratorRequest.setStartingSequenceNumber(sequenceNumber);

		}
		GetShardIteratorResult iteratorResponse = client.getShardIterator(iteratorRequest);
		StringWrapper iteratorId = new StringWrapper(iteratorResponse.getShardIterator());

		System.out.println("---------------------------------");
		int readCnt = 0;
		while (iteratorId.getString() != null) {

			//System.out.println("iterator : " + iteratorId.getString());
			System.out.print(".");
			if (count++ > 40) {
				count = 0;
				System.out.println();
			}

			// Read the next batch of records
			if (!getKinesisShardRecords(client, iteratorId) || readCnt > backoff) {
				try {
					// We've caught up, or reached our back-off limit
					// wait a sec before attempting another read so we don't exceed throughput.
					Thread.sleep(1);
					readCnt = 0;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
			readCnt++;
		}
		System.out.println("---------------------------------");
		System.out.println("All records read.");

	}

	// Read records from Kinesis Shard
	private static boolean getKinesisShardRecords(AmazonKinesis client, StringWrapper iteratorId) {

		// create reqest
		GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
		getRecordsRequest.setLimit(1000);
		getRecordsRequest.setShardIterator(iteratorId.getString());

		// call "get" operation and get everything in this shard range
		GetRecordsResult getRecordsResult = client.getRecords(getRecordsRequest);
		// get reference to next iterator for this shard
		String nextShardIterator = getRecordsResult.getNextShardIterator();
//		System.out.println(nextShardIterator);
		iteratorId.setString(nextShardIterator);
		

		// print out each record's data value
		for (Record record : getRecordsResult.getRecords()) {
			System.out.print("data: ");
			// pull out (JSON) data in this record
			String data = getDataString(record.getData().array());
			System.out.println(data);

			String sequenceNumber = record.getSequenceNumber();
			System.out.println("sequence Number : " + sequenceNumber);
			// save checkpoint after processing
			saveSequencePoint(sequenceNumber);
		}
		
		//return TRUE if we are not up-to-date yet, FALSE if all records have been read
		return getRecordsResult.getMillisBehindLatest() > 1;
	}
	
	// Decode the record data and unzip if data is GZIP compressed
	private static String getDataString(byte[] data) {
		String s = null;
		byte[] buffer = new byte[4096];

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		System.out.println("DATA!!!!!!!!");
		if (data != null && data.length >= 2 && checkGzipFormat(data)) {
			try {
				GZIPInputStream gzis = new GZIPInputStream(new ByteArrayInputStream(data), data.length);

				while (gzis.read(buffer) != -1) {
					outputStream.write(buffer);
				}
				s = new String(outputStream.toByteArray());
			} catch (IOException e) {
				e.printStackTrace();
				// try {
				// s = new String(data, "ANSI");
				// } catch (UnsupportedEncodingException e1) {
				// e.printStackTrace();
				// }
			}
		}
		return s;
	}

	private static boolean checkGzipFormat(byte[] buf) {
		ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(buf);
		PushbackInputStream pb = new PushbackInputStream(arrayInputStream, 2);
		byte[] signature = new byte[2];
		int len = 0;
		try {
			len = pb.read(signature);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			pb.unread(signature, 0, len);
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b) {
			return true;
		}
		return false;
	}
	
	//This class help pass String by reference and over comes immutability 
	private static class StringWrapper {
		private String string;

		public StringWrapper(String string) {
			this.setString(string);
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

	}
	
	private static String getSequenceCheckpoint() {
		String sequenceNumber = null;

		if (Files.exists(Paths.get(SEQUNECE_CHECKPOINT_FILE))) {
			try (Stream<String> stream = Files.lines(Paths.get(SEQUNECE_CHECKPOINT_FILE))) {

				Optional<String> first = stream.findFirst();
				sequenceNumber = first.isPresent() ? first.get() : null;
				System.out.println("checkpoint found : " + sequenceNumber);

			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		return sequenceNumber;
	}

	private static void saveSequencePoint(String checkpoint) {
		Path path = Paths.get(SEQUNECE_CHECKPOINT_FILE);
		try {
			Files.write(path, checkpoint.getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
