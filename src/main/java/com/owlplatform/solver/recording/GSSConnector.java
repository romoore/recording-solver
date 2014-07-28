/*
 * Owl Platform Recording Solver
 * Copyright (C) 2012 Robert Moore and the Owl Platform
 * Copyright (C) 2011 Rutgers University and Robert Moore
 * 
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *  
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *  
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.owlplatform.solver.recording;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.owlplatform.common.SampleMessage;

/**
 * A simple utility that combines a set of trace files into a single file.
 * 
 * @author Robert Moore
 * 
 */
public class GSSConnector {

	private static class GSSWriter extends Thread {

		private final LinkedBlockingQueue<SampleMessage> queue;

		public GSSWriter(LinkedBlockingQueue<SampleMessage> queue) {
			this.queue = queue;
		}

		/**
		 * Writes the provided sample to the output stream.
		 * 
		 * @param sample
		 *            the sensor sample to write to the output file.
		 * @return {@code true} if the sample is written successfully, else
		 *         {@code false}.
		 */
		protected boolean recordSample(SampleMessage sample) {
			if (this.outStream == null) {
				return true;
			}

			// 8-byte long timestamp, 4-byte length, and message
			ByteBuffer buff = ByteBuffer.allocate(sample
					.getLengthPrefixSensor() + 12);
			long offset = sample.getReceiverTimeStamp();
			buff.putLong(offset);
			buff.putInt(sample.getLengthPrefixSensor());
			buff.put(sample.getPhysicalLayer());
			buff.put(sample.getDeviceId());
			buff.put(sample.getReceiverId());
			buff.putLong(sample.getReceiverTimeStamp());
			buff.putFloat(sample.getRssi());
			if (sample.getSensedData() != null) {
				buff.put(sample.getSensedData());
			}

			buff.flip();

			try {
//				synchronized (this.outStream) {
					this.outStream.write(buff.array());
//					this.outStream.flush();
//				}
				
			} catch (IOException e) {
				log.error("Unable to write sample.", e);
				return false;
			}
			return true;
		}

		/**
		 * The output file where the combined input traces should be written.
		 */
		private String outputFile;

		private DataOutputStream outStream;

		/**
		 * Creates a new output file based on the base file name provided at the
		 * command-line and the current system time.
		 * 
		 * @return {@code true} if the new output file was created successfully,
		 *         else {@code false}.
		 */
		boolean prepareOutputFile() {

			File outFile = new File(this.outputFile);
			try {
				if (!outFile.createNewFile()) {
					log.error("File already exists: {}.", outFile);
					return false;
				}
			} catch (IOException e) {
				log.error("Unable to create file {}.", this.outputFile);
				return false;
			}

			if (!outFile.canWrite()) {
				log.error("Cannot write to {}.", outFile);
				return false;
			}

			try {

				if (this.outStream == null) {
					this.outStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
							outFile),10000000));
				}

				else {
					// Big error, shouldn't have 2 output files
					log.error("Trying to create a second output file.");
					return false;

				}
			} catch (FileNotFoundException e) {
				log.error("Could not find {} to create output stream.",
						this.outputFile);
				return false;
			}

			return true;
		}

		/**
		 * Shuts down this sensor after closing the aggregator connection and
		 * the input file stream.
		 */
		private void performShutdown() {
			log.info("Shutting down.");

			if (this.outStream != null) {
				try {
					this.outStream.flush();
					this.outStream.close();
				} catch (IOException e) {
					log.warn("Unable to close replay file.");
				}
			}
		}

		/**
		 * Timestamp of when replay of the file was started. Used to replay the
		 * samples at approximately the same rate that they were originally
		 * sent.
		 */
		private volatile long replayStart = Long.MIN_VALUE;
		
		

		/**
		 * Connects to the aggregator and begins sending samples as soon as the
		 * aggregator is ready to receive them.
		 */
		public void run() {
			if (this.outputFile == null) {
				log.warn("Missing one or more required parameters.");
				return;
			}

			if (!this.prepareOutputFile()) {
				log.error("Unable to prepare output file.");
				System.exit(1);
			}
			
			long numSamples = 0l;
			
			boolean keepRunning = true;
			while (keepRunning) {
				try {
					SampleMessage message = this.queue
							.poll(3, TimeUnit.SECONDS);
					if(message == null){
						keepRunning = false;
						break;
					}
					++numSamples;
					this.recordSample(message);
				} catch (InterruptedException ie) {
					keepRunning = false;
				}
			}

			log.info(String.format("%,d samples written",numSamples));
			this.performShutdown();
		}
	}

	private static class GSSReader extends Thread {
		private final LinkedBlockingQueue<SampleMessage> queue;

		public GSSReader(LinkedBlockingQueue<SampleMessage> queue) {
			this.queue = queue;
		}
		
		private long numSamples = 0l;

		/**
		 * The input stream for the replay file.
		 */
		private DataInputStream inputStream = null;

		/**
		 * The names of the files containing the recorded samples.
		 */
		private String[] inputFiles;

		/**
		 * Parses the next sample message from the replay file.
		 * 
		 * @return the next sample message in the replay file, or {@code null}
		 *         if there are no remaining samples.
		 */
		public SampleMessage getNextMessage() {
			SampleMessage message = new SampleMessage();

			if (this.inputStream != null) {
				try {
					// This code is largely copied from
					// org.grailrtls.sensor.prtocol.codecs.SampleDecoder
					int messageLength = this.inputStream.readInt();
					message.setPhysicalLayer(this.inputStream.readByte());
					--messageLength;
					byte[] deviceId = new byte[SampleMessage.DEVICE_ID_SIZE];
					byte[] receiverId = new byte[SampleMessage.DEVICE_ID_SIZE];
					int read = 0;
					while (read < deviceId.length) {
						int tmp = this.inputStream.read(deviceId, read,
								deviceId.length - read);
						if (tmp < 0) {
							log.error("Unable to read more bytes from input file (device id).");
							return null;
						}
						read += tmp;
					}
					message.setDeviceId(deviceId);
					messageLength -= deviceId.length;
					read = 0;
					while (read < receiverId.length) {
						int tmp = this.inputStream.read(receiverId, read,
								receiverId.length - read);
						if (tmp < 0) {
							log.error("Unable to read more bytes from input file (receiver id).");
							return null;
						}
						read += tmp;
					}
					message.setReceiverId(receiverId);
					messageLength -= receiverId.length;

					message.setReceiverTimeStamp(this.inputStream.readLong());
					messageLength -= 8;

					message.setRssi(this.inputStream.readFloat());
					messageLength -= 4;

					if (messageLength > 0) {
						byte[] data = new byte[messageLength];
						read = 0;
						while (read < data.length) {
							int tmp = this.inputStream.read(data, read,
									data.length - read);
							if (tmp < 0) {
								log.error("Unable to read more bytes from input file (data).");
								return null;
							}
							read += tmp;
						}
						message.setSensedData(data);
					}

				} catch (IOException ioe) {
					log.error(
							"Caught IO Exception while reading from input file: {}",
							ioe);
					return null;
				}

			}

			return message;
		}

		/**
		 * Prepares the input stream for reading from the replay file.
		 * 
		 * @param inFileName
		 *            the name of the replay file to open.
		 * @return {@code true} if the stream was created successfully, else
		 *         {@code false}.
		 */
		private boolean prepareInputFile(String inFileName) {
			File inFile = new File(inFileName);

			if (!inFile.exists()) {
				log.error("File does not exist: {}.", inFileName);
				return false;
			}

			if (!inFile.canRead()) {
				log.error("Cannot read from {}.", inFile);
				return false;
			}

			try {

				if (this.inputStream != null) {
					synchronized (this.inputStream) {
						this.inputStream.close();
					}
				}
				this.inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(
						inFile),10000000));
			} catch (FileNotFoundException e) {
				log.error("Could not find {} to create input stream.",
						inFileName);
				return false;
			} catch (IOException e) {
				log.error("Unable to open input file for reading.", e);
			}

			return true;
		}

		public void run() {

			long lastFileTime = 0l;

			for (int i = 0; i < this.inputFiles.length; ++i) {
				if (!this.prepareInputFile(this.inputFiles[i])) {
					log.error("Unable to prepare input file {}",
							this.inputFiles[i]);
					System.exit(1);
				}

				long fileStart = 0l;
				boolean keepFile = true;
				SampleMessage message = null;
				while (keepFile) {

					try {
						this.inputStream.readLong();
					} catch (IOException ioe) {
						log.info("Reached the end \"{}\" (TS).",
								this.inputFiles[i]);
						break;
					}

					message = getNextMessage();

					if (message == null) {
						log.info("Reached the end \"{}\" (Sample).",
								this.inputFiles[i]);

						break;
					}
					++numSamples;
					if (0l == fileStart) {
						fileStart = message.getReceiverTimeStamp();
					}

					message.setReceiverTimeStamp(message.getReceiverTimeStamp()
							- fileStart + lastFileTime);
					try {
						this.queue.put(message);
					} catch (InterruptedException e) {
						log.warn("Lost 1 sample while inserting into queue.", e);
					}

				}
				lastFileTime = message.getReceiverTimeStamp();

			}
			
			log.info(String.format("%,d samples read.",this.numSamples));
		}

		/**
		 * Shuts down this sensor after closing the aggregator connection and
		 * the input file stream.
		 */
		private void performShutdown() {
			log.info("Shutting down.");

			if (this.inputStream != null) {
				try {
					this.inputStream.close();
				} catch (IOException e) {
					log.warn("Unable to close replay file.");
				}
			}
		}
	}

	/**
	 * Logging facility for this class.
	 */
	public static final Logger log = LoggerFactory
			.getLogger(GSSConnector.class);

	/**
	 * Parses the command-line arguments and creates a new replay sensor.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 3) {
			printUsage();
			System.exit(1);
		}

		log.debug("Replay Sensor starting up.");
		GSSConnector sensor = new GSSConnector();

		sensor.setOutputFile(args[0]);
		String[] inFiles = new String[args.length - 1];
		System.arraycopy(args, 1, inFiles, 0, args.length - 1);
		sensor.setInputFiles(inFiles);

		log.info("Combining {} files.", inFiles.length);

		sensor.go();
	}

	/**
	 * Prints a message about available command-line parameters and their use.
	 */
	private static void printUsage() {
		StringBuffer sb = new StringBuffer();
		sb.append("One or more parameters is missing or invalid: <output file> <input file 1> <input file 2> ... ");
		sb.append("\n");
		System.err.println(sb.toString());

	}

	/**
	 * Flag to keep playing back the sample trace.
	 */
	protected boolean keepRunning = true;

	/**
	 * Sets the name of the output file.
	 * 
	 * @param outputFileName
	 *            the name of the output file.
	 */
	public void setOutputFile(String outputFileName) {
		this.writer.outputFile = outputFileName;
	}

	private final LinkedBlockingQueue<SampleMessage> queue = new LinkedBlockingQueue<SampleMessage>(
			100000
			);
	private final GSSReader reader = new GSSReader(this.queue);
	private final GSSWriter writer = new GSSWriter(this.queue);

	public void go() {
		final long start = System.currentTimeMillis();
		this.reader.start();
		this.writer.start();
		try {
			this.reader.join();
			this.writer.join();
		} catch (InterruptedException ie) {
			log.error("Unable to join worker threads.", ie);
		}
		log.info(String.format("Finished merging files in %,dms.",
				System.currentTimeMillis() - start));
	}

	public void setInputFiles(String[] inputFiles) {
		this.reader.inputFiles = inputFiles;
	}

}
