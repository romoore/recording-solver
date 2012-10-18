/*
 * Owl Platform Recording Solver
 * Copyright (C) 2012 Robert Moore and the Owl Platform
 * Copyright (C) 2012 Rutgers University and Robert Moore
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.owlplatform.common.SampleMessage;
import com.owlplatform.common.util.NumericUtils;

/**
 * A utility for converting Grail Sample Stream (.gss) files to CSV format.
 * 
 * @author Robert Moore II
 * 
 */
public class GSSToCSV {
	/**
	 * Logging facility for this class.
	 */
	private static final Logger log = org.slf4j.LoggerFactory
			.getLogger(GSSToCSV.class);
	
	private static final String THREE_DECIMAL_FLOAT = "%1.3f";

	/**
	 * String to describe the command line arguments for the utility.
	 */
	public static final String USAGE_STRING = "Usage: <Input GSS file> <Output CSV file> [-f]";

	/**
	 * File read buffer size.
	 */
	public static final int READ_FILE_BUFFER = 10485760;

	/**
	 * File write buffer size.
	 */
	public static final int WRITE_FILE_BUFFER = 10485760;

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println(USAGE_STRING);
			return;
		}
		String basename = getBaseName(args[0]);

		File gssFile = new File(args[0]);
		File csvFile = new File(args[1]);

    log.info("Input file: {}", args[0]);
    log.info("Output file: {}", args[1]);

		boolean forceOverwrite = false;

		if (args.length >= 3) {
			if ("-f".equalsIgnoreCase(args[2].trim())) {
				forceOverwrite = true;
			}
		}

		convertToCSV(gssFile, csvFile, forceOverwrite);

	}

	public static String getBaseName(final String fileName) {
		int lastDot = fileName.lastIndexOf(".gss");
		if (lastDot == -1) {
			return fileName;
		}

		return fileName.substring(0, lastDot);
	}

	public static boolean convertToCSV(final File gssFile, final File csvFile,
			final boolean forceOverwrite) {
		if (!gssFile.canRead()) {
			log.error("Unable to read \"" + gssFile.getName() + "\".");
			return false;
		}

		if (csvFile.exists() && !csvFile.canWrite()) {
			log.error("Unable to write \"" + csvFile.getName() + "\".");
			return false;
		}


		if (csvFile.exists()) {
			if (forceOverwrite) {
				log.info("Overwriting existing file \"" + csvFile.getName()
						+ "\".");
			} else {
				log.error("Not overwriting existing file \""
						+ csvFile.getName() + "\".");
				return false;
			}
		}

		InputStream gssIn = null;
		OutputStream csvOut = null;

		try {
			gssIn = new FileInputStream(gssFile);
			csvOut = new FileOutputStream(csvFile);
		} catch (FileNotFoundException e) {
			log.error("Unable to open stream or CSV file.", e);
			return false;
		}

		if (gssIn == null || csvOut == null) {
			log.error("Failure when opening input or output files.");
			return false;
		}
		long fileLength = gssFile.length();
		ArrayBlockingQueue<ReplaySample> sampleQueue = new ArrayBlockingQueue<ReplaySample>(1024, false);
		
		GSSReader reader = new GSSReader(gssIn, sampleQueue, fileLength);
		CSVWriter writer = new CSVWriter(csvOut, sampleQueue);
		reader.start();
		writer.start();
		
		try {
			reader.join();
			writer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		try {
			gssIn.close();
			csvOut.close();
		} catch (IOException ioe) {
			System.err.println("Could not close one or more streams.");
			return false;
		}

		return true;
	}

	private static class CSVWriter extends Thread {
		private static final Logger log = LoggerFactory
				.getLogger(CSVWriter.class);

		private final PrintWriter output;
		private final BlockingQueue<ReplaySample> queue;
		private boolean keepRunning = true;

		public CSVWriter(final OutputStream output,
				final BlockingQueue<ReplaySample> queue) {
			this.output = new PrintWriter(new BufferedOutputStream(output,
					WRITE_FILE_BUFFER));
			this.queue = queue;
		}

		public void shutDown() {
			this.keepRunning = false;
			this.interrupt();
		}

		public void run() {
			this.output
					.write("\"Offset (ms)\",\"Receiver Timestamp (ms)\",\"Physical Layer\",\"Device ID\",\"Receiver ID\",\"RSSI\",\"Received Data\"\n");

			ReplaySample message = null;
      long recordsWritten = 0l;
      long lastTime = System.currentTimeMillis();
      long now = 0l;
      long timeDiff = 0l;
      long lastRecords = 0l;
      long recordDiff = 0l;
      double recordRate = 0;
			while (this.keepRunning) {
				try {
					message = this.queue.poll(10,TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					log.error("Unable to retrieve sample from queue.");
					break;
				}
        if(message == null){
          log.info("No more samples to convert.");
          break;
        }
				this.output.write(Long.toString(message.getReplayTime()));
				this.output.write(',');
				this.output
						.write(Long.toString(message.getReceiverTimeStamp()));
				this.output.write(',');
				this.output
						.write(Integer.toString(message.getPhysicalLayer() & 0xFF));
				this.output.write(',');
				this.output.write(NumericUtils.toHexShortString(message.getDeviceId()));
				this.output.write(',');
				this.output.write(NumericUtils.toHexShortString(message.getReceiverId()));
				this.output.write(',');
				this.output.write(String.format(THREE_DECIMAL_FLOAT,message.getRssi()));
				this.output.write(',');
				if (message.getSensedData() != null) {
					this.output.write(NumericUtils.toHexString(message.getSensedData()));
				}
				this.output.write('\n');
        ++recordsWritten;

        if((recordsWritten & 0xFFFFF) == 0 && log.isDebugEnabled()){
          now = System.currentTimeMillis();
          timeDiff = now - lastTime;
          lastTime = now;

          recordDiff = recordsWritten - lastRecords;
          lastRecords = recordsWritten;

          recordRate = ((double)(recordDiff)/timeDiff)*1000;


            log.debug(String.format("Translated %,d records. (%,.1f R/s)", recordsWritten,recordRate));
        }
			}
      log.info("Wrote {} records.", recordsWritten);
			this.output.flush();
		}
	}

	private static class ReplaySample extends SampleMessage {
		private long replayTime;

		public long getReplayTime() {
			return replayTime;
		}

		public void setReplayTime(long replayTime) {
			this.replayTime = replayTime;
		}
	}

	private static class GSSReader extends Thread {

		private static final Logger log = LoggerFactory
				.getLogger(GSSReader.class);

		private final BlockingQueue<ReplaySample> queue;
		private final DataInputStream input;
		private final long totalStreamBytes;
		private boolean keepRunning = true;

		public GSSReader(final InputStream input,
				final BlockingQueue<ReplaySample> queue,
				final long totalStreamBytes) {
			this.queue = queue;
			this.input = new DataInputStream(new BufferedInputStream(input,
					READ_FILE_BUFFER));
			this.totalStreamBytes = totalStreamBytes;
		}

		public void shutdown() {
			this.keepRunning = false;
			this.interrupt();
		}

		public void run() {
			ReplaySample message = new ReplaySample();
			long recordsRead = 0;
			long bytesRead = 0l;
			long lastByte = 0l;
			long lastTime = System.currentTimeMillis();
			long diffTime = 0l, now = 0l;
			
			double byteRate = 0;
			while (this.keepRunning && (bytesRead < this.totalStreamBytes)) {
			  message = new ReplaySample();
				int amtRead = getNextMessage(this.input, message);
				if (amtRead < 0) {
					log.error("Unable to read a sample from the input file.");
					break;
				}
				bytesRead += amtRead;
				try {
					this.queue.put(message);
				} catch (InterruptedException e) {
					log.error("Unable to insert {}", message);
					continue;
				}
        if(log.isDebugEnabled()){
				 ++recordsRead;
			      if((recordsRead & 0xFFFFF) == 0){ 
			    	now = System.currentTimeMillis();
			        diffTime = now - lastTime;
			        lastTime = now;
			        
			        byteRate = (double)(bytesRead - lastByte)/diffTime;
			        lastByte = bytesRead;
			        log.debug(String.format("%.4f%% complete. (%.2f KB/s)", ((double)(bytesRead) / this.totalStreamBytes)*100, byteRate));
			      }
				}
			}
      log.info("Completed reading all {} samples.", recordsRead);
		}

		/**
		 * Parses the next sample message from the replay file.
		 * 
		 * @return the number of bytes that were read from the InputStream.
		 */
		public static int getNextMessage(final DataInputStream inputStream,
				final ReplaySample message) {
			int bytesRead = 0;
			if (inputStream != null) {
				try {
					// This code is largely copied from
					// org.grailrtls.sensor.prtocol.codecs.SampleDecoder
					long offset = inputStream.readLong();
					message.setReplayTime(offset);

					int messageLength = inputStream.readInt();
					bytesRead = messageLength + 12; // Message length + length
													// itself + offset
					message.setPhysicalLayer(inputStream.readByte());
					--messageLength;
					byte[] deviceId = new byte[SampleMessage.DEVICE_ID_SIZE];
					byte[] receiverId = new byte[SampleMessage.DEVICE_ID_SIZE];
					inputStream.readFully(deviceId);
					message.setDeviceId(deviceId);
					messageLength -= deviceId.length;

					inputStream.readFully(receiverId);
					message.setReceiverId(receiverId);
					messageLength -= receiverId.length;

					message.setReceiverTimeStamp(inputStream.readLong());
					messageLength -= 8;

					message.setRssi(inputStream.readFloat());
					messageLength -= 4;

					if (messageLength > 0) {
						byte[] data = new byte[messageLength];
						inputStream.readFully(data);
						message.setSensedData(data);
					} 

				} catch (IOException ioe) {
					log.error(
							"Caught IO Exception while reading from input file: {}",
							ioe);
					return -1;
				}

			}

			return bytesRead;
		}
	}

}
