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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.owlplatform.common.SampleMessage;
import com.owlplatform.solver.SolverAggregatorInterface;
import com.owlplatform.solver.listeners.ConnectionListener;
import com.owlplatform.solver.listeners.SampleListener;
import com.owlplatform.solver.protocol.messages.SubscriptionMessage;
import com.owlplatform.solver.rules.SubscriptionRequestRule;

/**
 * A solver application that records a stream of sensor sample messages into a
 * file or files. Useful for recording raw sensor data for the purposes of
 * future replay/testing of algorithms. Use in conjunction with the
 * {@link ReplaySensor}.
 * 
 * @author Robert Moore
 */
public class RecordingSolver implements ConnectionListener, SampleListener,
		Runnable {

	/**
	 * Logging facility for this class.
	 */
	public static final Logger log = LoggerFactory
			.getLogger(RecordingSolver.class);

	/**
	 * Parses the command-line arguments and creates a new recording solver.
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			printUsage();
			System.exit(1);
		}

		int aggPort = -1;
		try {
			aggPort = Integer.parseInt(args[1]);
		} catch (NumberFormatException nfe) {
			System.err.println("Invalid aggregator port.");
			printUsage();
			System.exit(1);
		}

		// Used if we only want to record a single physical layer
		byte physicalLayer = 0;
		// The output file name prefix
		String baseFileName = null;
		// How often to rotate the output file
		int outFileRotateSeconds = -1;
		
		for (int argsIndex = 2; argsIndex < args.length; ++argsIndex) {
			// Physical layer filtering
			if ("-p".equals(args[argsIndex])) {
				// Make sure there is one more parameter available.
				if (args.length > argsIndex++) {
					try {
						physicalLayer = (byte) Integer
								.parseInt(args[argsIndex]);
					} catch (Exception e) {
						System.err
								.println("Unable to parse physical layer identifier, defaulting to all types.");
						e.printStackTrace(System.err);
					}
				} else {
					System.err.println("Missing physical layer identifier.");
					printUsage();
					System.exit(1);
				}
			}
			// Output file rotation interval
			else if ("-R".equals(args[argsIndex])) {
				// Make sure there is one more parameter available.
				if (args.length > argsIndex++) {
					try {
						outFileRotateSeconds = Integer
								.parseInt(args[argsIndex]);
					} catch (Exception e) {
						System.err
								.println("Unable to output file rotation interval, not rotating");
						e.printStackTrace(System.err);
					}
				} else {
					System.err.println("Missing log rotation period.");
					printUsage();
					System.exit(1);
				}
			}
			// Output file name base
			else if ("-o".equals(args[argsIndex])) {
				// Make sure there is one more parameter available.
				if (args.length > argsIndex++) {
					baseFileName = args[argsIndex];
				} else {
					System.err.println("Missing output file name.");
					printUsage();
					System.exit(1);
				}
			} else {
				System.err.println("Skipping unrecognized argument: "
						+ args[argsIndex]);
			}
		}

		log.debug("Recording solver starting up.");
		RecordingSolver client = new RecordingSolver();
		client.setHost(args[0]);
		client.setPort(aggPort);
		if (baseFileName != null) {
			client.setBaseFileName(baseFileName);
		}
		client.setPhysicalLayer(physicalLayer);
		if (outFileRotateSeconds > 0) {
			client.setUseTimeBasedFileName(true);
			client.setOutFileRotationInterval(outFileRotateSeconds * 1000);
		}

		Thread solver = new Thread(client, "Recording Solver");
		solver.start();
		try {
			solver.join();
		} catch (InterruptedException e) {
			// Ignored
		}
	}

	/**
	 * Prints details about command-line arguments and what they do.
	 */
	private static void printUsage() {
		StringBuffer usageString = new StringBuffer();

		usageString.append("One or more parameters is missing or invalid: ");
		// Parameters list
		usageString
				.append("<aggregator host> <aggregator port> [-o <filename>] [-p <INT>] [-R <INT>]");

		usageString.append("\n\t");

		// Output file name
		usageString
				.append("-o : Followed by a string, prepended to the output file name, which is the date/time by default.");

		usageString.append("\n\t");

		// Physical layer
		usageString
				.append("-p : Followed by an integer value, identifies the physical layer to request of the aggregator.");

		usageString.append("\n\t");

		// Rotating output file
		usageString
				.append("-R : Followed by an integer value, enables output file rotation and specifies the rotation period in seconds.");

		usageString.append("\n");

		System.err.println(usageString.toString());

	}

	/**
	 * Base output file name for recording. If {@code useTimeBasedFileName} is
	 * {@code true}, then this value is prefixed to the filename timestamp.
	 */
	// The base name of the output file
	private String baseFileName = null;

	/**
	 * Format for output files.  All output files have a timestamp recorded in their filename.
	 * An example format is "2011.06.30-14:55:43.242".
	 */
	private DateFormat format = new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss.SSS");

	/**
	 * The host name/IP address of the aggregator.
	 */
	private String host;

	/**
	 * The port number of the aggregator.
	 */
	private int port;

	/**
	 * Aggregator interface.  Takes care of the connection/protocol business.
	 */
	private SolverAggregatorInterface aggregatorInterface = new SolverAggregatorInterface();

	/**
	 * How frequently to rotate the output file in milliseconds.
	 */
	private long outFileRotationInterval = -1l;

	/**
	 * Timer to rotate the output file.
	 */
	private Timer outFileRotationTimer = new Timer();

	/**
	 * The output stream for writing samples.
	 */
	private DataOutputStream outStream;

	/**
	 * The physical layer identifier for the subscription request.
	 */
	private byte physicalLayer;

	/**
	 * When the recording was started.  Needed to calculate relative timestamp offsets in the output file.
	 */
	private long recordingStart = Long.MIN_VALUE;

	/**
	 * Indicates whether to use a rotating, time-based output file naming
	 * scheme.
	 */
	// For rotating the output file
	private boolean useTimeBasedFileName = true;

	/**
	 * Creates a subscription request for the aggregator interface.
	 * @return a single subscription request rule that either specifies the physical layer identified
	 * in the command-line arguments, or for all physical layer types.
	 */
	protected SubscriptionRequestRule[] generateRequestRules() {

		SubscriptionRequestRule[] rules = new SubscriptionRequestRule[1];
		rules[0] = SubscriptionRequestRule.generateGenericRule();
		rules[0].setPhysicalLayer(this.physicalLayer);
		rules[0].setUpdateInterval(0l);

		return rules;
	}

	/**
	 * Called when a sample message is received from the aggregator.  Prepares a new sensor-layer sample
	 * message and records it to the output file.  If recording fails for any reason, then the
	 * solver will exit.
	 */
	public void sampleReceived(SolverAggregatorInterface aggregator,
			SampleMessage sampleMessage) {
		if (!this.recordSample(sampleMessage)) {
			this.performShutdown();
		}
	}

	/**
	 * Creates a new output file based on the base file name provided at the command-line and the
	 * current system time.
	 * @return {@code true} if the new output file was created successfully, else {@code false}.
	 */
	boolean prepareOutputFile() {
		StringBuffer outFileName = new StringBuffer();
		if (this.baseFileName != null) {
			outFileName.append(this.baseFileName);
		}

		if (this.useTimeBasedFileName) {
			if (this.baseFileName != null) {
				outFileName.append('_');
			}
			long now = System.currentTimeMillis();
			outFileName.append(this.format.format(new Date(now)));
			// GRAIL Solver Samples?
		}

		outFileName.append(".gss");
		
		File outFile = new File(outFileName.toString());
		try {
			if (!outFile.createNewFile()) {
				log.error("File already exists: {}.", outFile);
				return false;
			}
		} catch (IOException e) {
			log.error("Unable to create file {}.", outFileName);
			return false;
		}

		if (!outFile.canWrite()) {
			log.error("Cannot write to {}.", outFile);
			return false;
		}

		try {

			if (this.outStream == null) {
				this.outStream = new DataOutputStream(new FileOutputStream(
						outFile));
			}
			/*
			 * We've already created an output stream and are now rotating the
			 * file. Make sure we don't switch streams while another thread is
			 * writing samples. Also be sure to restart the recording start time
			 * (for replay).
			 */
			else {
				synchronized (this.outStream) {
					OutputStream oldFile = this.outStream;
					this.outStream = new DataOutputStream(new FileOutputStream(
							outFile));
					this.recordingStart = System.currentTimeMillis();
					try {
						oldFile.close();
					} catch (IOException e) {
						log.warn("Unable to close old output file: {}",e);
						e.printStackTrace();
					}
				}
				
			}
		} catch (FileNotFoundException e) {
			log
					.error("Could not find {} to create output stream.",
							outFileName);
			return false;
		}

		return true;
	}

	/**
	 * Writes the provided sample to the output stream.
	 * @param sample the sensor sample to write to the output file.
	 * @return {@code true} if the sample is written successfully, else {@code false}.
	 */
	private boolean recordSample(SampleMessage sample) {
		if (this.outStream == null) {
			return true;
		}

		// 8-byte long timestamp, 4-byte length, and message
		ByteBuffer buff = ByteBuffer.allocate(sample.getLengthPrefixSensor() + 12);
		long offset = System.currentTimeMillis() - this.recordingStart;
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
			synchronized (this.outStream) {
				this.outStream.write(buff.array());
				this.outStream.flush();
			}
			log.debug("Wrote {}.", sample);
		} catch (IOException e) {
			log.error("Unable to write sample.",e);
			return false;
		}
		return true;
	}

	/**
	 * Initiates the connection to the aggregator, creates the output file, and starts recording.
	 */
	public void run() {

		if (this.host == null || this.port < 0) {
			log.warn("Missing one or more required parameters.");
			return;
		}
		this.aggregatorInterface.setHost(this.host);
		this.aggregatorInterface.setPort(this.port);
		this.aggregatorInterface.setRules(this.generateRequestRules());
		this.aggregatorInterface.addConnectionListener(this);
		this.aggregatorInterface.addSampleListener(this);
		this.aggregatorInterface.setStayConnected(true);

		if (this.outFileRotationInterval > 0l) {

			this.outFileRotationTimer.schedule(new TimerTask() {

				@Override
				public void run() {
					if (!prepareOutputFile()) {
						// FIXME: This is a dirty exit!
						System.exit(1);
					}

				}

			}, 1l, this.outFileRotationInterval);
		} else {
			if (!this.prepareOutputFile()) {
				this.performShutdown();
			}
		}

		log.debug("Attempting connection to {}:{}", this.host, Integer.valueOf(this.port));

		if (!this.aggregatorInterface.doConnectionSetup()) {
			log.warn("Aggregator connection failed.");
			return;
		}

	}

	/**
	 * Sets the base file name for the output file.  This is the string that begins the output file
	 * name, before the timestamp.
	 * @param baseFileName the base name for the output file.
	 */
	public void setBaseFileName(String baseFileName) {
		this.baseFileName = baseFileName;
	}

	/**
	 * Sets the host/IP address for the aggregator interface.
	 * @param host the host/IP address of the aggregator.
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Sets how frequently the output file should be rotated.
	 * @param outFileRotationInterval the output file rotation interval, in seconds.
	 */
	public void setOutFileRotationInterval(long outFileRotationInterval) {
		this.outFileRotationInterval = outFileRotationInterval;
	}

	/**
	 * Sets the physical layer for the aggregator request.
	 * @param physicalLayer the physical layer identifier for the aggregator request.
	 */
	public void setPhysicalLayer(byte physicalLayer) {
		this.physicalLayer = physicalLayer;
	}

	/**
	 * Sets the port number for the aggregator interface.
	 * @param port the port number of the aggregator.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Sets the flag to use a timestamp-based output file name.  This is the default behavior.
	 * @param useTimeBasedFileName {@code true} if the output filename should contain a timestamp.
	 */
	public void setUseTimeBasedFileName(boolean useTimeBasedFileName) {
		this.useTimeBasedFileName = useTimeBasedFileName;
	}

	/**
	 * Shuts down the solver after closing the output file.
	 */
	private void performShutdown() {
		if (this.outStream != null) {
			synchronized (this.outStream) {
				try {
					this.outStream.close();
				} catch (IOException ioe) {
					log.warn(
							"IOException caught while closing output file: {}",
							ioe);
				}
			}
		}
		System.exit(0);
	}

	/**
	 * Called when the aggregator interface has closed.  Shuts down the solver.
	 */
	@Override
	public void connectionEnded(SolverAggregatorInterface aggregator) {
		this.performShutdown();
	}

	/**
	 * Called once the connection to the aggregator has been established.
	 */
	@Override
	public void connectionEstablished(SolverAggregatorInterface aggregator) {
		// Set the starting timestamp if this is the first time we've connected to the aggregator
		if (this.recordingStart < 0) {
			this.recordingStart = System.currentTimeMillis();
			log
					.info("Starting recording at {}.", new Date(
							this.recordingStart));
		}

	}

	/**
	 *  Not used by this solver. 
	 */
	@Override
	public void connectionInterrupted(SolverAggregatorInterface aggregator) {
		// Not used
	}

	@Override
	public void subscriptionReceived(SolverAggregatorInterface aggregator,
			SubscriptionMessage response) {
		// TODO Auto-generated method stub
		
	}
}
