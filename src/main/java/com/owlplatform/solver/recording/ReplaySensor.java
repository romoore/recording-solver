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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.owlplatform.common.SampleMessage;
import com.owlplatform.sensor.SensorAggregatorInterface;
import com.owlplatform.sensor.listeners.ConnectionListener;


/**
 * A simple utility that replays sample traces recorded by the
 * {@link RecordingSolver}. Useful for testing new solver algorithms or when
 * live data is unavailable.
 * 
 * @author Robert Moore
 * 
 */
public class ReplaySensor implements ConnectionListener, Runnable {

	/**
	 * Logging facility for this class.
	 */
	public static final Logger log = LoggerFactory
			.getLogger(ReplaySensor.class);

	/**
	 * The input stream for the replay file.
	 */
	private DataInputStream inputStream = null;

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

		int aggPort = -1;
		try {
			aggPort = Integer.parseInt(args[1]);
		} catch (NumberFormatException nfe) {
			System.err.println("Invalid aggregator port.");
			printUsage();
			System.exit(1);
		}

		log.debug("Replay Sensor starting up.");
		ReplaySensor sensor = new ReplaySensor();
		sensor.setHost(args[0]);
		sensor.setPort(aggPort);
		sensor.setReplayFileName(args[2]);

		float playbackSpeed = 1f;
		boolean updateTimestamps = false;

		for (int i = 3; i < args.length; ++i) {
			// Playback speed
			if ("-X".equals(args[i])) {
				// Make sure there is one more parameter available.
				if (args.length > i++) {
					try {
						playbackSpeed = Float.parseFloat(args[i]);
						log.info("Playing back at {}x", playbackSpeed);
					} catch (Exception e) {
						System.err
								.println("Unable to parse playback rate, defaulting to 1.0x.");
						e.printStackTrace(System.err);
					}
				} else {
					System.err.println("Missing playback speed value.");
					printUsage();
					System.exit(1);
				}
			} else if ("-t".equals(args[i])) {
				updateTimestamps = true;
			}

			else {
				System.err.println("Unrecognized option " + args[i]);
			}
		}

		sensor.setPlaybackSpeed(playbackSpeed);
		sensor.setUpdateTimestamps(updateTimestamps);

		new Thread(sensor, String.format("%fx Replay Sensor", playbackSpeed))
				.start();
	}

	/**
	 * Prints a message about available command-line parameters and their use.
	 */
	private static void printUsage() {
		StringBuffer sb = new StringBuffer();
		sb
				.append("One or more parameters is missing or invalid: <aggregator host> <aggregator port> <input file> -X <FLOAT> -t");
		sb.append("\n\t");
		sb
				.append("-X : Followed by a float, indicates the playback speed desired.");
		sb.append("\n\t");
		sb
				.append("-t : The Replay Sensor will update timestamps in the file to the current system time.");
		sb.append("\n");
		System.err.println(sb.toString());

	}

	/**
	 * The host name/IP address of the aggregator.
	 */
	private String host;

	/**
	 * The port number of the aggregator.
	 */
	private int port;

	/**
	 * Flag to keep playing back the sample trace.
	 */
	protected boolean keepRunning = true;

	/**
	 * Flag that indicates the aggregator is ready to receive samples from this
	 * sensor.
	 */
	protected boolean canSendSamples = false;

	/**
	 * The name of the file containing the recorded samples.
	 */
	private String replayFileName;

	/**
	 * How quickly or slowly to playback the samples. File timestamps are
	 * interepreted after being multiplied by this factor.
	 */
	private float playbackSpeed = 1.0f;
	
	/**
	 * Whether the Replay Sensor should update sample timestamps to the current time.
	 */
	private boolean updateTimestamps = false;
	
	/**
	 * Sets the name of the replay file.
	 * 
	 * @param replayFileName
	 *            the name of the replay file.
	 */
	public void setReplayFileName(String replayFileName) {
		this.replayFileName = replayFileName;
	}

	/**
	 * The aggregator interface. Does all the protocol work so we don't have to.
	 */
	private SensorAggregatorInterface aggregatorInterface = new SensorAggregatorInterface();

	/**
	 * Prepares the input stream for reading from the replay file.
	 * 
	 * @param inFileName
	 *            the name of the replay file to open.
	 * @return {@code true} if the stream was created successfully, else {@code
	 *         false}.
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
			this.inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(inFile),1000000));
		} catch (FileNotFoundException e) {
			log.error("Could not find {} to create input stream.", inFileName);
			return false;
		}

		return true;
	}

	/**
	 * Sends the provided sample message to the aggregator if everything is
	 * prepared correctly.
	 * 
	 * @param sampleMessage
	 *            the sample message to send.
	 */
	public void sendSampleMessage(SampleMessage sampleMessage) {
		if (!this.canSendSamples) {
			log.warn("Connection not ready, discarding {}.", sampleMessage);
			return;
		}

		if (sampleMessage == null) {
			log.error("Unable to retrieve next sample. Exiting.");
			this.performShutdown();
		}

		this.aggregatorInterface.sendSample(sampleMessage);
		log.debug("Sent {}.", sampleMessage);

	}

	/**
	 * Shuts down this sensor after closing the aggregator connection and the
	 * input file stream.
	 */
	private void performShutdown() {
		log.info("Shutting down.");
		this.aggregatorInterface.doConnectionTearDown();

		if (this.inputStream != null) {
			try {
				this.inputStream.close();
			} catch (IOException e) {
				log.warn("Unable to close replay file.");
			}
		}
		System.exit(0);
	}

	/**
	 * Parses the next sample message from the replay file.
	 * 
	 * @return the next sample message in the replay file, or {@code null} if
	 *         there are no remaining samples.
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
						log
								.error("Unable to read more bytes from input file (device id).");
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
						log
								.error("Unable to read more bytes from input file (receiver id).");
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
						int tmp = this.inputStream.read(data, read, data.length
								- read);
						if (tmp < 0) {
							log
									.error("Unable to read more bytes from input file (data).");
							return null;
						}
						read += tmp;
					}
					message.setSensedData(data);
				}

			} catch (IOException ioe) {
				log
						.error(
								"Caught IO Exception while reading from input file: {}",
								ioe);
				return null;
			}

		}

		return message;
	}

	/**
	 * Timestamp of when replay of the file was started. Used to replay the
	 * samples at approximately the same rate that they were originally sent.
	 */
	private volatile long replayStart = Long.MIN_VALUE;

	/**
	 * Connects to the aggregator and begins sending samples as soon as the
	 * aggregator is ready to receive them.
	 */
	public void run() {
		if (this.host == null || this.port < 0 || this.replayFileName == null) {
			log.warn("Missing one or more required parameters.");
			return;
		}

		if (!this.prepareInputFile(this.replayFileName)) {
			System.exit(1);
		}

		this.aggregatorInterface.setHost(this.host);
		this.aggregatorInterface.setPort(this.port);
		this.aggregatorInterface.addConnectionListener(this);
		this.aggregatorInterface.setStayConnected(true);

		log.debug("Attempting connection to {}:{}", this.host, Integer
				.valueOf(this.port));

		if (!this.aggregatorInterface.doConnectionSetup()) {
			log.warn("Aggregator connection failed.");
			return;
		}

		while (this.keepRunning) {

			if (!this.canSendSamples) {
				try {
					log.debug("Not ready to send samples.  Waiting 100ms.");
					Thread.sleep(100);
				} catch (InterruptedException ie) {
					// Ignored
				}
				continue;
			}

			long now = System.currentTimeMillis();
			long realTimeIndex = now - this.replayStart;
			long nextSampleIndex = 0l;

			try {
				nextSampleIndex = this.inputStream.readLong();
			} catch (IOException ioe) {
				log.error("Unable to retrieve next timestamp.");
				this.performShutdown();
			}

			long simTimeIndex = (long) ((float) realTimeIndex * this.playbackSpeed);

			// If we're more than 10ms ahead, then perform a sleep.
			if ((simTimeIndex + 10) < nextSampleIndex) {
				long sleepDelay = nextSampleIndex - simTimeIndex;
				sleepDelay /= this.playbackSpeed;
				try {
					log.debug("Sleeping {}ms.", sleepDelay);
					Thread.sleep(sleepDelay);
				} catch (InterruptedException ie) {
					log.info("Unable to sleep full {}ms.", sleepDelay);
				}
			}

			SampleMessage message = getNextMessage();
			if(message == null)
			{
				log.warn("Reached the end of the replay file.");
				this.performShutdown();
			}
			if(this.updateTimestamps)
			{
				message.setReceiverTimeStamp(System.currentTimeMillis());
			}

			this.sendSampleMessage(message);
		}

		this.performShutdown();
	}

	/**
	 * Sets the host name/IP address for the aggregator interface.
	 * 
	 * @param host
	 *            the host name/IP address of the aggregator.
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Sets the port number for the aggregator interface.
	 * 
	 * @param port
	 *            the port number of the aggregator.
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * Called when the aggregator connection has closed, causes the sensor to
	 * exit.
	 */
	@Override
	public void connectionEnded(SensorAggregatorInterface aggregator) {
		log.info("Lost connection to {}.", aggregator);
		this.keepRunning = false;
	}

	/**
	 * Logs an informative message about connecting to the aggregator.
	 */
	@Override
	public void connectionEstablished(SensorAggregatorInterface aggregator) {
		log.info("Connected to {}.", aggregator);
	}

	/**
	 * Stops the sensor from sending samples to the aggregator.
	 */
	@Override
	public void connectionInterrupted(SensorAggregatorInterface aggregator) {
		log.info("Temporarily lost connection to {}.", aggregator);
		this.canSendSamples = false;

	}

	/**
	 * Once called, the sensor will start sending sample messages according to
	 * the replay file. If this is the first time connecting to the aggregator,
	 * sets the playback start timestamp (for calculating offsets in the file).
	 */
	@Override
	public void readyForSamples(SensorAggregatorInterface aggregator) {
		log.info("Ready to send samples to {}.", aggregator);
		if (this.replayStart < 0) {
			this.replayStart = System.currentTimeMillis();
		}
		this.canSendSamples = true;

	}

	public float getPlaybackSpeed() {
		return playbackSpeed;
	}

	public void setPlaybackSpeed(float playbackSpeed) {
		this.playbackSpeed = playbackSpeed;
	}

	public boolean isUpdateTimestamps() {
		return updateTimestamps;
	}

	public void setUpdateTimestamps(boolean updateTimestamps) {
		this.updateTimestamps = updateTimestamps;
	}
}
