/*
 * Owl Platform No-Op Solver
 * Copyright (C) 2014 Robert Moore and the Owl Platform
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
 * A solver application that does nothing.
 * 
 * @author Robert Moore
 */
public class NoOpSolver implements ConnectionListener, SampleListener,
		Runnable {

	/**
	 * Logging facility for this class.
	 */
	public static final Logger log = LoggerFactory
			.getLogger(NoOpSolver.class);

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
			
		}

		log.debug("No-Op solver starting up.");
		NoOpSolver client = new NoOpSolver();
		client.setHost(args[0]);
		client.setPort(aggPort);
		
		client.setPhysicalLayer(physicalLayer);
		

		Thread solver = new Thread(client, "No-Op Solver");
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
				.append("<aggregator host> <aggregator port> [-p <INT>]");

		usageString.append("\n\t");

		// Physical layer
		usageString
				.append("-p : Followed by an integer value, identifies the physical layer to request of the aggregator.");

		usageString.append("\n\t");

		usageString.append("\n");

		System.err.println(usageString.toString());

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
	 * Aggregator interface.  Takes care of the connection/protocol business.
	 */
	protected SolverAggregatorInterface aggregatorInterface = new SolverAggregatorInterface();

	/**
	 * The physical layer identifier for the subscription request.
	 */
	private byte physicalLayer;

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
		// Does nothing (No-Op)
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

		log.debug("Attempting connection to {}:{}", this.host, Integer.valueOf(this.port));

		if (!this.aggregatorInterface.doConnectionSetup()) {
			log.warn("Aggregator connection failed.");
			return;
		}

	}

	/**
	 * Sets the host/IP address for the aggregator interface.
	 * @param host the host/IP address of the aggregator.
	 */
	public void setHost(String host) {
		this.host = host;
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
	 * Shuts down the solver after closing the output file.
	 */
	protected void performShutdown() {
		
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
		// Not used
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
