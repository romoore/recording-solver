/*
 * Owl Platform
 * Copyright (C) 2012 Robert Moore and the Owl Platform
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.owlplatform.common.SampleMessage;
import com.owlplatform.common.util.HashableByteArray;
import com.owlplatform.common.util.NumericUtils;

/**
 * @author Robert Moore
 */
public class TempRecordingSolver extends RecordingSolver {

  private final HashMap<HashableByteArray, Boolean> idMap = new HashMap<HashableByteArray, Boolean>();

  private PrintWriter output;

  DateFormat dateFormat = SimpleDateFormat.getDateTimeInstance(
      SimpleDateFormat.SHORT, SimpleDateFormat.FULL);

  /**
   * Parses the command-line arguments and creates a new recording solver.
   * 
   * @param args
   */
  public static void main(String[] args) {

    if (args.length < 2) {
      printUsage();
      System.exit(1);
    }
    LinkedList<Integer> ids = new LinkedList<Integer>();

    // The output file name prefix
    String baseFileName = null;
    int aggregatorPort = 7008;

    for (int argsIndex = 1; argsIndex < args.length; ++argsIndex) {
      // Physical layer filtering
      if ("-p".equals(args[argsIndex])) {
        // Make sure there is one more parameter available.
        if (args.length > argsIndex++) {
          try {
            aggregatorPort = (byte) Integer.parseInt(args[argsIndex]);
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
      } else if ("-t".equals(args[argsIndex])) {
        // Ensure one more parameter available
        if (args.length > argsIndex++) {
          // Split next param on commas
          String idList = args[argsIndex];
          String[] idArr = idList.split(",");
          for (int i = 0; i < idArr.length; ++i) {
            try {
              int id = Integer.parseInt(idArr[i]);
              ids.add(Integer.valueOf(id));
            } catch (NumberFormatException nfe) {
              System.err.println("Unable to parse Identifier " + idArr[i]);
              continue;
            }
          }
        } else {
          System.err.println("Missing identifier list.");
          printUsage();
          System.exit(1);
        }
      }

      else {
        System.err
            .println("Skipping unrecognized argument: " + args[argsIndex]);
      }
    }

    log.debug("Recording solver starting up.");
    TempRecordingSolver client = new TempRecordingSolver();
    client.filenameExtension = "csv";
    client.addIds(ids);
    client.setHost(args[0]);
    client.setPort(aggregatorPort);
    if (baseFileName != null) {
      client.setBaseFileName(baseFileName);
    }
    client.setPhysicalLayer(SampleMessage.PHYSICAL_LAYER_PIPSQUEAK);

    Thread solver = new Thread(client, "Temp. Recording Solver");
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
        .append("<aggregator host> [-p <aggregator port>] -o <output file> [-t <ID 1>,<ID 2>,...,<ID N>]");

    usageString.append("\n\t");

    // Output file name
    usageString
        .append("-o : Followed by a string, prepended to the output file name, which is the date/time by default.");

    usageString.append("\n\t");

    // Port number
    usageString
        .append("-p : Followed by an integer value, the solver port of the aggregator.");

    usageString.append("\n\t");

    // Rotating output file
    usageString
        .append("-t : Comma-separated list (with no spaces) of identifier values to capture. If not specified, defaults to all Pip devices.");

    usageString.append("\n");

    System.err.println(usageString.toString());

  }

  boolean prepareOutputFile() {
    StringBuffer outFileName = new StringBuffer();
    if (this.baseFileName != null) {
      outFileName.append(this.baseFileName);
    }

    outFileName.append(".").append(this.filenameExtension);

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
        this.output = new PrintWriter(new FileWriter(outFile));
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
          this.output = new PrintWriter(new FileWriter(outFile));
          try {
            oldFile.close();
          } catch (IOException e) {
            log.warn("Unable to close old output file: {}", e);
            e.printStackTrace();
          }
        }

      }
    } catch (FileNotFoundException e) {
      log.error("Could not find {} to create output stream.", outFileName);
      return false;
    } catch (IOException ioe) {
      log.error("Could not open output file for writing.", ioe);

      return false;
    }

    return true;
  }

  public void addIds(List<Integer> ids) {
    for (Integer i : ids) {
      this.addId(i.intValue());
    }
  }

  public void addId(int id) {
    byte[] bytes = new byte[16];
    bytes[12] = (byte) (id >> 24);
    bytes[13] = (byte) (id >> 16);
    bytes[14] = (byte) (id >> 8);
    bytes[15] = (byte) id;
    HashableByteArray arr = new HashableByteArray(bytes);
    System.out.println("Added " + id + "/" + arr.toString());
    this.idMap.put(arr, Boolean.TRUE);
  }

  @Override
  protected boolean recordSample(SampleMessage sample) {    
    // Skip non-Pips
    if (sample.getPhysicalLayer() != 1) {
      return true;
    }
    HashableByteArray ident = new HashableByteArray(sample.getDeviceId());
    if (!this.idMap.containsKey(ident)) {
      return true;
    }

    // This better be an integer
    byte[] data = sample.getSensedData();
    if (data == null || data.length != 4) {
      return true;
    }
    int temp = 0;
    temp |= data[0] & 0xFF;
    temp |= (data[1] << 8) & 0xFF00;
    temp |= (data[2] << 16) & 0xFF0000;
    temp |= (data[3] << 24) & 0xFF000000;

    int id = 0;
    id |= sample.getDeviceId()[15] & 0xFF;
    id |= (sample.getDeviceId()[14] << 8) & 0xFF00;
    id |= (sample.getDeviceId()[13] << 16) & 0xFF0000;
    id |= (sample.getDeviceId()[12] << 24) & 0xFF000000;
    final String outString = String.format("%s,%d,%d",
        this.dateFormat.format(new Date()), Integer.valueOf(id),
        Integer.valueOf(temp));
    System.out.println(outString);
    this.output.println(outString);
    this.output.flush();
    return true;
  }

  /**
   * Shuts down the solver after closing the output file.
   */
  @Override
  protected void performShutdown() {
    if (this.output != null) {
      synchronized (this.output) {

        this.output.close();

      }
    }
    System.exit(0);
  }

}
