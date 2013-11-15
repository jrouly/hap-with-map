/**
 * Copyright 2013 AMALTHEA REU; Dillon Rose; Michel Rouly
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package root.benchmark.canopy;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.canopy.CanopyDriver;

import root.input.InputJob;

/**
 * <p>
 * This is a wrapper class for the Mahout Canopy Clustering tool.
 * </p>
 * 
 * <p>
 * This class is used for benchmarking only.
 * </p>

 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class CanopyJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.

	private static String distanceMeasure;
	private static String threshold1;
	private static String threshold2;

	// -------------------------------------------------------------------
	// The following configuration variables must be set by the user.

	// These are the names of the initial input and final output datasets
	// to be used. Must be specified by the user.
	private static String inputDirectory;
	private static String outputDirectory;


	// -------------------------------------------------------------------
	// The following are temporary environment variables, not to be configured.

	// These argument arrays will be passed down into the Responsibility
	// and Availability MapReduce drivers.
	private static String[] canopyArgs = new String[11];


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("out", "o", "Output Directory", true);
		addOption("distance", "dm", "Distance Measure", "org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure");
		addOption("threshold1", "t1", "Inner Threshold", "1");
		addOption("threshold2", "t2", "Outer Threshold", "1");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory);
		distanceMeasure = getOption("distance");
		threshold1 = getOption("threshold1");
		threshold2 = getOption("threshold2");

		// Set the input and output directories as specified by the user.
		canopyArgs[0] = "-i";
		canopyArgs[1] = inputDirectory;
		canopyArgs[2] = "-o";
		canopyArgs[3] = outputDirectory;
		canopyArgs[4] = "-dm";
		canopyArgs[5] = distanceMeasure;
		canopyArgs[6] = "-t1";
		canopyArgs[7] = threshold1;
		canopyArgs[8] = "-t2";
		canopyArgs[9] = threshold2;
		canopyArgs[10] = "-cl";
	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Canopy Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-o\t\t" + outputDirectory);
		System.out.println("\t-dm\t\t" + distanceMeasure);
		System.out.println("\t-t1\t\t" + threshold1);
		System.out.println("\t-t2\t\t" + threshold2);
		System.out.println("\t-cl");
		System.out.println();
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public int run(String[] args) throws Exception {

		constructParameterList();

		if (parseArguments(args) == null) {
			return -1;
		}

		initializeConfigurationParameters();

		printJobHeader();

		Configuration conf = getConf();

		URI inputURI = new URI(inputDirectory);

		FileSystem fs = FileSystem.get(inputURI, conf);

		Path input = new Path(inputDirectory);
		if (!fs.exists(input)) {
			System.err.println("Input Directory does not exist.");
			System.exit(2);
		}

		ToolRunner.run(conf, new CanopyDriver(), canopyArgs);

		return 0;
	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run( 
				new Configuration(),
				new CanopyJob(),
				args);
		System.exit(res);
	}

}