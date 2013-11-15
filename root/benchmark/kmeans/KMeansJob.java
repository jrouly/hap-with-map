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
package root.benchmark.kmeans;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;

import root.benchmark.canopy.CanopyJob;
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
public class KMeansJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.

	private static String distanceMeasure;
	private static String convergenceDelta;
	private static String maxIter;
	private static String k;

	// -------------------------------------------------------------------
	// The following configuration variables must be set by the user.

	// These are the names of the initial input and final output datasets
	// to be used. Must be specified by the user.
	private static String inputDirectory;
	private static String outputDirectory;
	private static String clusterDirectory;


	// -------------------------------------------------------------------
	// The following are temporary environment variables, not to be configured.

	// These argument arrays will be passed down into the Responsibility
	// and Availability MapReduce drivers.
	private static String[] kmeansArgs = new String[16];


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		//  --overwrite --clustering
		addOption("input", "i", "Input Directory", true);
		addOption("clusters", "c", "Cluster Directory", true);
		addOption("k", "k", "k", true);
		addOption("out", "o", "Output Directory", true);
		addOption("distance", "dm", "Distance Measure", true);
		addOption("convergenceDelta", "cd", "Convergence Delta", true);
		addOption("maxIter", "mIter", "Max Number of Iterations", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory);
		clusterDirectory = getOption("clusters");
		clusterDirectory = cleanDirectoryName(clusterDirectory);
		k = getOption("k");
		distanceMeasure = getOption("distance");
		convergenceDelta = getOption("convergenceDelta");
		maxIter = getOption("maxIter");


		// --convergenceDelta 0.001 --overwrite --maxIter 50 --clustering
		// Set the input and output directories as specified by the user.
		kmeansArgs[0] = "--input";
		kmeansArgs[1] = inputDirectory;
		kmeansArgs[2] = "--clusters";
		kmeansArgs[3] = clusterDirectory;
		kmeansArgs[4] = "-k";
		kmeansArgs[5] = k;
		kmeansArgs[6] = "--output";
		kmeansArgs[7] = outputDirectory;
		kmeansArgs[8] = "--distanceMeasure";
		kmeansArgs[9] = distanceMeasure;
		kmeansArgs[10] = "--convergenceDelta";
		kmeansArgs[11] = convergenceDelta;
		kmeansArgs[12] = "--maxIter";
		kmeansArgs[13] = maxIter;
		kmeansArgs[14] = "--overwrite";
		kmeansArgs[15] = "--clustering";
	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: KMeansJob");
		System.out.println("\t--input\t\t" + inputDirectory);
		System.out.println("\t--clusters\t\t" + clusterDirectory);
		System.out.println("\t--k\t\t" + k);
		System.out.println("\t--output\t\t" + outputDirectory);
		System.out.println("\t--distanceMeasure\t\t" + distanceMeasure);
		System.out.println("\t--convergenceDelta\t\t" + convergenceDelta);
		System.out.println("\t--maxIter\t\t" + maxIter);
		System.out.println("\t--overwrite");
		System.out.println("\t--clustering");
		System.out.println();
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public int run(String[] args) throws Exception {

		constructParameterList();
		for( String str : args ) { 
			System.out.println(str);
		}

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

		ToolRunner.run(conf, new KMeansDriver(), kmeansArgs);

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