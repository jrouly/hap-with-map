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
package root.benchmark;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.topdown.postprocessor.ClusterOutputPostProcessorDriver;

import root.benchmark.canopy.CanopyJob;
import root.benchmark.kmeans.KMeansJob;
import root.input.InputJob;


/**
 * <p>
 * This is a driver for running the Hierarchical Affinity Propagation job
 * on the Reuters news archive input dataset.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class ReutersBenchmarkJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.
	// -For VectorizationJob
	private static String exclusionThreshold;
	private static String minimumDocumentFrequency;
	private static String distanceMetric;
	private static String vectorizationOutputDirectory = "/vectorization";
	private static String threshold1;
	private static String threshold2;

	// -For AffinityPropagationJob
	private static String canopyOutputDirectory = "/canopy";
	private static String kmeansOutputDirectory = "/kmeans";
	private static String outputDirectory = "/output";
	private static String numIterations;
	private static String convergenceDelta;

	private static String timeStamp = "/timestamp";

	// -------------------------------------------------------------------
	// The following configuration variables must be set by the user.

	// -For VectorizationJob
	private static String inputDirectory;
	private static String workingDirectory;

	// -------------------------------------------------------------------
	// The following are environment variables, not to be configured.


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("workDir", "w", "Working Directory", true);
		addOption("excThres", "x", "Exclusion Threshold", "100");
		addOption("minDocFreq", "mdf", "Minimum Document Frequency", "1");
		addOption("distance", "dm", "Distance Measure",
				"org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure");
		addOption("maxIter", "mIter", "Maximum number of Iterations", true);
		addOption("convergenceDelta", "cd", "Threshold for Convergence", true);

		addOption("threshold1", "t1", "Inner Threshold", true);
		addOption("threshold2", "t2", "Outer Threshold", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		workingDirectory = getOption("workDir");
		workingDirectory = cleanDirectoryName(workingDirectory);
		exclusionThreshold = getOption("excThres");
		minimumDocumentFrequency = getOption("minDocFreq");
		distanceMetric = getOption("distance");
		numIterations = getOption("maxIter");
		convergenceDelta = getOption("convergenceDelta");

		threshold1 = getOption("threshold1");
		threshold2 = getOption("threshold2");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Reuters Benchmarking Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-w\t\t" + workingDirectory);
		System.out.println("\t-x\t\t" + exclusionThreshold);
		System.out.println("\t-mdf\t\t" + minimumDocumentFrequency);
		System.out.println("\t-dm\t\t" + distanceMetric);
		System.out.println("\t-mIter\t\t" + numIterations);
		System.out.println("\t-cd\t\t" + convergenceDelta);
		System.out.println("\t-t1\t\t" + threshold1);
		System.out.println("\t-t2\t\t" + threshold2);
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

		URI workingURI = new URI(conf.get("fs.default.name"));
		URI inputURI = new URI(inputDirectory);

		FileSystem workingFS = FileSystem.get(workingURI, conf);
		FileSystem inputFS = FileSystem.get(inputURI, conf);

		Path inputDirectoryPath = new Path(inputDirectory);
		if (!inputFS.exists(inputDirectoryPath)) {
			throw new Exception("Input directory not found.");
		}
		Path workingDirectoryPath = new Path(workingDirectory);
		if (workingFS.exists(workingDirectoryPath)) {
			throw new Exception("Working Directory already exists.");
		}
		if (!workingFS.mkdirs(workingDirectoryPath)) {
			throw new Exception("Failed to create Working Directory.");
		}

		String[] vectorizationArgs = { 
				"-i",   inputDirectory,
				"-o",   workingDirectory + vectorizationOutputDirectory,
				"-x",   exclusionThreshold,
				"-mdf", minimumDocumentFrequency,
		};
		System.out.println();
		ToolRunner.run(conf,new ReutersVectorizationJob(), vectorizationArgs);

		long starttime, stoptime, deltatime;
		starttime = System.currentTimeMillis();

		String[] canopyArgs = { 
				"-i",  workingDirectory + vectorizationOutputDirectory + "/vectorFiles/tf-vectors",
				"-o",  workingDirectory + canopyOutputDirectory,
				"-dm", distanceMetric, 
				"-t1", threshold1,
				"-t2", threshold2
		};
		ToolRunner.run( conf, new CanopyJob(), canopyArgs );

		String[] kmeansArgs = {
				"-i",     workingDirectory + vectorizationOutputDirectory + "/vectorFiles/tf-vectors",
				"-o",     workingDirectory + kmeansOutputDirectory,
				"-c",     workingDirectory + canopyOutputDirectory + "/clusters-0-final",
				"-dm",    distanceMetric,
				"-cd",    convergenceDelta,
				"-mIter", numIterations,
				"-k",     "3"
		};
		ToolRunner.run( conf, new KMeansJob(), kmeansArgs );

		Path input = new Path(workingDirectory + kmeansOutputDirectory);
		Path output = new Path(workingDirectory + outputDirectory);
		boolean sequential = false;

		ClusterOutputPostProcessorDriver.run(input, output, sequential);

		stoptime = System.currentTimeMillis();
		deltatime = stoptime - starttime;

		conf.setStrings(CONF_PREFIX + "Dataset",    "Image");
		conf.setStrings(CONF_PREFIX + "Iterations", numIterations);
		conf.setStrings(CONF_PREFIX + "Levels",     convergenceDelta);
		writeTimestamp(conf, workingDirectory + timeStamp, deltatime);

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
				new ReutersBenchmarkJob(),
				args);
		System.exit(res);
	}

}
