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
package root.hap;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import root.hap.availability.HierarchicalAvailabilityDriver;
import root.hap.cluster.HierarchicalClusterDriver;
import root.hap.responsibility.HierarchicalResponsibilityDriver;
import root.input.InputJob;


/**
 * <p>
 * This is the driver class for the Hierarchical Affinity Propagation algorithm
 * implemented in MapReduce.
 * </p>
 * 
 * <p>
 * This job is broken up into three sub-jobs, namely the Availability,
 * Responsibility, and Cluster jobs. The Availability and Responsibility 
 * jobs iterate until the iteration threshold is reached. ClusterJob operates
 * at the end to extract the cluster data.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see HierarchicalResponsibilityDriver
 * @see HierarchicalAvailabilityDriver
 * @see HierarchicalClusterDriver
 * 
 */
public class HierarchicalAffinityPropagationJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.

	// This is the number of iterations the algorithm will be required
	// to run before halting. Defaults to 1.
	private static String numIterations;
	private static String simMatFileName="/part-m-00000";

	// This is the value for lambda, or the dampening factor.
	// Defaults to 0.
	private static String lambda;

	// These are the prefixes of the intermediary file IO locations.
	private static String RD_File = "/RD";
	private static String AD_File = "/AD";
	private static String CD_File = "/CD";
	private static String workingDirectory;

	// -------------------------------------------------------------------
	// The following configuration variables must be set by the user.

	// These are the names of the initial input and final output datasets
	// to be used. Must be specified by the user.
	private static String inputDirectory;
	private static String outputDirectory;

	// This is the value for the size of the data set (NxN)
	private static String N = null;

	// This is the value for the number of levels of clusters HAP should
	// generate
	private static String numLevels = null;

	// -------------------------------------------------------------------
	// The following are temporary environment variables, not to be configured.

	// These argument arrays will be passed down into the Responsibility
	// and Availability MapReduce drivers.
	private static String[] RDargs = new String[12];
	private static String[] ADargs = new String[12];
	private static String[] CDargs = new String[6];

	private static final HierarchicalResponsibilityDriver HRD = 
			new HierarchicalResponsibilityDriver();
	private static final HierarchicalAvailabilityDriver HAD = 
			new HierarchicalAvailabilityDriver();
	private static final HierarchicalClusterDriver HCD = 
			new HierarchicalClusterDriver();


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("workDir", "w", "Working Directory", true);
		addOption("out", "o", "Output Directory", true);
		addOption("numLevels", "l", "Number of Levels", "1");
		addOption("numIter", "iter", "Number of Iterations", "1");
		addOption("lambda", "lambda", "Dampening Factor", "0");
		addOption("inputSize", "n", "Cardinality of the Dataset", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Hierarchical Affinity Propagation Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-w\t\t" + workingDirectory);
		System.out.println("\t-o\t\t" + outputDirectory);
		System.out.println("\t-l\t\t" + numLevels);
		System.out.println("\t-iter\t\t" + numIterations);
		System.out.println("\t-lambda\t" + lambda);
		System.out.println("\t-n\t" + N);
		System.out.println();
	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		workingDirectory = getOption("workDir");
		workingDirectory = cleanDirectoryName(workingDirectory);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory);
		numLevels = getOption("numLevels");
		numIterations = getOption("numIter");
		lambda = getOption("lambda");
		N = getOption("inputSize");

		// Set the input and output directories as specified by the user.
		RDargs[0] = "-i";
		RDargs[1] = RD_File;
		RDargs[2] = "-o";
		RDargs[3] = AD_File;
		RDargs[4] = "-n";
		RDargs[5] = N;
		RDargs[6] = "-lambda";
		RDargs[7] = lambda;
		RDargs[8] = "-l";
		RDargs[9] = numLevels;
		RDargs[10] = "-iter";

		ADargs[0] = "-i";
		ADargs[1] = RD_File;
		ADargs[2] = "-o";
		ADargs[3] = AD_File;
		ADargs[4] = "-n";
		ADargs[5] = N;
		ADargs[6] = "-lambda";
		ADargs[7] = lambda;
		ADargs[8] = "-l";
		ADargs[9] = numLevels;
		ADargs[10] = "-iter";

		CDargs[0] = "-i";
		CDargs[1] = CD_File;
		CDargs[2] = "-o";
		CDargs[3] = outputDirectory;
		CDargs[4] = "-n";
		CDargs[5] = N;

		// Initialize numIterations if it wasn't set by args
		if (Integer.valueOf(numIterations) < Integer.valueOf(numLevels)) {
			System.err.println(
					"Number of Iterations \'" + numIterations
					+ "\' was less than Number of Levels \'"
					+ numLevels + "\'");
			System.exit( 2 );
		}

		if( Integer.valueOf( N ) <= 0 ) { 
			System.err.println("[ERROR]: Invalid dataset cardinality.");
			System.err.println("[INFO]: N = " + N );
			System.exit( 1 );
		}

	}


	/* 
	 * Extract the value of N from the similarity matrix. This is based on 
	 * the maximum length of any vector from the matrix.
	 */
	//	private void extractNFromSimMat(Configuration conf, FileSystem fs,
	//			Path simMatPath) throws IOException {
	//		@SuppressWarnings("resource")
	//		SequenceFile.Reader reader = new SequenceFile.Reader(fs, simMatPath,
	//				conf);
	//		Text key = new Text();
	//		VectorWritable value = new VectorWritable();
	//
	//		int maxSize = -1;
	//
	//		while (reader.next(key, value)) {
	//			int size = value.get().size();
	//			if (size > maxSize) {
	//				maxSize = size;
	//			}
	//		}
	//
	//		N = maxSize + "";
	//		ADargs[5] = N;
	//		RDargs[5] = N;
	//		CDargs[5] = N;
	//	}


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

		if (workingDirectory == null) {
			workingDirectory = workingFS.getWorkingDirectory().toString();
		}

		// Check to see if input directory exists
		Path input = new Path(inputDirectory);
		if (!inputFS.exists(input)) {
			System.err.println("Input Directory does not exist.");
			System.exit(2);
		}

		Path simMatPath = new Path(inputDirectory+simMatFileName);
		if (!inputFS.exists(simMatPath)) {
			System.err.println("Similarity Matrix File does not exist.");
			System.exit(2);
		}

		int iterations = Integer.valueOf(numIterations);
		for (int i = 0; i < iterations; i++) {

			RDargs[1] = workingDirectory + RD_File + i;
			RDargs[3] = workingDirectory + AD_File + i;
			RDargs[11] = i + "";

			// If this is the first run, read from initial input.
			if (i == 0) {
				RDargs[1] = inputDirectory;
			}

			System.out.println();
			System.out.println("----------------------");
			System.out.println("Updating Responsibilty");
			System.out.println("----------------------");
			System.out.println("\tInput: " + RDargs[1]);
			System.out.println("\tOutput: " + RDargs[3]);
			System.out.println("\tIteration: " + (i + 1) + " of "
					+ numIterations);
			System.out.println();
			ToolRunner.run(conf, HRD, RDargs);

			// Delete input directory to remove wasted space.
			// Preserve the initial input, though.
			if( i > 0 ) { 
				workingFS.delete(new Path(RDargs[1]), true);
			}

			ADargs[1] = workingDirectory + AD_File + i;
			ADargs[3] = workingDirectory + RD_File + (i + 1);
			ADargs[11] = i + "";

			if (i == iterations - 1) {
				ADargs[3] = workingDirectory + CD_File;
			}

			System.out.println();
			System.out.println("---------------------");
			System.out.println("Updating Availability");
			System.out.println("---------------------");
			System.out.println("\tInput: " + ADargs[1]);
			System.out.println("\tOutput: " + ADargs[3]);
			System.out.println("\tIteration: " + (i + 1) + " of "
					+ numIterations);
			System.out.println();
			ToolRunner.run(conf, HAD, ADargs);

			workingFS.delete(new Path(ADargs[1]), true);

		}

		CDargs[1] = workingDirectory + CD_File;

		System.out.println();
		System.out.println("---------------------");
		System.out.println("Extracting Clusters");
		System.out.println("---------------------");
		System.out.println("\tInput: " + CDargs[1]);
		System.out.println("\tOutput: " + CDargs[3]);
		System.out.println();
		ToolRunner.run(conf, HCD, CDargs);

		workingFS.delete(new Path(CDargs[1]), true);

		workingFS.close();
		inputFS.close();
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
				new HierarchicalAffinityPropagationJob(),
				args);
		System.exit(res);
	}

}