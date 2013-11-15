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
package root.input.reuters21578;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import root.hap.HierarchicalAffinityPropagationJob;
import root.input.InputJob;
import root.output.LogJob;


/**
 * <p>
 * This is a driver for running the Hierarchical Affinity Propagation job
 * on the Reuters news archive input dataset.
 * </p>
 * 
 * <p>
 * ReutersJob only accepts input formatted in the manner of the extracted 
 * plaintext Reuters dataset.
 * </p>
 * 
 * <p>
 * To run this program from the command line:<br/>
 * <code>$ bin/hadoop root.Driver directive options</code>
 * </p>
 * 
 * <p>
 * Include (at minimum) the following external libraries on the classpath: <br />
 * <code>
 * 	lib/run/*.jar<br/>
 * 	lib/build/*.jar<br/>
 * </code>
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class ReutersJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.
	// -For VectorizationJob
	private static String exclusionThreshold;
	private static String minimumDocumentFrequency;
	private static String distanceMetric;
	private static String vectorizationOutputDirectory = "/vectorization";
	private static String tf_tfidf;

	// -For AffinityPropagationJob
	private static String apInputDirectory = "/similarityMatrix";
	private static String apOutputDirectory = "/affinityPropagation";
	private static String numIterations;
	private static String numLevels;
	private static String diagScale;
	private static String lambda;
	private static String N;

	// -For HiveJob
	//	private static String dir_hiveDataRoot = "/hive";
	//	private static String dir_dataWordDict = "/word-dictionary";
	private static String dir_dataMetaData = "/file-dictionary";
	//		private static String dir_dataTFVectors = "/vectorFiles/";
	//	private static String topNWords;

	private static String timeStamp = "/timestamp";

	// -------------------------------------------------------------------
	// The following configuration variables must be set by the user.

	// -For VectorizationJob
	private static String inputDirectory;
	private static String workingDirectory;

	// -For AffinityPropagationJob

	// -For HiveJob
	//	private static String url_mysql; // mysql://localhost:3306/db_presentation
	//	private static String usr_mysql; // amalthea
	//	private static String psw_mysql; // Bwn8wZxBHS2Tdxe3

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
		addOption("tf_tfidf", "tf_tfidf", "Use TF or TF-IDF Vectors", "tf");
		addOption("numLevels", "l", "Number of Levels", "1");
		addOption("diagScale","smd","Similarity Matrix seed scale",true);
		addOption("numIter", "iter", "Number of Iterations", "1");
		addOption("lambda", "lambda", "Dampening Factor", "0");
		addOption("inputSize", "n", "Cardinality of the Dataset", true);
		//		addOption("url_mysql", "sql", "URL for MySQL DB", true);
		//		addOption("usr_mysql", "sqlu", "User for MySQL DB","");
		//		addOption("psw_mysql", "sqlp", "Password for MySQL DB","");
		//		addOption("topNWords", "topN", "Numbers of Words to put in DB", "10");

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
		tf_tfidf = getOption("tf_tfidf");
		if (!(tf_tfidf.equals("tf") || tf_tfidf.equals("tfidf"))) {
			System.err.println("Invalid input for tf_tfidf: \'"+tf_tfidf+"\'");
			System.exit(1);
		}
		numLevels = getOption("numLevels");
		diagScale = getOption("diagScale");
		numIterations = getOption("numIter");
		lambda = getOption("lambda");
		N = getOption("inputSize");
		//		url_mysql = getOption("url_mysql");
		//		usr_mysql = getOption("usr_mysql");
		//		psw_mysql = getOption("psw_mysql");
		//		topNWords = getOption("topNWords");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() { 
		System.out.println("[INFO]: Reuters Job" );
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-w\t\t" + workingDirectory);
		System.out.println("\t-x\t\t" + exclusionThreshold);
		System.out.println("\t-mdf\t\t" + minimumDocumentFrequency);
		System.out.println("\t-dm\t\t" + distanceMetric);
		System.out.println("\t-tf_tfidf\t\t" + tf_tfidf);
		System.out.println("\t-l\t\t" + numLevels);
		System.out.println("\t-smd\t\t" + diagScale);
		System.out.println("\t-iter\t\t" + numIterations);
		System.out.println("\t-lambda\t\t" + lambda);
		System.out.println("\t-n\t\t" + N);
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

		//		workingFS.mkdirs( new Path( workingDirectory + vectorizationOutputDirectory ) );
		//		workingFS.mkdirs( new Path( workingDirectory + vectorizationOutputDirectory + apInputDirectory ));
		//
		//		String[] similarityMatrixConverterArgs = { 
		//				"-i",   inputDirectory + "/Ssym.csv",
		//				"-o",   workingDirectory + vectorizationOutputDirectory + apInputDirectory,
		//				"-l",    numLevels
		//		};
		//		ToolRunner.run( conf, new SimilarityMatrixConverter(), similarityMatrixConverterArgs );


		String[] vectorizationArgs = { 
				"-i",   inputDirectory,
				"-o",   workingDirectory + vectorizationOutputDirectory,
				"-x",   exclusionThreshold,
				"-l",   numLevels,
				"-mdf", minimumDocumentFrequency,
				"-dm",  distanceMetric,
				"-tf_tfidf", tf_tfidf,
				"-smd", diagScale
		};
		System.out.println();
		ToolRunner.run(conf,new VectorizationJob(), vectorizationArgs);

		long starttime, stoptime, deltatime;
		starttime = System.currentTimeMillis();

		String[] hapArgs = {
				"-i", workingDirectory + vectorizationOutputDirectory
				+ apInputDirectory,
				"-o", workingDirectory + apOutputDirectory,
				"-l", numLevels,
				"-w", workingDirectory,
				"-iter", numIterations,
				"-lambda", lambda,
				"-n", N
		};
		ToolRunner.run(conf, new HierarchicalAffinityPropagationJob(), hapArgs);

		stoptime = System.currentTimeMillis();
		deltatime = stoptime - starttime;

		conf.setStrings(CONF_PREFIX + "Dataset",    "Reuters");
		conf.setStrings(CONF_PREFIX + "iterations", numIterations);
		conf.setStrings(CONF_PREFIX + "levels",     numLevels);
		conf.setStrings(CONF_PREFIX + "lambda",     lambda);
		conf.setStrings(CONF_PREFIX + "SMatSeed",   diagScale);
		writeTimestamp(conf, workingDirectory + timeStamp, deltatime);

		String[] logJobArgs = {
				"-s", inputDirectory,
				"-c", workingDirectory + apOutputDirectory,
				"-t", workingDirectory+timeStamp,
				"-f", workingDirectory + vectorizationOutputDirectory
				+ dir_dataMetaData +"/fileName2docId"
		};
		ToolRunner.run(conf, new LogJob(), logJobArgs);
		//
		//		// Move files into position for HiveJob
		//		workingFS = FileSystem.get(workingURI, conf);
		//		workingFS.mkdirs(new Path(workingDirectory + dir_hiveDataRoot));
		//		Path currWordDict = new Path(workingDirectory
		//				+ vectorizationOutputDirectory
		//				+ "/vectorFiles/dictionary.file-0");
		//		Path nextWordDict = new Path(workingDirectory
		//				+ vectorizationOutputDirectory + dir_dataWordDict
		//				+ "/dictionary.file-0");
		//		workingFS.mkdirs(new Path(workingDirectory
		//				+ vectorizationOutputDirectory + dir_dataWordDict));
		//		workingFS.rename(currWordDict, nextWordDict);
		//
		//		String[] hiveArgs = {
		//				"--sqldb", url_mysql,
		//				"--sqlusr", usr_mysql,
		//				"--sqlpsw", psw_mysql,
		//				"--dataroot", workingDirectory + dir_hiveDataRoot,
		//				"--ap-out", workingDirectory + apOutputDirectory,
		//				"--word-dict", workingDirectory + vectorizationOutputDirectory
		//				+ dir_dataWordDict,
		//				"--metadata", workingDirectory + vectorizationOutputDirectory
		//				+ dir_dataMetaData,
		//				"--tf-vec", workingDirectory + vectorizationOutputDirectory
		//				+ dir_dataTFVectors + tf_tfidf + "-vectors",
		//				"--nwords", topNWords
		//		};
		//		ToolRunner.run(conf, new OutputJob(), hiveArgs);

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
				new ReutersJob(),
				args);
		System.exit(res);
	}

}
