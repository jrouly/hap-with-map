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
package root.input.points;

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
 * on the images input as RGB vectors.
 * </p>
 * 
 * <p>
 * ImagesJob accepts an image as its input dataset, where the image is 
 * represented as a set of point RGB vectors.
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
public class PointsJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.
	// -For VectorizationJob

	// -For AffinityPropagationJob
	private static String apInputDirectory="/similarityMatrix";
	private static String apOutputDirectory="/affinityPropagation";
	private static String numIterations;
	private static String numLevels;
	private static String diagScale;
	private static String lambda;
	private static String N;

	// -For HiveJob
	//	private static String dir_hiveDataRoot  = "/hive";
	private static String dir_dataMetaData  = "file-dictionary";
	private static String dir_dataVectors = "/vectorFiles/";
	//	private static String dir_dataTFVectorFile = "vector-file";
	//	private static String wordDictionaryDirectory = "/wordDict";

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
	//	private static String topNWords;

	// -------------------------------------------------------------------
	// The following are environment variables, not to be configured.


	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("workDir", "w", "Working Directory", true);
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
	@Override
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		workingDirectory = getOption("workDir");
		workingDirectory = cleanDirectoryName(workingDirectory);
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
		System.out.println("[INFO]: Image Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-w\t\t" + workingDirectory);
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
	public int run( String[] args ) throws Exception {

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

		Path vectorDirectoryPath = new Path(inputDirectory);
		if( ! inputFS.exists( vectorDirectoryPath ) ) { 
			throw new Exception("Points directory not found.");
		}
		Path workingDirectoryPath = new Path(workingDirectory);
		if( workingFS.exists( workingDirectoryPath ) ) { 
			throw new Exception("Working Directory already exists.");
		}
		if(!workingFS.mkdirs(workingDirectoryPath)){
			throw new Exception("Failed to create Working Directory.");
		}


		String [] vectorizationArgs={
				"-i",   inputDirectory,
				"-w",   workingDirectory,
				"-smd", diagScale,
				"-l",   numLevels,
				"-dm",  "org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure"
		};
		System.out.println();
		ToolRunner.run(conf,new VectorizationJob(), vectorizationArgs);

		long starttime,stoptime,deltatime;
		starttime = System.currentTimeMillis();

		String [] hapArgs={
				"-i" , workingDirectory+apInputDirectory,
				"-o" , workingDirectory+apOutputDirectory,
				"-l" , numLevels,
				"-w" , workingDirectory,
				"-iter", numIterations,
				"-lambda", lambda,
				"-n", N
		};
		ToolRunner.run(conf, new HierarchicalAffinityPropagationJob(), hapArgs);

		stoptime = System.currentTimeMillis();
		deltatime = stoptime-starttime;

		conf.setStrings(CONF_PREFIX + "Dataset",    "Image");
		conf.setStrings(CONF_PREFIX + "Iterations", numIterations);
		conf.setStrings(CONF_PREFIX + "Levels",     numLevels);
		conf.setStrings(CONF_PREFIX + "Lambda",     lambda);
		conf.setStrings(CONF_PREFIX + "SMatSeed",   diagScale);
		writeTimestamp(conf, workingDirectory + timeStamp, deltatime);

		String[] logJobArgs = {
				"-s", inputDirectory,
				"-c", workingDirectory + apOutputDirectory,
				"-t", workingDirectory + timeStamp,
				"-f", workingDirectory + dir_dataVectors
				+ dir_dataMetaData + "/vectorName2docId",
		};
		ToolRunner.run(conf,new LogJob(), logJobArgs);

		//		workingFS = FileSystem.get(workingURI, conf);
		//		workingFS.mkdirs(new Path(workingDirectory+dir_hiveDataRoot));
		//
		//		String [] hiveArgs={
		//				"-sqldb"    , url_mysql,
		//				"-sqlusr"   , usr_mysql,
		//				"-sqlpsw"   , psw_mysql,
		//				"-dataroot" , workingDirectory+dir_hiveDataRoot,
		//				"-ap-out"   , workingDirectory+apOutputDirectory,
		//				"-word-dict", workingDirectory+wordDictionaryDirectory,
		//				"-metadata" , workingDirectory+dir_dataVectors+dir_dataMetaData,
		//				"-tf-vec"   , workingDirectory+dir_dataVectors+dir_dataTFVectorFile,
		//				"-nwords"   , topNWords
		//		};
		//		ToolRunner.run(conf,new OutputJob(), hiveArgs);

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String [] args) throws Exception{
		int res = ToolRunner.run( 
				new Configuration(),
				new PointsJob(),
				args);
		System.exit(res);
	}

}
