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

import root.hap.HierarchicalAffinityPropagationJob;
import root.input.InputJob;
import root.input.lyrl2004.VectorizationJob;
import root.output.LogJob;


/**
 * <p>
 * This is a driver for running the Hierarchical Affinity Propagation job
 * on the Lyrl input dataset.
 * </p>
 * 
 * <p>
 * LyrlJob only accepts input formatted in the manner of the Lyrl2004 dataset.
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
 * @since 2013.06.24
 * 
 */
public class LyrlBenchmarkJob extends InputJob {

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
	private static String vectorDirectory;
	private static String wordDictDirectory;
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
	protected void constructParameterList() {

		addOption("vector", "v", "Vector Directory", true);
		addOption("wordDictDir", "wd", "Word Dictionary Directory", true);
		addOption("workDir", "w", "Working Directory", true);
		addOption("numLevels", "l", "Number of Levels", "1");
		addOption("diagScale","smd","Similarity Matrix seed scale",true);
		addOption("numIter", "iter", "Number of Iterations", "1");
		addOption("lambda", "lambda", "Dampening Factor", "0");
		//		addOption("url_mysql", "sql", "URL for MySQL DB", true);
		//		addOption("usr_mysql", "sqlu", "User for MySQL DB","");
		//		addOption("psw_mysql", "sqlp", "Password for MySQL DB","");
		//		addOption("topNWords", "topN", "Numbers of Words to put in DB", "10");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		vectorDirectory = getOption("vector");
		vectorDirectory = cleanDirectoryName(vectorDirectory);
		wordDictDirectory = getOption("wordDictDir");
		wordDictDirectory = cleanDirectoryName(wordDictDirectory);
		workingDirectory = getOption("workDir");
		workingDirectory = cleanDirectoryName(workingDirectory);
		numLevels = getOption("numLevels");
		diagScale = getOption("diagScale");
		numIterations = getOption("numIter");
		lambda = getOption("lambda");
		//		url_mysql = getOption("url_mysql");
		//		usr_mysql = getOption("usr_mysql");
		//		psw_mysql = getOption("psw_mysql");
		//		topNWords = getOption("topNWords");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Lyrl2004 Job");
		System.out.println("\t-v\t\t" + vectorDirectory);
		System.out.println("\t-wd\t\t" + wordDictDirectory);
		System.out.println("\t-w\t\t" + workingDirectory);
		System.out.println("\t-l\t\t" + numLevels);
		System.out.println("\t-smd\t\t" + diagScale);
		System.out.println("\t-iter\t\t" + numIterations);
		System.out.println("\t-lambda\t\t" + lambda);
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
		URI inputURI = new URI(vectorDirectory);

		FileSystem workingFS = FileSystem.get(workingURI, conf);
		FileSystem inputFS = FileSystem.get(inputURI, conf);

		Path vectorDirectoryPath = new Path(vectorDirectory);
		if( ! inputFS.exists( vectorDirectoryPath ) ) { 
			throw new Exception("Vector directory not found.");
		}
		Path wordDictDirectoryPath = new Path(wordDictDirectory);
		if( ! inputFS.exists( wordDictDirectoryPath ) ) { 
			throw new Exception("Word Dictionary directory not found.");
		}
		Path workingDirectoryPath = new Path(workingDirectory);
		if( workingFS.exists( workingDirectoryPath ) ) { 
			throw new Exception("Working Directory already exists.");
		}
		if(!workingFS.mkdirs(workingDirectoryPath)){
			throw new Exception("Failed to create Working Directory.");
		}


		String [] vectorizationArgs={
				"-i" ,  vectorDirectory,
				"-wd",  wordDictDirectory,
				"-w",   workingDirectory,
				"-l",   numLevels,
				"-dm",  "org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure",
				"-smd", diagScale
		};
		System.out.println();
		ToolRunner.run(conf, new VectorizationJob(), vectorizationArgs);


		long starttime,stoptime,deltatime;
		starttime = System.currentTimeMillis();

		String [] hapArgs={
				"-i" , workingDirectory+apInputDirectory,
				"-o" , workingDirectory+apOutputDirectory,
				"-l" , numLevels,
				"-w" , workingDirectory,
				"-iter", numIterations,
				"-lambda", lambda
		};
		ToolRunner.run(conf, new HierarchicalAffinityPropagationJob(), hapArgs);

		stoptime = System.currentTimeMillis();
		deltatime = stoptime-starttime;

		conf.setStrings(CONF_PREFIX + "Dataset",    "Lyrl2004");
		conf.setStrings(CONF_PREFIX + "iterations", numIterations);
		conf.setStrings(CONF_PREFIX + "levels",     numLevels);
		conf.setStrings(CONF_PREFIX + "lambda",     lambda);
		conf.setStrings(CONF_PREFIX + "SMatSeed",   diagScale);
		writeTimestamp(conf, workingDirectory + timeStamp, deltatime);


		String[] logJobArgs = { 
				"-s", vectorDirectory,
				"-c", workingDirectory + apOutputDirectory,
				"-t", workingDirectory+timeStamp,
				"-f", workingDirectory+dir_dataVectors
				+dir_dataMetaData+"/vectorName2docId",
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
				new LyrlBenchmarkJob(),
				args);
		System.exit(res);
	}

}
