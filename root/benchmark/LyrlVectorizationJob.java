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

import root.input.InputJob;
import root.input.lyrl2004.FormatVectorsJob;
import root.input.lyrl2004.FormatWordDictJob;


/**
 * <p>
 * This class manipulates the input directory of files provided to LyrlJob 
 * in preparation for use by Hierarchical Affinity Propagation.
 * </p>
 * 
 * <p>
 * The class performs four main tasks:
 * <ol>
 *  <li>Copies files to HDFS and renames files 1-N</li>
 *  <li>Converts text to sequence file</li>
 *  <li>Creates vectors from text</li>
 *  <li>Create a similarity matrix</li>
 * </ol>
 * </p>
 * 
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class LyrlVectorizationJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.

	// -------------------------------------------------------------------
	// The following configuration variables must be set by the user.
	private static String inputDirectory;
	private static String wordDictDirectory;
	private static String workDir;

	// -------------------------------------------------------------------
	// The following are environment variables, not to be configured.
	private static String vectorDirectory = "/vectorFiles/vector-file";
	private static String fileDictDirectory = "/vectorFiles/file-dictionary";
	private static String wordDictionaryDirectory = "/wordDict";


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("workDir", "w", "Working Directory", true);
		addOption("wordDictDir", "wd", "Word Dictionary Directory", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		workDir = getOption("workDir");
		workDir = cleanDirectoryName(workDir);
		wordDictDirectory = getOption("wordDictDir");
		wordDictDirectory = cleanDirectoryName(wordDictDirectory);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() { 
		System.out.println("[INFO]: Lyrl2004 Vectorization Job" );
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-w\t\t" + workDir);
		System.out.println("\t-wd\t\t" + wordDictDirectory);
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
		FileSystem inputFS = FileSystem.get(inputURI, conf);

		printConfiguredParameters();

		Path inputDirectoryPath = new Path(inputDirectory);

		if (!inputFS.exists(inputDirectoryPath)) {
			throw new Exception("Input directory not found.");
		}

		vectorDirectory = workDir + vectorDirectory;
		fileDictDirectory = workDir + fileDictDirectory;
		wordDictionaryDirectory = workDir+ wordDictionaryDirectory;

		// 1: Formatting vectors
		System.out.println();
		System.out.println("------------------");
		System.out.println("Formatting Vectors");
		System.out.println("------------------");
		System.out.println("\tInput: " + inputDirectory);
		System.out.println("\tVector Directory: " + vectorDirectory);
		System.out.println("\tFile Dictionary Directory: " + fileDictDirectory);
		System.out.println();
		String[] arguments_FormatVectorsJob = { 
				"-i", inputDirectory,
				"-v", vectorDirectory,
				"-f", fileDictDirectory
		};
		ToolRunner.run(new FormatVectorsJob(), arguments_FormatVectorsJob);

		// 2: Formatting Word Dictionary
		System.out.println();
		System.out.println("--------------------------");
		System.out.println("Formatting Word Dictionary");
		System.out.println("--------------------------");
		System.out.println("\tInput: " + wordDictDirectory);
		System.out.println("\tOutput: " + wordDictionaryDirectory);
		System.out.println();
		String[] arguments_FormatWordDictJob = { 
				"-i", wordDictDirectory,
				"-o", wordDictionaryDirectory
		};
		ToolRunner.run(new FormatWordDictJob(), arguments_FormatWordDictJob);

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new LyrlVectorizationJob(),args);
		System.exit(res);
	}
}
