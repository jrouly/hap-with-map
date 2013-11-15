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
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

import root.input.InputJob;
import root.input.reuters21578.RenameFilesJob;


/**
 * <p>
 * This class manipulates the input directory of files provided to ReutersJob 
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
 * @see RenameFilesJob
 * 
 */
public class ReutersVectorizationJob extends InputJob {

	// -------------------------------------------------------------------
	// The following configuration variables will have default values.
	private static String exclusionThreshold;
	private static String minimumDocumentFrequency;

	// -------------------------------------------------------------------
	// The following configuration variables must be set by the user.
	private static String inputDirectory;
	private static String outputDirectory;

	// -------------------------------------------------------------------
	// The following are environment variables, not to be configured.
	private static String sequenceFilesDirectory = "/sequenceFiles";
	private static String vectorDirectory = "/vectorFiles";
	private static String filenameDictionaryDirectory = "/";
	private static String renamedInputdirectory = "/renamedInput";


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("out", "o", "Output Directory", true);
		addOption("excThres", "x", "Exclusion Threshold", "100");
		addOption("minDocFreq", "mdf", "Minimum Document Frequency", "1");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory);
		exclusionThreshold = getOption("excThres");
		minimumDocumentFrequency = getOption("minDocFreq");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Reuters Benchmarking Vectorization Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-o\t\t" + outputDirectory);
		System.out.println("\t-x\t\t" + exclusionThreshold);
		System.out.println("\t-mdf\t\t" + minimumDocumentFrequency);
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

		String workingDirectory = workingFS.getWorkingDirectory() + "/";

		outputDirectory = workingDirectory + outputDirectory;

		Path inputDirectoryPath = new Path(inputDirectory);
		Path outputDirectoryPath = new Path(outputDirectory);

		if (!inputFS.exists(inputDirectoryPath)) {
			throw new Exception("Input directory not found.");
		}
		if (workingFS.delete(outputDirectoryPath, true)) {
			System.out.println("Output directory cleaned.");
		}

		sequenceFilesDirectory = outputDirectory + sequenceFilesDirectory;
		vectorDirectory = outputDirectory + vectorDirectory;
		filenameDictionaryDirectory = outputDirectory
				+ filenameDictionaryDirectory;
		renamedInputdirectory = outputDirectory + renamedInputdirectory;

		// 1: Renames files 1-N
		System.out.println();
		System.out.println("--------------");
		System.out.println("Renaming Files");
		System.out.println("--------------");
		System.out.println("\tInput: " + inputDirectory);
		System.out.println("\tOutput: " + renamedInputdirectory);
		System.out.println();
		String[] arguments_renameFiles = { "-i",inputDirectory,
				"-o",renamedInputdirectory, "-f",filenameDictionaryDirectory };
		ToolRunner.run(new RenameFilesJob(), arguments_renameFiles);

		// 2: Converts text to sequence file
		System.out.println();
		System.out.println("--------------------------------------");
		System.out.println("Creating Sequence Files From Directory");
		System.out.println("--------------------------------------");
		System.out.println("\tInput: " + renamedInputdirectory);
		System.out.println("\tOutput: " + sequenceFilesDirectory);
		System.out.println();
		String[] arguments_SequenceFilesFromDirectory = { "-i",
				renamedInputdirectory, "-o", sequenceFilesDirectory };
		ToolRunner.run(new SequenceFilesFromDirectory(),
				arguments_SequenceFilesFromDirectory);

		// 3: Creates vectors of text
		System.out.println();
		System.out.println("----------------");
		System.out.println("Creating Vectors");
		System.out.println("----------------");
		System.out.println("\tInput: " + sequenceFilesDirectory);
		System.out.println("\tOutput: " + vectorDirectory);
		System.out.println();
		String[] arguments_SparseVectorsFromSequenceFiles = { "-i",
				sequenceFilesDirectory, "-o", vectorDirectory, "-x",
				exclusionThreshold, "-md", minimumDocumentFrequency };
		ToolRunner.run(new SparseVectorsFromSequenceFiles(),
				arguments_SparseVectorsFromSequenceFiles);

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new ReutersVectorizationJob(),args);
		System.exit(res);
	}
}
