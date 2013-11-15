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
package root.input.lyrl2004;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import root.input.InputJob;


/**
 * <p>
 * This class takes an input directory storing vectorized data files and
 * formats them in a usable fashion.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class FormatVectorsJob extends InputJob {

	private static String inputDirectory;
	private static String vectorDirectory;
	private static String fileDictDirectory;


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("vector", "v", "Output Directory", true);
		addOption("fileDict", "f", "Output Directory", true);

	}


	/*
	 * Grab arguments from the user.
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		vectorDirectory = getOption("vector");
		vectorDirectory = cleanDirectoryName(vectorDirectory);
		fileDictDirectory = getOption("fileDict");
		fileDictDirectory = cleanDirectoryName(fileDictDirectory);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Lyrl2004 Format Vectors Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-v\t\t" + vectorDirectory);
		System.out.println("\t-f\t\t" + fileDictDirectory);
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

		Path in = new Path(inputDirectory);
		Path docIdFile = new Path(fileDictDirectory+"/vectorName2docId");
		Path vectorFile = new Path(vectorDirectory+"/part-r-00000");

		@SuppressWarnings("resource")
		SequenceFile.Writer metadataWriter = new SequenceFile.Writer(workingFS, conf,
				docIdFile, Text.class, Text.class);

		@SuppressWarnings("resource")
		SequenceFile.Writer vectorWriter = new SequenceFile.Writer(workingFS, conf,
				vectorFile, Text.class, VectorWritable.class);

		FileStatus[] files = inputFS.listStatus(in);

		int counter = 0;

		for (FileStatus f : files) {
			Path curr = f.getPath();
			if (curr.getName().startsWith(".")) {
				throw new Exception("Bad Data: Hidden Files Exist");
			}

			Scanner sc = new Scanner(new BufferedReader(new InputStreamReader(
					inputFS.open(curr))));

			while (sc.hasNext()) {

				String key = sc.next();

				RandomAccessSparseVector vector = new RandomAccessSparseVector(
						10000);

				String line = sc.nextLine().trim();
				Scanner lineScanner = new Scanner(line);
				while (lineScanner.hasNext()) {

					String pair = lineScanner.next();

					int k = Integer.valueOf(pair.split(":")[0]);
					double v = Double.valueOf(pair.split(":")[1]);

					vector.setQuick(k, v);

				}

				String nextName = counter + "";
				String nextFileName = "/" + counter;
				counter++;

				VectorWritable vec = new VectorWritable();
				vec.set(vector);
				vectorWriter.append(new Text(nextFileName), vec);

				metadataWriter.append(new Text(key), new Text(nextName));

				lineScanner.close();

			}

			sc.close();
		}

		metadataWriter.close();
		vectorWriter.close();

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FormatVectorsJob(),
				args);
		System.exit(res);
	}

}
