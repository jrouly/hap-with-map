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
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;


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
 * @deprecated
 * 
 */
public class FormatVectorsJob extends AbstractJob {

	private static String inputDirectory;
	private static String vectorDirectory;
	private static String fileDictDirectory;


	/*
	 * Construct arguments list.
	 */
	private void addArguments() {

		addOption("input", "i", "Input Directory", true);
		addOption("vector", "v", "Output Directory", true);
		addOption("fileDict", "f", "Output Directory", true);

	}


	/*
	 * Grab arguments from the user.
	 */
	private void initArguments() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		vectorDirectory = getOption("vector");
		vectorDirectory = cleanDirectoryName(vectorDirectory);
		fileDictDirectory = getOption("fileDict");
		fileDictDirectory = cleanDirectoryName(fileDictDirectory);

	}


	/*
	 * Clean a directory name to remove initial '/' characters.
	 */
	private static String cleanDirectoryName(String dirName) {
		if (dirName.charAt(dirName.length() - 1) == '/') {
			dirName = dirName.substring(0, dirName.length() - 1);
		}
		return dirName;
	}


	/**
	 * This method allows the Job to act as a {@link ToolRunner} and 
	 * interface properly with the Driver.
	 * 
	 * @param args Configuration arguments
	 * @return Exit status
	 * @see ToolRunner
	 */
	@Override
	public int run(String[] args) throws Exception {

		addArguments();

		if (parseArguments(args) == null) {
			return -1;
		}

		initArguments();

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

				String line = sc.nextLine();

				RandomAccessSparseVector vector = new RandomAccessSparseVector(
						10000);

				String[] split=line.split(",");

				double val1 = Double.valueOf(split[0]);
				double val2 = Double.valueOf(split[1]);
				int val3 = Integer.valueOf(split[2]);

				vector.setQuick(0, val1);
				vector.setQuick(1, val2);

				String nextName = counter + "";
				String nextFileName = "/" + counter;
				counter++;

				VectorWritable vec = new VectorWritable();
				vec.set(vector);
				vectorWriter.append(new Text(nextFileName), vec);

				String point = "{x:"+val1+",y:"+val2+",cluster:"+val3+"}";

				metadataWriter.append(new Text(point), new Text(nextName));


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
