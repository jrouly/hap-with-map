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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import root.input.InputJob;


/**
 * <p>
 * This class takes an input directory of image files and renames them with
 * docIds (1-N) A file with mapping from filename to docId is created
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.04
 * 
 */
public class FormatPointsJob extends InputJob {

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


	/**
	 * {@inheritDoc}
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
		System.out.println("[INFO]: Format Images Job");
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

				String line = sc.nextLine();

				DenseVector vector = new DenseVector(3);

				String[] split=line.split(",");

				double x = Double.valueOf(split[0]);
				double y = Double.valueOf(split[1]);
//				int cluster = Integer.valueOf(split[2]);
				vector.setQuick(0, x);
				vector.setQuick(1, y);

				String nextName = counter + "";
				String nextFileName = "/" + counter;
				counter++;

				VectorWritable vec = new VectorWritable();
				vec.set(vector);
				vectorWriter.append(new Text(nextFileName), vec);

				String point = String.format("%.2f-%.2f",x,y);

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
		int res = ToolRunner.run(new Configuration(), new FormatPointsJob(),
				args);
		System.exit(res);
	}

}
