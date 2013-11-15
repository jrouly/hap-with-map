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

import root.input.InputJob;


/**
 * <p>
 * This class takes an output directory storing the word dictionary file and
 * formats it in a usable fashion.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class FormatWordDictJob extends InputJob {

	private static String inputDirectory;
	private static String outputDirectory;


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("out", "o", "Output Directory", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory)+"/word-dictionary";

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Lyrl2004 Format Word Dictionary Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-o\t\t" + outputDirectory);
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

		URI workingURI = new URI(outputDirectory);
		URI inputURI = new URI(inputDirectory);

		FileSystem workingFS = FileSystem.get(workingURI, conf);
		FileSystem inputFS = FileSystem.get(inputURI, conf);

		Path in = new Path(inputDirectory);
		Path wordDictFile = new Path(outputDirectory);

		@SuppressWarnings("resource")
		SequenceFile.Writer wordDictWriter = new SequenceFile.Writer(workingFS, conf,
				wordDictFile, Text.class, Text.class);

		FileStatus[] files = inputFS.listStatus(in);

		for (FileStatus f : files) {
			Path curr = f.getPath();
			if (curr.getName().startsWith(".")) {
				throw new Exception("Bad Data: Hidden Files Exist");
			}

			Scanner sc = new Scanner(new BufferedReader(new InputStreamReader(
					inputFS.open(curr))));

			while (sc.hasNext()) {
				String line= sc.nextLine();
				String [] data=line.split(" ");
				Text text = new Text (data[0]+"\t"+data[1]);
				wordDictWriter.append(text, text);

			}
			
			sc.close();
		}

		wordDictWriter.close();

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new FormatWordDictJob(),args);
		System.exit(res);
	}
}
