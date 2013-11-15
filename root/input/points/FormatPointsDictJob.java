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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import root.input.InputJob;


/**
 * <p>
 * This class takes an output directory storing the image dictionary file and
 * formats it in a usable fashion.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class FormatPointsDictJob extends InputJob {

	private static String outputDirectory;


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("out", "o", "Output Directory", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory)+"/word-dictionary";

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Format Image Dictionary Job");
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
		URI fsURI = new URI( outputDirectory ); 
		FileSystem fs = FileSystem.get(fsURI, conf);

		Path wordDictFile = new Path(outputDirectory);

		SequenceFile.Writer wordDictWriter = new SequenceFile.Writer(fs, conf,
				wordDictFile, Text.class, Text.class);


		Text x = new Text ("x\t0");
		wordDictWriter.append(new Text(), x);

		Text y = new Text ("y\t1");
		wordDictWriter.append(new Text(), y);

		Text cluster = new Text ("cluster\t1");
		wordDictWriter.append(new Text(), cluster);

		wordDictWriter.close();

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new FormatPointsDictJob(),args);
		System.exit(res);
	}
}
