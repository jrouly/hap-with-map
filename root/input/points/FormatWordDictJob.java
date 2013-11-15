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
import org.apache.mahout.common.AbstractJob;


/**
 * <p>
 * This class takes an output directory storing the points dictionary file and
 * formats it in a usable fashion.
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
public class FormatWordDictJob extends AbstractJob {

	private static String outputDirectory;


	/*
	 * Construct arguments list.
	 */
	private void addArguments() {

		addOption("out", "o", "Output Directory", true);

	}


	/*
	 * Grab arguments from the user.
	 */
	private void initArguments() {

		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory)+"/word-dictionary";

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
		URI fsURI = new URI( outputDirectory ); 
		FileSystem fs = FileSystem.get(fsURI, conf);

		Path wordDictFile = new Path(outputDirectory);

		SequenceFile.Writer wordDictWriter = new SequenceFile.Writer(fs, conf,
				wordDictFile, Text.class, Text.class);


		Text x = new Text ("x\t0");
		wordDictWriter.append(x, x);
		
		Text y = new Text ("y\t1");
		wordDictWriter.append(y, y);

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
