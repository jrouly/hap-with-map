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
package root.input.images;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

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
 * @since 2013.06.25
 * 
 */
public class RenameFilesJob extends InputJob {

	private static String inputDirectory;
	private static String outputDirectory;
	private static String filenameDictionaryDirectory;


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("out", "o", "Output Directory", true);
		addOption("fileDictDir", "f", "Filename Dictionary Directory", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory);
		filenameDictionaryDirectory = getOption("fileDictDir");
		filenameDictionaryDirectory = cleanDirectoryName(filenameDictionaryDirectory);
		
	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Image Rename Files Job");
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-o\t\t" + outputDirectory);
		System.out.println("\t-f\t\t" + filenameDictionaryDirectory);
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

		URI workingURI = new URI( conf.get("fs.default.name") );
		URI inputURI = new URI( inputDirectory );

		FileSystem workingFS = FileSystem.get(workingURI, conf);
		FileSystem inputFS = FileSystem.get(inputURI, conf);

		Path in = new Path(inputDirectory);
		Path docIdFile = new Path( filenameDictionaryDirectory + "file-dictionary/fileName2docId" );

		@SuppressWarnings("resource")
		SequenceFile.Writer writer = new SequenceFile.Writer(
				workingFS, 
				conf,
				docIdFile, 
				Text.class, 
				Text.class);

		FileStatus[] files = inputFS.listStatus(in);
		BufferedReader renamedReader;
		BufferedWriter renamedWriter;

		int N = files.length;
		N /= 10;

		String line = "";
		while (N > 0) {
			N /= 10;
			line += "0";
		}

		int counter = 0;

		for (FileStatus f : files) {
			Path curr = f.getPath();

			// Flag if hidden files exist in input directory
			if (curr.getName().startsWith(".")) {
				throw new Exception("Bad Data: Hidden Files Exist");
			}

			String tmpLine = line;
			int tmpCounter = counter / 10;
			while (tmpCounter > 0) {
				tmpCounter /= 10;
				tmpLine = tmpLine.substring(1);
			}

			String nextName = outputDirectory + "/" + tmpLine + counter;
			counter++;

			Path next = new Path(nextName);

			FSDataInputStream inp = inputFS.open( curr );
			renamedReader = new BufferedReader( new InputStreamReader( inp ) );

			FSDataOutputStream outp = workingFS.create( next );
			renamedWriter = new BufferedWriter( new OutputStreamWriter( outp ) );

			String copyLine;
			while( (copyLine = renamedReader.readLine()) != null ) { 
				renamedWriter.write( copyLine + "\n" );
			}

			writer.append(new Text(curr.getName()), new Text(next.getName()));

			renamedReader.close();
			renamedWriter.close();

		}

		writer.close();

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RenameFilesJob(), args);
		System.exit(res);
	}
}
