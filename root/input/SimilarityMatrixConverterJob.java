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
package root.input;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;


/**
 * <p>
 * This class takes an input directory of texts files and renames them with
 * docIds (1-N) A file with mapping from filename to docId is created
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public class SimilarityMatrixConverterJob extends InputJob {

	private static String inputSimMat;
	private static String outputDirectory;
	private static String levels;
	private static final double TAU_INIT = Double.POSITIVE_INFINITY;


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Similarity Matrix", true);
		addOption("out", "o", "Output Directory", true);
		addOption("levels", "l", "Levels", true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputSimMat = getOption("input");
		inputSimMat = cleanDirectoryName(inputSimMat);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory);
		levels = getOption("levels");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() {
		System.out.println("[INFO]: Similarity Matrix Converter");
		System.out.println("\t-i\t\t" + inputSimMat);
		System.out.println("\t-o\t\t" + outputDirectory);
		System.out.println("\t-l\t\t" + levels);
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

		int numLevels = Integer.valueOf(levels);


		URI workingURI = new URI( conf.get("fs.default.name") );
		URI inputURI = new URI( inputSimMat );

		FileSystem workingFS = FileSystem.get(workingURI, conf);
		FileSystem inputFS = FileSystem.get(inputURI, conf);

		Path in = new Path(inputSimMat);
		Path out = new Path(outputDirectory + "/part-m-00000");

		Scanner reader = new Scanner( new BufferedReader( new InputStreamReader( inputFS.open( in ) ) ) );

		SequenceFile.Writer writer = new SequenceFile.Writer(
				workingFS, 
				conf,
				out, 
				Text.class, 
				VectorWritable.class);


		int counter=0;

		int N=0;

		Text outSKey = new Text();
		Text outRKey = new Text();
		Text outAKey = new Text();
		Text outCKey = new Text();
		Text outTKey = new Text();
		Text outPKey = new Text();



		while(reader.hasNext()) { 

			String line=reader.nextLine();
			String [] vecLine = line.split(",");
			N = vecLine.length;
			double [] vecVal = new double [N];

			for(int i=0 ; i< N ; i++ ){
				vecVal[i] = Double.valueOf(vecLine[i]);
			}

			DenseVector vectorS = new DenseVector ( vecVal );
			DenseVector out0VecN = new DenseVector ( N );
			out0VecN.assign(0.0);
			DenseVector out0Vec1 = new DenseVector(1);
			out0Vec1.assign(0.0);
			DenseVector outTVec = new DenseVector(1);
			outTVec.assign( TAU_INIT );

			VectorWritable vectorWritable = new VectorWritable ( vectorS );
			VectorWritable out0VecNWritable = new VectorWritable(out0VecN);
			VectorWritable out0Vec1Writable = new VectorWritable(out0Vec1);
			VectorWritable outTVecWritable = new VectorWritable(outTVec);

			outSKey = new Text(counter+"\t0\tS");
			writer.append(outSKey, vectorWritable);


			for (int i = 0; i < numLevels; i++) {
				outRKey = new Text(counter + "\t" + i + "\tR");
				outAKey = new Text(counter + "\t" + i + "\tA");
				writer.append(outRKey, out0VecNWritable);
				writer.append(outAKey, out0VecNWritable);
			}


			for (int i = 0; i < numLevels; i++) {
				outCKey = new Text(counter + "\t" + i + "\tC");
				outTKey = new Text(counter + "\t" + i + "\tT");
				outPKey = new Text(counter + "\t" + i + "\tP");
				writer.append(outCKey, out0Vec1Writable);
				writer.append(outTKey, outTVecWritable);
				writer.append(outPKey, out0Vec1Writable);
			}




			counter++;

		}


		reader.close();
		writer.close();

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SimilarityMatrixConverterJob(), args);
		System.exit(res);
	}
}
