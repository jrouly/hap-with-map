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
package root.input.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import root.input.InputJob;


/**
 * <p>
 * Create similarity matrix.
 * </p>
 * 
 * <p>
 * Takes input directory of vectors and creates similarity matrix.
 * </p>
 * 
 * <p>
 * <code>Input key [Text]:</code> column number<br />
 * <code>Input value [VectorWritable]:</code> the corresponding vector
 * </p>
 * 
 * <p>
 * <code>Output key [VectorWritable]:</code> tab separated list: {Column Number,
 * LevelNumber, ID}<br />
 * <code>Output value [VectorWritable]:</code> {Row Number : Row Value, 
 * Row Number : Row Value,...}
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see SimilarityMatrixMapper
 * @see CreateSeedVector
 * @see DenseVector
 * 
 */
public class CreateSimilarityMatrixJob extends InputJob {

	public static final String SEEDS = "seeds";
	public static final String SEEDS_PATH_KEY = "seedsPath";
	public static final String DISTANCE_MEASURE_KEY = "vectorDistSim.measure";
	public static final String OUT_TYPE_KEY = "outType";

	public static String inputDirectory;
	public static String outputDirectory;
	public static String distanceMeasure;
	public static String numLevels;
	public static String diagScale;


	/**
	 * {@inheritDoc}
	 */
	protected void constructParameterList() {

		addOption("input", "i", "Input Directory", true);
		addOption("out", "o", "Output Directory", true);
		addOption("distance", "dm", "Distance Measure", true);
		addOption("numLevels","l","Number of Levels",true);
		addOption("diagScale","smd","Similarity Matrix seed scale",true);

	}


	/**
	 * {@inheritDoc}
	 */
	protected void initializeConfigurationParameters() {

		inputDirectory = getOption("input");
		inputDirectory = cleanDirectoryName(inputDirectory);
		outputDirectory = getOption("out");
		outputDirectory = cleanDirectoryName(outputDirectory);
		distanceMeasure = getOption("distance");
		numLevels = getOption("numLevels");
		diagScale = getOption("diagScale");

	}


	/**
	 * {@inheritDoc}
	 */
	protected void printConfiguredParameters() { 
		System.out.println("[INFO]: Create Similarity Matrix Job" );
		System.out.println("\t-i\t\t" + inputDirectory);
		System.out.println("\t-o\t\t" + outputDirectory);
		System.out.println("\t-dm\t\t" + distanceMeasure);
		System.out.println("\t-l\t\t" + numLevels);
		System.out.println("\t-smd\t\t" + diagScale);
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

		DistanceMeasure measure = ClassUtils.instantiateAs(distanceMeasure,
				DistanceMeasure.class);

		conf.setInt("numLevels", Integer.valueOf(numLevels));
		conf.setLong("diagScale", Long.valueOf(diagScale));
		conf.set(DISTANCE_MEASURE_KEY, measure.getClass().getName());
		conf.set(SEEDS_PATH_KEY, inputDirectory.toString());

		Job job = new Job(conf, "CreateSimilarityMatrix: " + inputDirectory);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapperClass(SimilarityMatrixMapper.class);

		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(inputDirectory));
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

		job.setJarByClass(CreateSimilarityMatrixJob.class);

		if (!job.waitForCompletion(true)) {
			throw new IllegalStateException(
					"CreateSimilarityMatrix failed processing " + inputDirectory);
		}

		return 0;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CreateSimilarityMatrixJob(),
				args);
		System.exit(res);
	}

}
