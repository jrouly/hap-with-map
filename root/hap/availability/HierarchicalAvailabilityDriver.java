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
package root.hap.availability;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;


/**
 * <p>
 * This is a driver for running the Availability Update step of the 
 * Hierarchical Affinity Propagation job.
 * </p>
 * 
 * <p>
 * This algorithm does not care about its original input format; only that it 
 * receives an appropriately formatted Mahout Similarity Matrix.
 * </p>
 * 
 * <p>
 * The exact breakdown of the MapReduce implementation is discussed in the 
 * appropriate {@link AvailabilityMapper} and {@link AvailabilityReducer}.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see AvailabilityMapper
 * @see AvailabilityReducer
 * 
 */
public class HierarchicalAvailabilityDriver extends AbstractJob {

	private static String inputDirectory;
	private static String outputDirectory;
	private static String matrixN;
	private static String lambda;
	private static String numLevels;
	private static String numIteration;


	/*
	 * Construct arguments list.
	 */
	private void addArguments() {

		addOption("input", "i", "Input Directory", true);
		addOption("out", "o", "Output Directory", true);
		addOption("N", "n", "Size of Matrix (NxN)", true);
		addOption("lambda", "lambda", "Dampening Factor", true);
		addOption("numLevels", "l", "Number of Levels", true);
		addOption("numIter", "iter", "Number of Iterations", true);

	}


	/*
	 * Grab arguments from the user.
	 */
	private void initArguments() {

		inputDirectory = getOption("input");
		outputDirectory = getOption("out");
		matrixN = getOption("N");
		lambda = getOption("lambda");
		numLevels = getOption("numLevels");
		numIteration = getOption("numIter");

	}


	/**
	 * This method allows {@link HierarchicalAvailabilityDriver} to act as a 
	 * {@link ToolRunner} and interface properly with any Driver.
	 * 
	 * @param args Configuration arguments
	 * @return Exit status
	 * @see ToolRunner
	 */
	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		addArguments();

		if (parseArguments(args) == null) {
			return -1;
		}

		initArguments();

		conf.setInt("matrixN",Integer.valueOf(matrixN));
		conf.setFloat("lambda",Float.valueOf(lambda));
		conf.setInt("numLevels",Integer.valueOf(numLevels));
		conf.setInt("numIteration",Integer.valueOf(numIteration));

		Job job = new Job(conf, "HierarchicalAvailability");
		job.setJarByClass(HierarchicalAvailabilityDriver.class);

		job.setMapperClass(AvailabilityMapper.class);
		job.setReducerClass(AvailabilityReducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputDirectory));
		FileOutputFormat.setOutputPath(job, new Path(outputDirectory));

		return job.waitForCompletion(true) ? 0 : 1;

	}


	/**
	 * Redirects user input to be parsed and used as configuration values.
	 * 
	 * @param args User arguments
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run( 
				new Configuration(), 
				new HierarchicalAvailabilityDriver(), 
				args);
		System.exit(res);
	}
}