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
package root.hap.cluster;

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


/**
 * <p>
 * This is a driver for running the Cluster Extraction step of the 
 * Hierarchical Affinity Propagation job.
 * </p>
 * 
 * <p>
 * This algorithm takes the final Responsibility and Availability matrices 
 * from the Hierarchical Affinity Propagation algorithm and extracts the 
 * selected clusters for display.
 * </p>
 * 
 * <p>
 * The exact breakdown of the MapReduce implementation is discussed in the 
 * appropriate {@link ClusterMapper} and {@link ClusterReducer}.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see ClusterMapper
 * @see ClusterReducer
 * 
 */
public class HierarchicalClusterDriver extends AbstractJob {

	private static String inputDirectory;
	private static String outputDirectory;
	private static String matrixN;


	/*
	 * Construct arguments list.
	 */
	private void addArguments() {

		addOption("input", "i", "Input Directory", true);
		addOption("out", "o", "Output Directory", true);
		addOption("N", "n", "Size of Matrix (NxN)", true);

	}


	/*
	 * Grab arguments from the user.
	 */
	private void initArguments() {

		inputDirectory = getOption("input");
		outputDirectory = getOption("out");
		matrixN = getOption("N");

	}


	/**
	 * This method allows {@link HierarchicalClusterDriver} to act as a 
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

		Job job = new Job(conf, "HierarchicalResponsibility");
		job.setJarByClass(HierarchicalClusterDriver.class);

		job.setMapperClass(ClusterMapper.class);
		job.setReducerClass(ClusterReducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

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
				new HierarchicalClusterDriver(), 
				args);
		System.exit(res);
	}
}