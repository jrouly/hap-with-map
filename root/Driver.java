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
package root;

import org.apache.hadoop.util.ProgramDriver;

import root.benchmark.ReutersBenchmarkJob;
import root.hap.HierarchicalAffinityPropagationJob;
import root.input.images.ImagesJob;
import root.input.lyrl2004.LyrlJob;
import root.input.points.PointsJob;
import root.input.reuters21578.ReutersJob;
import root.output.OutputJob;


/**
 * <p>
 * This is the main executable Driver class which controls all paths of 
 * execution and user interaction.
 * </p>
 * 
 * <p>
 * This class acts as a shell or wrapper around the software project, directing
 * user inquiry to an appropriate sub-driver class or job.
 * </p>
 * 
 * <p>
 * To run this program from the command line:<br/>
 * <code>$ bin/hadoop root.Driver directive options</code>
 * </p>
 * 
 * <p>
 * Include (at minimum) the following external libraries on the classpath: <br />
 * <code>
 * 	lib/run/*.jar<br/>
 * 	lib/build/*.jar<br/>
 * </code>
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see HierarchicalAffinityPropagationJob
 * @see OutputJob
 * @see LyrlJob
 * @see PointsJob
 * @see ReutersJob
 * 
 */
public class Driver {

	/**
	 * Redirects user input to the appropriate input driver class.
	 * 
	 * @param args User arguments
	 */
	public static void main(String args[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			
			pgd.addClass("hap", HierarchicalAffinityPropagationJob.class,
					"Run Hierarchical Affinity Propagation on an existing Similarity Matrix");
//			pgd.addClass("hive", OutputJob.class,
//					"Export data to the visualizations database");
			pgd.addClass("lyrl", LyrlJob.class,
					"Analyze the Lyrl input dataset");
//			pgd.addClass("points", PointsJob.class,
//					"Analyze the Gaussian Points input dataset");
			pgd.addClass("reuters", ReutersJob.class,
					"Analyze the Reuters input dataset");
			pgd.addClass("images", ImagesJob.class,
					"Analyze an image input dataset");
			pgd.addClass("reuters-benchmark", ReutersBenchmarkJob.class,
					"Run a KMeans benchmark on the Reuters input dataset.");
			pgd.addClass("points", PointsJob.class,
					"Run a points.");
			pgd.driver(args);

			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}
