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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;


/**
 * <p>
 * Create list of seed vectors for use in construction of similarity matrix.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see CreateSimilarityMatrixJob
 * @see SimilarityMatrixMapper
 * 
 */
final class CreateSeedVector {


	/**
	 * Load in the seed vectors which will be used for the similarity matrix.
	 * 
	 * @param conf configuration file
	 * @return list of configured seed vectors
	 */
	public static List<NamedVector> loadSeedVectors(Configuration conf)
			throws IOException, URISyntaxException {

		String seedPathStr = conf.get(CreateSimilarityMatrixJob.SEEDS_PATH_KEY);
		if (seedPathStr == null || seedPathStr.isEmpty()) {
			return Collections.emptyList();
		}

		List<NamedVector> seedVectors = Lists.newArrayList();

		Path seedsDirPath = new Path(seedPathStr + "/part-r-00000");

		URI uri = new URI( conf.get("fs.default.name") );
		FileSystem fs = FileSystem.get(uri, conf);

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, seedsDirPath,
				conf);

		Text key = new Text();
		VectorWritable value = new VectorWritable();

		while (reader.next(key, value)) {
			VectorWritable vw = (VectorWritable) value;
			Vector vector = vw.get();
			seedVectors.add(new NamedVector(vector, key.toString()));
		}

		reader.close();

		if (seedVectors.isEmpty()) {
			throw new IllegalStateException("No seeds found. Check your path: "
					+ seedPathStr);
		}

		return seedVectors;
	}

}
