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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

import root.hap.util.KeyUtilities;


/**
 * <p>
 * Mapper class for updating availability.
 * </p>
 * 
 * <p>
 * This mapper takes in a row from the input matrices and distributes its
 * contents to the {@link AvailabilityReducer}s.
 * </p>
 * 
 * <p>
 * <code>Input key [Text]:</code> information about this row<br />
 * <code>Input value [VectorWritable]:</code> the corresponding input row
 * </p>
 * 
 * <p>
 * <code>Output key [Text]:</code> tab separated list: {Column Number, Level}
 * <br />
 * <code>Output value [Iterable&lt;Text&gt;]:</code> tab separated list: 
 * {Row Number, Level, Matrix ID, Element Value}
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see HierarchicalAvailabilityDriver
 * @see AvailabilityReducer
 * 
 */
public class AvailabilityMapper extends 
Mapper<Text, VectorWritable, Text, Text> {


	/**
	 * <p>
	 * This method takes in a vector or matrix row and outputs its elements
	 * in a fashion usable by the Reducer.
	 * </p>
	 * 
	 * @param keyIn vector identification text
	 * @param valIn vector data
	 */
	public void map(Text keyIn, VectorWritable valIn, Context context)
			throws IOException, InterruptedException {

		int numLevels = context.getConfiguration().getInt("numLevels", -1);
		int N = context.getConfiguration().getInt("matrixN", -1);

		String[] keyData = KeyUtilities.explode(keyIn, false);

		String row = keyData[KeyUtilities.INDEX];
		String level = keyData[KeyUtilities.LEVEL];
		String id = keyData[KeyUtilities.ID];

		Text keyOut, keyOutLevelAbove, valOut;

		// Begin filtering out vectors based on their Matrix ID.
		if (id.equals("A")) {  // Availability vectors

			double elementValue;

			for (int col = 0; col < N; col++) {

				elementValue = valIn.get().get( col );

				keyOut = new Text();
				keyOut.set(col + "\t" + level);

				valOut = new Text();
				valOut.set(row + "\t" + level + "\t" + id + "\t" + elementValue);

				context.write(keyOut, valOut);
			}

		} else if (id.equals("R")) {  // Responsibility vectors

			double elementValue;

			for (int col = 0; col < N; col++) {

				elementValue = valIn.get().get(col);

				keyOut = new Text();
				keyOut.set(col + "\t" + level);

				valOut = new Text();
				valOut.set(row + "\t" + level + "\t" + id + "\t" + elementValue);

				context.write(keyOut, valOut);
			}

			int levelAbove = Integer.valueOf(level) + 1;

			if (levelAbove < numLevels) {  // Second level of Responsibility

				for (int col = 0; col < N; col++) {

					elementValue = valIn.get().get(col);

					keyOutLevelAbove = new Text();
					keyOutLevelAbove.set(col + "\t" + levelAbove);

					valOut = new Text();
					valOut.set(row + "\t" + level + "\t" + id + "\t"
							+ elementValue);

					context.write(keyOutLevelAbove, valOut);
				}
			}
		} else if (id.equals("S")) {  // Similarity vectors

			double elementValue;

			for (int i = 0; i < numLevels; i++) {
				for (int col = 0; col < N; col++) {

					elementValue = valIn.get().get(col);

					keyOut = new Text();
					keyOut.set(col + "\t" + i);

					valOut = new Text();
					valOut.set(row + "\t" + i + "\t" + id + "\t" + elementValue);

					context.write(keyOut, valOut);
				}
			}
		} else if (id.equals("T") || id.equals("P")) {  // 1D Tau and Phi vectors
			double elementValue = valIn.get().get(0);

			keyOut = new Text();
			keyOut.set(row + "\t" + level);
			valOut = new Text();

			valOut.set(row + "\t" + level + "\t" + id + "\t" + elementValue);

			context.write(keyOut, valOut);
		} else if (id.equals("C")) {  // Exemplar Preference vector
			double elementValue = valIn.get().get(0);

			keyOut = new Text();
			keyOut.set(row + "\t" + level);

			valOut = new Text();
			valOut.set(row + "\t" + level + "\t" + id + "\t" + elementValue);

			context.write(keyOut, valOut);

			int levelAbove = Integer.valueOf(level) + 1;

			if (levelAbove < numLevels) {
				elementValue = valIn.get().get(0);

				keyOutLevelAbove = new Text();
				keyOutLevelAbove.set(row + "\t" + levelAbove);

				valOut = new Text();
				valOut.set(row + "\t" + level + "\t" + id + "\t" + elementValue);

				context.write(keyOutLevelAbove, valOut);
			}
		}


		keyIn = null;
		valIn = null;
		System.gc();
	}
}