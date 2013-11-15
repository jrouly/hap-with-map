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
package root.hap.responsibility;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

import root.hap.util.KeyUtilities;


/**
 * <p>
 * Mapper class for updating responsibility.
 * </p>
 * 
 * <p>
 * This mapper takes in a column from the input matrices and distributes its
 * contents to the {@link ResponsibilityReducer}s.
 * </p>
 * 
 * <p>
 * <code>Input key [Text]:</code> information about this column<br />
 * <code>Input value [VectorWritable]:</code> the corresponding input column
 * </p>
 * 
 * <p>
 * <code>Output key [Text]:</code> tab separated list: {Row Number, Level}
 * <br />
 * <code>Output value [Iterable&lt;Text&gt;]:</code> tab separated list: 
 * {Column Number, Level, Matrix ID, Element Value}
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see HierarchicalResponsibilityDriver
 * @see ResponsibilityReducer
 * 
 */
public class ResponsibilityMapper extends
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

		int N = context.getConfiguration().getInt("matrixN", -1);
		int numLevels = context.getConfiguration().getInt("numLevels", -1);

		String[] keyData = KeyUtilities.explode( keyIn , false );

		String col = keyData[KeyUtilities.INDEX];
		String level = keyData[KeyUtilities.LEVEL];
		String id = keyData[KeyUtilities.ID];

		int colInt= Integer.valueOf(col);
		String diagID = id.toLowerCase();
		
		Text keyOut,keyOutLevelBelow,valOut;

		if( id.equals("A") ) { 
			
			double elementValue = valIn.get().get(colInt);

			for (int diagIter = 0; diagIter < N; diagIter++) {

				keyOut = new Text();
				keyOut.set(diagIter + "\t" + level);
				valOut = new Text();

				valOut.set(colInt + "\t" + level + "\t" + diagID + "\t"
						+ elementValue);

				context.write(keyOut, valOut);
			}
			
			for( int row = 0; row < N; row++ ) { 
				elementValue = valIn.get().get( row );

				keyOut = new Text();
				keyOut.set(row + "\t" + level);

				valOut = new Text();
				valOut.set(col + "\t" + level + "\t" + id
						+ "\t" + elementValue);
				context.write(keyOut, valOut);

			}

			int levelBelow = Integer.valueOf(level) - 1;

			if (levelBelow >= 0) {

				for( int row = 0; row < N; row++ ) { 
					
					elementValue = valIn.get().get( row );

					keyOutLevelBelow = new Text();
					keyOutLevelBelow.set(row + "\t" + levelBelow);

					valOut = new Text();
					valOut.set(col + "\t" + level + "\t"
							+ id + "\t" + elementValue);

					context.write(keyOutLevelBelow, valOut);
				}
			}
		} else if( id.equals("R") ) { 
			
			double elementValue = valIn.get().get(colInt);

			for (int diagIter = 0; diagIter < N; diagIter++) {

				keyOut = new Text();
				keyOut.set(diagIter + "\t" + level);
				valOut = new Text();

				valOut.set(colInt + "\t" + level + "\t" + diagID + "\t"
						+ elementValue);

				context.write(keyOut, valOut);
			}
			
			for( int row = 0; row < N; row++ ) { 
				
				elementValue = valIn.get().get( row );

				keyOut = new Text();
				keyOut.set(row + "\t" + level);

				valOut = new Text();
				valOut.set(col + "\t" + level + "\t" + id
						+ "\t" + elementValue);

				context.write(keyOut, valOut);
			}
			
		} else if( id.equals("S") ) { 
			
			double elementValue;
			
			for (int i = 0; i < numLevels; i++) {
				for( int row = 0; row < N; row++ ) { 
					
					elementValue = valIn.get().get( row );

					keyOut = new Text();
					keyOut.set(row + "\t" + i );

					valOut = new Text();
					valOut.set(col + "\t" + i + "\t" + id + "\t"
							+ elementValue);

					context.write(keyOut, valOut);
					
				}
			}
		} else if( id.equals("T") || id.equals("P") || id.equals("C") ) { 
			double elementValue = valIn.get().get( 0 );
		
			keyOut = new Text();
			keyOut.set(col + "\t" + level);
			valOut = new Text();

			valOut.set(col + "\t" + level + "\t" + id + "\t"
					+ elementValue);

			context.write(keyOut, valOut);
		}


		keyIn = null;
		valIn = null;
		System.gc();
	}
}