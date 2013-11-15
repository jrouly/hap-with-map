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

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;

import root.hap.util.KeyUtilities;


/**
 * <p>
 * Reducer class for extracting clusters.
 * </p>
 * 
 * <p>
 * This reducer takes in a set of rows and diagonals from the HAP algorithm 
 * generated matrices and uses their contents to extract cluster descriptions.
 * </p>
 * 
 * <p>
 * <code>Input key [Text]:</code> information about these rows<br />
 * <code>Input value [Iterable&lt;Text&gt;]:</code> the corresponding rows
 * </p>
 * 
 * <p>
 * <code>Output key [Text]:</code> ignored, not used<br />
 * <code>Output value [VectorWritable]:</code> tab separated list: 
 * {Exemplar ID, Vector ID, Level}
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @see HierarchicalClusterDriver
 * @see ClusterReducer
 * 
 */
public class ClusterReducer extends Reducer<Text, Text, Text, Text> {


	/**
	 * <p>
	 * This method serves to extract cluster data by accessing a
	 * single row and the diagonal of each matrix. Diagonal values are used
	 * to determine if the exemplar is in a valid location (specifically if 
	 * the diagonal value is positive).
	 * </p>
	 * 
	 * @param keyIn vector identification block
	 * @param valIn vector data
	 */
	public void reduce(Text keyIn, Iterable<Text> valIn, Context context)
			throws IOException, InterruptedException {

		int N = context.getConfiguration().getInt("matrixN", -1);

		// data structures to reconstruct the rows we're working on
		DenseVector A = new DenseVector( N );
		DenseVector diagA = new DenseVector(N);
		DenseVector R = new DenseVector( N );
		DenseVector diagR = new DenseVector(N);

		int reducerRowNum = Integer.valueOf(keyIn.toString().split("\t")[0]);
		int reducerLevelNum = Integer.valueOf(keyIn.toString().split("\t")[1]);

		for (Text text : valIn) {
			// **** important, do not remove this conversion ****
			// we must do this for some reason..not sure why.
			// if we don't the loop doesn't iterate properly.
			// **** important, do not remove this conversion ****
			text = new Text(text.toString());

			String[] textData = KeyUtilities.explode( text, true );
			String id = textData[ KeyUtilities.ID ];
			String row = textData[ KeyUtilities.INDEX ];
			String value = textData[ KeyUtilities.VALUE ];

			int rowInt = Integer.valueOf( row );
			double valDouble = Double.valueOf( value );

			switch( id.charAt( 0 ) ) { 
			case 'R':
				R.setQuick( rowInt, valDouble );
				break;
			case 'r':
				diagR.setQuick( rowInt, valDouble );
				break;
			case 'A':
				A.setQuick( rowInt, valDouble );
				break;
			case 'a':
				diagA.setQuick( rowInt, valDouble );
				break;
			default:
				System.err.println("[ERROR]: Invalid matrix ID.");
				System.exit( 1 );
				break;
			}

		}

		//		printInput( A, diagA, R, diagR );

		updateExemplars( context, A, diagA, R, diagR, reducerRowNum, reducerLevelNum, N );


		keyIn = null;
		valIn = null;
		A = null;
		diagA = null;
		R = null;
		diagR = null;
		System.gc();
	}


	//	private void printInput( DenseVector A, DenseVector diagA, DenseVector R, DenseVector diagR ) {
	//		System.out.println( A );
	// 		System.out.println( diagA );
	//		System.out.println( R );
	//		System.out.println( diagR );
	//
	//		System.out.println("------------");
	//	}


	/*
	 * ALGORITHM: Update Exemplars C(i,l) = argmax [ A(i,j,l) + R(i,j,l) ] where
	 * A(j,j,l) + R(i,j,l) > 0 Find the maximum of A+R keeping row constant
	 * varying column for level equal level to lower C level
	 */
	private void updateExemplars(Context context, DenseVector A, DenseVector diagA, DenseVector R,
			DenseVector diagR, int reducerRowNum, int reducerLevelNum, int N ) 
					throws IOException, InterruptedException {

		boolean [] validExemplars = new boolean [N];
		boolean validExist = false;
		Arrays.fill(validExemplars, false);

		for ( int diagIter = 0; diagIter < N ; diagIter++ ){
			double diagVal = diagA.get( diagIter ) + diagR.get( diagIter );
			if( diagVal > 0 ){
				validExemplars[diagIter] = true;
				validExist = true;
			}
		}

		DenseVector sum = (DenseVector) A.plus(R);
		double maxValue = Double.NEGATIVE_INFINITY;
		int maxValueIndex = sum.maxValueIndex();

		if( validExist ){

			for ( int diagIter = 0; diagIter < N ; diagIter++ ){

				double validValue = sum.get( diagIter );
				if( validExemplars [diagIter] && validValue > maxValue){
					maxValue = validValue;
					maxValueIndex = diagIter;
				}

			}

		}else{

			maxValue = sum.maxValue();
			maxValueIndex = sum.maxValueIndex();

		}

		Text output = new Text( maxValueIndex + "\t" + reducerRowNum + "\t"
				+ reducerLevelNum );

		// Sentinel: In case an invalid exemplar ID is passed to output.
		if (maxValueIndex != -1) {
			context.write(new Text(),output);
		}
	}

}
