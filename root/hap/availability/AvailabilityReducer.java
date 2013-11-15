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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import root.hap.util.KeyUtilities;


/**
 * <p>
 * Reducer class for updating availability.
 * </p>
 * 
 * <p>
 * This reducer takes in a set of rows from the input matrices and uses their
 * contents to update the Availability matrix.
 * </p>
 * 
 * <p>
 * <code>Input key [Text]:</code> information about these rows<br />
 * <code>Input value [Iterable&lt;Text&gt;]:</code> the corresponding input rows
 * </p>
 * 
 * <p>
 * <code>Output key [Text]:</code> tab separated list: {Column Number, Level, 
 * Matrix ID}
 * <br />
 * <code>Output value [VectorWritable]:</code> vector of appropriate data
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
public class AvailabilityReducer extends
Reducer<Text, Text, Text, VectorWritable> {


	/**
	 * <p>
	 * This method serves to update the availability matrix by accessing a
	 * single row of each matrix.
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
		DenseVector R = new DenseVector( N );
		DenseVector RLevelBelow = new DenseVector( N );
		DenseVector S = new DenseVector( N );
		DenseVector T = new DenseVector( 1 );
		DenseVector P = new DenseVector( 1 );
		DenseVector C = new DenseVector( 1 );
		DenseVector CLevelBelow = new DenseVector( 1 );

		int reducerColNum = Integer.valueOf(keyIn.toString().split("\t")[0]);
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
			String level = textData[ KeyUtilities.LEVEL ];
			String value = textData[ KeyUtilities.VALUE ];

			int rowInt = Integer.valueOf( row );
			double valDouble = Double.valueOf( value );
			int levelInt = Integer.valueOf( level );

			switch( id.charAt( 0 ) ) { 
			case 'R':
				if( reducerLevelNum == levelInt ) { 
					R.setQuick( rowInt, valDouble );
				} else { 
					RLevelBelow.setQuick( rowInt, valDouble );
				}
				break;
			case 'A':
				A.setQuick( rowInt, valDouble );
				break;
			case 'S':
				S.setQuick( rowInt, valDouble );
				break;
			case 'C':
				if( reducerLevelNum == levelInt ) { 
					C.setQuick( 0, valDouble );
				} else { 
					CLevelBelow.setQuick( 0, valDouble );
				}
				break;
			case 'T':
				T.setQuick( 0, valDouble );
				break;
			case 'P':
				P.setQuick( 0, valDouble );
				break;
			default:
				System.err.println("[ERROR]: Invalid matrix ID.");
				System.exit( 1 );
				break;
			}

		}

		//		printInput(A, R, RLevelBelow, S, T, P, C, CLevelBelow);

		outputPhi(context, P, reducerColNum, reducerLevelNum, "P");

		outputExemplars(context, C, reducerColNum, reducerLevelNum, "C");

		if (reducerLevelNum != 0) {

			updateTau(context, RLevelBelow, CLevelBelow, T, reducerColNum,
					reducerLevelNum, N, "T");

		} else {
			outputTau(context, T, reducerColNum, reducerLevelNum, "T");
		}

		if( reducerLevelNum == 0 ) { 
			outputSimilarity(context, S, reducerColNum, reducerLevelNum, "S");
		}

		outputResponsibility(context, R, reducerColNum, reducerLevelNum, "R");

		updateAvailability(context, A, R, S, P, C, reducerColNum,
				reducerLevelNum, N, "A");


		keyIn = null;
		valIn = null;
		A = null;
		R = null;
		RLevelBelow = null;
		S = null;
		T = null;
		P = null;
		C = null;
		CLevelBelow = null;
		System.gc();
	}


	//	private void printInput( DenseVector A, DenseVector R, DenseVector RLevelBelow,
	//			DenseVector S, DenseVector T, DenseVector P, DenseVector C,
	//			DenseVector CLevelBelow ) { 
	//
	//		System.out.println( A );
	//		System.out.println( R );
	//		System.out.println( RLevelBelow );
	//		System.out.println( S );
	//		System.out.println( T );
	//		System.out.println( P );
	//		System.out.println( C );
	//		System.out.println( CLevelBelow );
	//
	//		System.out.println("------------");
	//	}


	/*
	 * Output Phi directly.
	 */
	private void outputPhi(Context context, DenseVector P, int reducerColNum,
			int reducerLevelNum, String phi) throws IOException,
			InterruptedException {

		VectorWritable PWritable = new VectorWritable(P);

		context.write(new Text(reducerColNum + "\t" + reducerLevelNum + "\t"
				+ phi), PWritable);
	}


	/*
	 * Output Exemplar Preferences directly.
	 */
	private void outputExemplars(Context context, DenseVector C, int reducerColNum,
			int reducerLevelNum, String exemplar) throws IOException,
			InterruptedException {

		VectorWritable CWritable = new VectorWritable(C);

		context.write(new Text(reducerColNum + "\t" + reducerLevelNum + "\t"
				+ exemplar), CWritable);
	}


	/*
	 * Output Tau directly.
	 */
	private void outputTau(Context context, DenseVector T, int reducerColNum,
			int reducerLevelNum, String tau) throws IOException,
			InterruptedException {

		VectorWritable TWritable = new VectorWritable(T);

		context.write(new Text(reducerColNum + "\t" + reducerLevelNum + "\t"
				+ tau), TWritable);
	}


	/*
	 * Output Similarity directly.
	 */
	private void outputSimilarity(Context context, DenseVector S, int reducerColNum,
			int reducerLevelNum, String similarity) throws IOException,
			InterruptedException {

		VectorWritable SWritable = new VectorWritable(S);

		context.write(new Text(reducerColNum + "\t" + reducerLevelNum + "\t"
				+ similarity), SWritable);
	}


	/*
	 * Output Responsibility directly.
	 */
	private void outputResponsibility(Context context, DenseVector R, int reducerColNum,
			int reducerLevelNum, String responsibility) throws IOException,
			InterruptedException {

		VectorWritable RWritable = new VectorWritable(R);

		context.write(new Text(reducerColNum + "\t" + reducerLevelNum + "\t"
				+ responsibility), RWritable);
	}


	/*
	 * ALGORITHM: Update Tau T(i,l)=C(i,l-1)+R(i,i,l-1)+sum ( max(0,p(k,j,l-1) )
	 * 
	 * Find the maximum of A+S keeping row constant varying column for level one
	 * above level of lower P level
	 */
	private void updateTau(Context context, DenseVector RLevelBelow,
			DenseVector CLevelBelow, DenseVector T, int reducerColNum, 
			int reducerLevelNum, int N, String tau) throws IOException, InterruptedException {

		double sumOfPositives = 0.0;
		for( int rowNum = 0; rowNum < N; rowNum++ ) { 
			if( rowNum == reducerColNum ) { 
				continue;
			}
			double value = RLevelBelow.get( rowNum );
			sumOfPositives += (value > 0) ? value : 0;
		}

		double rhoValue = RLevelBelow.get( reducerColNum );
		double exemplarValue = CLevelBelow.get( 0 ); // "CValue"

		double tauValue = rhoValue + exemplarValue + sumOfPositives;

		T.setQuick( 0, tauValue );
		VectorWritable TWritable = new VectorWritable( T );

		context.write(
				new Text(reducerColNum + "\t" + reducerLevelNum + "\t" + tau),
				TWritable);
	}


	/*
	 * ALGORITHM: sum of positive R(i',k) where i'!=k : k=i A(i,k) = { min[ 0 ,
	 * r(k,k) + sum of positive R(i',k) where i'!={i,k} ] : k != i Find the
	 * maximum of S+A excluding self, subtract this from S, store this in self
	 * (R).
	 */
	private void updateAvailability(Context context, DenseVector A,
			DenseVector R, DenseVector S, DenseVector P, DenseVector C,
			int reducerColNum, int reducerLevelNum, int N, String availability) 
					throws IOException, InterruptedException {

		DenseVector oldA = A.clone();

		// get positive values into RPositive
		DenseVector RPositive = R.clone();
		for( int rowNum = 0; rowNum < N; rowNum++ ) {
			double RValue = RPositive.get( rowNum );
			if( RValue < 0 ) { 
				RPositive.setQuick( rowNum, 0 );
			}
		}

		// reset diagonal value from R
		RPositive.setQuick( reducerColNum, R.get( reducerColNum ) );

		// sum R Positive values
		double RPSum = RPositive.zSum();

		double CVal = C.get(0);
		double PVal = P.get(0);

		double CHat = CVal + PVal;

		// sum together CHat and RPSum into a vector
		A.assign( CHat + RPSum );

		A = (DenseVector) A.minus( RPositive );

		for( int rowNum = 0; rowNum < N; rowNum++ ) {

			if( rowNum == reducerColNum ) { 
				continue;
			}

			double value = A.get( rowNum ) < 0 ? A.get( rowNum ) : 0;

			A.setQuick( rowNum, value );
		}


		double lambda = context.getConfiguration().getFloat("lambda", 0);
		A = (DenseVector) A.times( 1 - lambda );
		oldA = (DenseVector) oldA.times( lambda );
		A = (DenseVector) A.plus( oldA );

		VectorWritable AWritable = new VectorWritable( A );

		context.write(new Text(reducerColNum + "\t" + reducerLevelNum + "\t"
				+ availability), AWritable);

	}
}
