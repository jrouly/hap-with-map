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
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import root.hap.util.KeyUtilities;


/**
 * <p>
 * Reducer class for updating responsibility.
 * </p>
 * 
 * <p>
 * This reducer takes in a set of columns from the input matrices and uses their
 * contents to update the Responsibility matrix.
 * </p>
 * 
 * <p>
 * <code>Input key [Text]:</code> information about these columns<br />
 * <code>Input value [Iterable&lt;Text&gt;]:</code> the corresponding input 
 * columns
 * </p>
 * 
 * <p>
 * <code>Output key [Text]:</code> tab separated list: {Row Number, Level, 
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
 * @see HierarchicalResponsibilityDriver
 * @see ResponsibilityReducer
 * 
 */
public class ResponsibilityReducer extends
Reducer<Text, Text, Text, VectorWritable> {


	/**
	 * <p>
	 * This method serves to update the responsibility matrix by accessing a
	 * single column of each matrix.
	 * </p>
	 * 
	 * @param keyIn vector identification block
	 * @param valIn vector data
	 */
	public void reduce(Text keyIn, Iterable<Text> valIn, Context context)
			throws IOException, InterruptedException {

		int N = context.getConfiguration().getInt("matrixN", -1);
		int numLevels = context.getConfiguration().getInt("numLevels", -1);

		// data structures to reconstruct the rows we're working on
		DenseVector A = new DenseVector(N);
		DenseVector diagA = new DenseVector(N);
		DenseVector ALevelAbove = new DenseVector(N);
		DenseVector R = new DenseVector(N);
		DenseVector diagR = new DenseVector(N);
		DenseVector S = new DenseVector(N);
		DenseVector T = new DenseVector(1);
		DenseVector P = new DenseVector(1);
		DenseVector C = new DenseVector(1);

		int reducerRowNum = Integer.valueOf(keyIn.toString().split("\t")[0]);
		int reducerLevelNum = Integer.valueOf(keyIn.toString().split("\t")[1]);

		for (Text text : valIn) { 
			// **** important, do not remove this conversion ****
			// we must do this for some reason..not sure why.
			// if we don't the loop doesn't iterate properly.
			// **** important, do not remove this conversion ****
			text = new Text(text.toString());

			String[] textData = KeyUtilities.explode(text, true);

			String col = textData[KeyUtilities.INDEX];
			String val = textData[KeyUtilities.VALUE];
			String level = textData[KeyUtilities.LEVEL];
			String id = textData[KeyUtilities.ID];

			int colInt = Integer.valueOf(col);
			double valDouble = Double.valueOf(val);
			int levelInt = Integer.valueOf(level);

			switch (id.charAt(0)) {
			case 'R':
				R.setQuick(colInt, valDouble);
				break;
			case 'r':
				diagR.setQuick(colInt, valDouble);
				break;
			case 'A':
				if (reducerLevelNum == levelInt) {
					A.setQuick(colInt, valDouble);
				} else {
					ALevelAbove.set(colInt, valDouble);
				}
				break;
			case 'a':
				diagA.setQuick(colInt, valDouble);
				break;
			case 'S':
				S.setQuick(colInt, valDouble);
				break;
			case 'C':
				C.setQuick(0, valDouble);
				break;
			case 'T':
				T.setQuick(0, valDouble);
				break;
			case 'P':
				P.setQuick(0, valDouble);
				break;
			default:
				System.err.println("[ERROR]: Invalid matrix ID.");
				System.exit(1);
				break;
			}
		}

		//		printInput(A, ALevelAbove, diagA, R, diagR, S, T, P, C);

		outputTau(context, T, reducerRowNum, reducerLevelNum, "T");

		// We want this part to get skipped on the 1st iteration
		int numIteration = context.getConfiguration()
				.getInt("numIteration", -1);

		if (numIteration == 0) {

			outputExemplars(context, C, reducerRowNum, reducerLevelNum, "C");

		} else {

			updateExemplars(context, A, R, diagA, diagR, C, reducerRowNum, reducerLevelNum, 
					N, "C");

		}

		if (reducerLevelNum != numLevels - 1 && numIteration != 0) {

			updatePhi(context, ALevelAbove, S, P, reducerRowNum, 
					reducerLevelNum, N, "P");

		} else {

			outputPhi(context, P, reducerRowNum, reducerLevelNum, "P");

		}

		outputAvailability(context, A, reducerRowNum, reducerLevelNum, "A");

		if( reducerLevelNum == 0 ) { 
			outputSimilarity(context, S, reducerRowNum, reducerLevelNum, "S");
		}

		updateResponsibility(context, A, S, R, T, reducerLevelNum,
				reducerRowNum, N, "R");


		keyIn = null;
		valIn = null;
		A = null;
		diagA = null;
		ALevelAbove = null;
		R = null;
		diagR = null;
		S = null;
		T = null;
		P = null;
		C = null;
		System.gc();
	}


	//	private void printInput(DenseVector A, DenseVector ALevelAbove, DenseVector diagA,
	//			DenseVector R, DenseVector diagR, DenseVector S, DenseVector T, DenseVector P,
	//			DenseVector C) {
	//
	//		System.out.println(A);
	//		System.out.println(ALevelAbove);
	//		System.out.println(diagA);
	//		System.out.println(R);
	//		System.out.println(diagR);
	//		System.out.println(S);
	//		System.out.println(T);
	//		System.out.println(P);
	//		System.out.println(C);
	//
	//		System.out.println("------------");
	//	}


	/*
	 * Output Tau directly.
	 */
	private void outputTau(Context context, DenseVector T, int reducerRowNum,
			int reducerLevelNum, String tau) throws IOException,
			InterruptedException {

		VectorWritable TWritable = new VectorWritable(T);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ tau), TWritable);

	}

	/*
	 * Output Exemplar Preferences directly.
	 */
	private void outputExemplars(Context context, DenseVector C,
			int reducerRowNum, int reducerLevelNum, String exemplar)
					throws IOException, InterruptedException {

		VectorWritable CWritable = new VectorWritable(C);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ exemplar), CWritable);

	}


	/*
	 * ALGORITHM: Update Exemplars C(i,l) = argmax [ A(i,j,l) + R(i,j,l) ] where
	 * A(j,j,l) + R(i,j,l) > 0 Find the maximum of A+R keeping row constant
	 * varying column for level equal level to lower C level
	 */
	private void updateExemplars(Context context, DenseVector A, DenseVector R,
			DenseVector diagA, DenseVector diagR, DenseVector C, int reducerRowNum, 
			int reducerLevelNum, int N, String exemplar) 
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
		
		if( validExist ){
			
			for ( int diagIter = 0; diagIter < N ; diagIter++ ){
				
				double validValue = sum.get( diagIter );
				if( validExemplars [diagIter] && validValue > maxValue){
					maxValue = validValue;
				}
				
			}
			
		}else{
			
			maxValue = sum.maxValue();
			
		}
		
		C.setQuick(0, maxValue);

		VectorWritable CWritable = new VectorWritable(C);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ exemplar), CWritable);

	}


	/*
	 * Output Phi directly.
	 */
	private void outputPhi(Context context, DenseVector P, int reducerRowNum,
			int reducerLevelNum, String phi) throws IOException,
			InterruptedException {

		VectorWritable PWritable = new VectorWritable(P);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ phi), PWritable);

	}


	/*
	 * ALGORITHM: Update Phi P(i,l)=max( A(i,j,l+1) + S(i,j,l+1) )
	 * 
	 * Find the maximum of A+S keeping row constant varying column for level one
	 * above level of lower P level
	 */
	private void updatePhi(Context context, DenseVector ALevelAbove,
			DenseVector SLevelAbove, DenseVector P, int reducerRowNum, 
			int reducerLevelNum, int N, String phi) 
					throws IOException, InterruptedException {

		DenseVector sum = (DenseVector) ALevelAbove.plus(SLevelAbove);
		double maxValue = sum.maxValue();

		P.setQuick( 0, maxValue );

		VectorWritable PWritable = new VectorWritable(P);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ phi), PWritable);
	}


	/*
	 * Output Availability directly.
	 */
	private void outputAvailability(Context context, DenseVector A,
			int reducerRowNum, int reducerLevelNum, String availability)
					throws IOException, InterruptedException {

		VectorWritable AWritable = new VectorWritable(A);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ availability), AWritable);

	}


	/*
	 * Output Similarity directly.
	 */
	private void outputSimilarity(Context context, DenseVector S,
			int reducerRowNum, int reducerLevelNum, String similarity)
					throws IOException, InterruptedException {

		VectorWritable SWritable = new VectorWritable(S);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ similarity), SWritable);

	}


	/*
	 * ALGORITHM: Update Responsibilty R(i,k) = S(i,k) - max[ S(i,j) + A(i,j) ]
	 * 
	 * Find the maximum of S+A excluding self, subtract this from S, store this
	 * in self (R).
	 */
	private void updateResponsibility(Context context, DenseVector A,
			DenseVector S, DenseVector R, DenseVector T, int reducerLevelNum,
			int reducerRowNum, int N, String responsibilty) throws IOException,
			InterruptedException {

		DenseVector oldR = R.clone();
		DenseVector sum = (DenseVector) A.plus(S);

		double maxValue = sum.maxValue();
		int maxValueIndex = sum.maxValueIndex();
		maxValue *= -1;

		sum.set(maxValueIndex, Double.NEGATIVE_INFINITY);

		double tau = T.get(0);

		double YH = Math.min(maxValue, tau);

		double actualMax = sum.maxValue();
		actualMax*=-1;

		double YH2 = Math.min(actualMax, tau);

		R=(DenseVector) S.plus(YH);
		R.setQuick(maxValueIndex, S.get(maxValueIndex)+YH2);

		// Dampen
		double lambda = context.getConfiguration().getFloat("lambda", 0);
		R = (DenseVector) R.times(1 - lambda);
		oldR = (DenseVector) oldR.times(lambda);
		R = (DenseVector) R.plus(oldR);

		VectorWritable RWritable = new VectorWritable(R);

		context.write(new Text(reducerRowNum + "\t" + reducerLevelNum + "\t"
				+ responsibilty), RWritable);

	}
}
