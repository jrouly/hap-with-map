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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;


/**
 * <p>
 * Mapper class for creating similarity matrix
 * </p>
 * 
 * <p>
 * Calculates pairwise distances from every seed vector. Row Value equals the 
 * negative distance from the input vector and the vector of the Row Number.
 * When Row Number equals Column Number, the distance must be calculated 
 * in a special way.
 * </p>
 * 
 * <p>
 * <code>Input key [Text]:</code> column number<br />
 * <code>Input value [VectorWritable]:</code> the corresponding vector
 * </p>
 * 
 * <p>
 * <code>Output key [Text]:</code> tab separated list: {Column Number,
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
 * @see CreateSimilarityMatrixJob
 * @see CreateSeedVector
 * @see DenseVector
 * 
 */
public final class SimilarityMatrixMapper extends
Mapper<WritableComparable<?>, VectorWritable, Text, VectorWritable> {

	private DistanceMeasure measure;
	private List<NamedVector> seedVectors;

	private final double TAU_INIT = Double.POSITIVE_INFINITY;

	private double smatDiagScale;

	@Override
	protected void map(WritableComparable<?> key, VectorWritable value,
			Context context) throws IOException, InterruptedException {

		smatDiagScale = context.getConfiguration().getLong("diagScale", -1);
		int levels = context.getConfiguration().getInt("numLevels", -1);

		String keyName = key.toString().substring(1);
		Vector valVec = value.get();

		int N = seedVectors.size();

		DenseVector outSVec = new DenseVector(N);
		outSVec.assign(0.0);
		DenseVector out0Vec = new DenseVector(N);
		out0Vec.assign(0.0);

		Text outSKey = new Text(keyName + "\t0\tS");
		Text outRKey = new Text();
		Text outAKey = new Text();

		for (NamedVector seedVector : seedVectors) {
			double distance = measure.distance(seedVector, valVec);

			String seedVectorName = seedVector.getName().substring(1);
			int seedVectorPos = Integer.valueOf(seedVectorName);

			if (keyName.equals(seedVectorName)) {
				double diagValue = generateSMatDiagValue( smatDiagScale );
				outSVec.set(seedVectorPos, diagValue );
			} else {
				outSVec.set(seedVectorPos, -1 * distance);
			}
		}


		VectorWritable outSVecWritable = new VectorWritable(outSVec);

		context.write(outSKey, outSVecWritable);

		VectorWritable out0VecWritable = new VectorWritable(out0Vec);

		for (int i = 0; i < levels; i++) {
			outRKey = new Text(keyName + "\t" + i + "\tR");
			outAKey = new Text(keyName + "\t" + i + "\tA");
			context.write(outRKey, out0VecWritable);
			context.write(outAKey, out0VecWritable);
		}

		Text outCKey = new Text();
		Text outTKey = new Text();
		Text outPKey = new Text();

		out0Vec = new DenseVector(1);
		out0Vec.assign(0.0);
		DenseVector outTVec = new DenseVector(1);
		//		outTVec.assign(Double.MAX_VALUE / 2);
		outTVec.assign( TAU_INIT );

		out0VecWritable = new VectorWritable(out0Vec);
		VectorWritable outTVecWritable = new VectorWritable(outTVec);

		for (int i = 0; i < levels; i++) {
			outCKey = new Text(keyName + "\t" + i + "\tC");
			outTKey = new Text(keyName + "\t" + i + "\tT");
			outPKey = new Text(keyName + "\t" + i + "\tP");
			context.write(outCKey, out0VecWritable);
			context.write(outTKey, outTVecWritable);
			context.write(outPKey, out0VecWritable);
		}

	}


	/*
	 * Generate a random value in a range offset from the scale value by
	 * some order of magnitude.
	 */
	private double generateSMatDiagValue( double scale ) { 
		if( scale < 0 ) { 
			System.err.println("[ERROR]: Invalid Similarity Matrix diagonal scale.");
			System.exit( 1 );
		}

//		// This is the "order of magnitude" which our range is shifted from the
//		// input scale value
//		double magnitude = 10; 

//		double range_min = scale * magnitude;
//		double range_max = range_min * magnitude;
//
//		double range = range_max - range_min;

//		double value = -1 * ((Math.random() * range) + range_min);
		double value = -1 * (Math.random() * scale);

		return value;
	}


	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		super.setup(context);
		try { 
			Configuration conf = context.getConfiguration();
			measure = ClassUtils.instantiateAs(
					conf.get(CreateSimilarityMatrixJob.DISTANCE_MEASURE_KEY),
					DistanceMeasure.class);
			measure.configure(conf);
			seedVectors = CreateSeedVector.loadSeedVectors(conf);
		} catch( Exception e ) { 
			System.err.println("[ERROR]: Unable to proceed with setup.");
			System.err.println( e );
			System.exit( 1 );
		}
	}
}
