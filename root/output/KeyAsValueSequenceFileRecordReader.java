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
package root.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.SequenceFileRecordReader;


/**
 * <p>
 * This Sequence File Record Reader allows the reading of an input file 
 * formatted where the key is effectively ignored and all data is stored in 
 * the value.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
@SuppressWarnings("rawtypes")
public class KeyAsValueSequenceFileRecordReader implements RecordReader<Text, Text> {

	private final SequenceFileRecordReader<WritableComparable, Writable>
	sequenceFileRecordReader;

	private WritableComparable innerKey;
	private Writable innerValue;

	public KeyAsValueSequenceFileRecordReader(Configuration conf, FileSplit split)
			throws IOException {

		sequenceFileRecordReader =
				new SequenceFileRecordReader<WritableComparable, Writable>(conf, split);
		innerKey = sequenceFileRecordReader.createKey();
		innerValue = sequenceFileRecordReader.createValue();
	}

	public Text createKey() {
		return new Text();
	}

	public Text createValue() {
		return new Text();
	}

	/** Read key/value pair in a line. */
	public synchronized boolean next(Text key, Text value) throws IOException {
		Text tKey = key;
		Text tValue = value;
		if (!sequenceFileRecordReader.next(innerKey, innerValue)) {
			return false;
		}
		//		tKey.set(innerKey.toString());
		tKey.set( "ignored" );
		//		tValue.set(innerValue.toString());
		tValue.set( innerKey.toString() + "\t" + innerValue.toString() );
		return true;
	}

	public float getProgress() throws IOException {
		return sequenceFileRecordReader.getProgress();
	}

	public synchronized long getPos() throws IOException {
		return sequenceFileRecordReader.getPos();
	}

	public synchronized void close() throws IOException {
		sequenceFileRecordReader.close();
	}

}


