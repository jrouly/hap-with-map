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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;


/**
 * <p>
 * This Sequence File Input Format allows the reading of an input file 
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
public class KeyAsValueSequenceFileInputFormat extends SequenceFileAsTextInputFormat {
	
	@Override
	public RecordReader<Text, Text> getRecordReader(InputSplit split,
			JobConf job,
			Reporter reporter) throws IOException {
		
		reporter.setStatus(split.toString());
		
		return new KeyAsValueSequenceFileRecordReader( job, (FileSplit) split);
	}
	
}
