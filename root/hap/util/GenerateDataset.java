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
package root.hap.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


/**
 * <p>
 * Utility class to generate randomized datasets. All values generated will be 
 * integers in the range [-100,0].
 * </p>
 * 
 * <p>
 * Discussion of implementation.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 * @deprecated
 * 
 */
public class GenerateDataset {

	public static void main(String[] args) throws IOException, URISyntaxException { 
		
		if( args.length != 3 ) { 
			System.err.println("Usage: GenerateDataset <N> <L> <outputfile>");
			System.exit(2);
		}
		int N = Integer.valueOf(args[0]);
		int L = Integer.valueOf(args[1]);
		
		Configuration workingConf = new Configuration();
		URI workingURI = new URI( workingConf.get("fs.default.name") );
		FileSystem workingFS = FileSystem.get(workingURI, workingConf);
		
		Path path = new Path(args[2]);
		
		SequenceFile.Writer writer = new SequenceFile.Writer(workingFS, workingConf,
				path, Text.class, Text.class);
		
		String[] IDs = {"S", "R", "A"};
		
		for( int id = 0; id < 3; id++ ) {
			
			for( int row = 0; row < N; row++ ) {
				for( int col = 0; col < N; col++ ) { 
					
					if(IDs[id].equals("S")){
						int value = id > 0 ? 0 : (int)(Math.random() * -100);
						if(row==col){
							writer.append(new Text(row+"\t"+col+"\t0\t"+IDs[id]), new Text(-100000+""));
						}else{
							writer.append(new Text(row+"\t"+col+ "\t0\t"+IDs[id]), new Text(value+""));
						}
					}
					
					for( int level = 0; level < L; level++ ) { 
						
						int value = id > 0 ? 0 : (int)(Math.random() * -100);
						if(!IDs[id].equals("S")){
							writer.append(new Text(row+"\t"+col+"\t"+ level+ "\t"+IDs[id]), new Text(value+""));
						}
						
					}
				}
			}
		}
		
		String [] IDs2 = {"T", "P", "C"};
		
		for( int id = 0; id < 3; id++ ) {
			
			for( int index = 0; index < N; index++ ) {
				for( int level = 0; level < L; level++ ) { 
					
					int value =  0 ;
					if(IDs2[id].equals("T")){
						writer.append(new Text(index+"\t"+ level+ "\t"+IDs2[id]), new Text(100000+""));
					}else{
						writer.append(new Text(index+"\t"+ level+ "\t"+IDs2[id]), new Text(value+""));
					}
					
				}
			}
		}
		
		writer.close();
		
	}

}
