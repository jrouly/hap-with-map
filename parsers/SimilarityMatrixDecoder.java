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
package parsers;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;


/**
 * 
 * @since 2013.07.14
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 */
public class SimilarityMatrixDecoder {
	public static void main(String [] args) throws IOException{
		
		Scanner in = new Scanner(new FileReader(args[0]));
		FileWriter out = new FileWriter(args[1]);
		int n = Integer.valueOf( args[2] );
		int counter;
		while(in.hasNext()){
			counter=0;
			String line=in.nextLine();
			String [] units = line.split(",");
			for(String unit:units){
				String[] info = unit.split(":");
				while(Integer.valueOf(info[0])!=counter){
					out.append("0,");
					counter++;
				}
				out.append(info[1]+",");
				counter++;
			}
			System.out.println("Counter: " + counter);
			System.out.println("n: " + n);
			while( counter < n ) { 
				out.append("0,");
				counter++;
			}
			out.append("\n");
		}
		out.close();
		in.close();
	}
}
