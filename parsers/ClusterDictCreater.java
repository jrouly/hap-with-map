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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;


/**
 * 
 * @since 2013.07.14
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 */
public class ClusterDictCreater {
	public static void main(String[] args) throws IOException {
		Scanner reader = new Scanner(new BufferedReader(new FileReader(
				new File( args[0] ))));
		PrintStream writer = new PrintStream( new FileOutputStream( new File (args[1]) ) );
		String line;

		while(reader.hasNext()){
			line=reader.nextLine();
			String [] info = line.split(",");
			double x = Double.valueOf( info[0] );
			double y = Double.valueOf( info[1] );
			int cluster = Integer.valueOf( info[2] );
			writer.printf("%.2f-%.2f,%d\n",x,y,cluster);
		}

		writer.close();
		reader.close();

	}
}
