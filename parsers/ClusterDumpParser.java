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
public class ClusterDumpParser {
	public static void main(String[] args) throws IOException {
		File work = new File(args[0]);
		File[] levels = work.listFiles();
		PrintStream writer = new PrintStream( new FileOutputStream( new File (args[1]) ) );
		String line;

		int levelCounter = 1;
		int clusterCounter = 1;

		for (File level : levels) {
			if (level.toString().charAt(level.toString().lastIndexOf("/") + 1) == '.') {
				continue;
			}
			File[] clustersFiles = level.listFiles();
			for (File clustersFile : clustersFiles) {
				if (clustersFile.toString().charAt(
						clustersFile.toString().lastIndexOf("/") + 1) == '.') {
					continue;
				}
				Scanner reader = new Scanner(new BufferedReader(new FileReader(
						clustersFile)));
				
				while (reader.hasNext()) {
					line = reader.nextLine();
					if(line.contains("1.0: ")){
						line=line.substring(line.indexOf("[")+1);
						line=line.substring(0,line.indexOf("]"));
						String[] xy = line.split(",");
						double x = Double.valueOf(xy[0].split(":")[1]);
						double y = Double.valueOf(xy[1].split(":")[1]);
						writer.printf("%.2f-%.2f,%d,%d\n",x,y,clusterCounter,levelCounter);
					}else{
						line = reader.nextLine();
						clusterCounter++;
					}
				}
				
				reader.close();
			}
			clusterCounter=0;
			levelCounter++;
		}
		writer.close();

	}
}
