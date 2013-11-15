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
package root.input;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;


/**
 * <p>
 * This is an abstract interface describing drivers for running various 
 * MapReduce jobs on any given input dataset in this software framework.
 * </p>
 * 
 * <p>
 * To run this program from the command line:<br/>
 * <code>$ bin/hadoop root.Driver directive options</code>
 * </p>
 * 
 * <p>
 * Include (at minimum) the following external libraries on the classpath: <br />
 * <code>
 * 	lib/run/*.jar<br/>
 * 	lib/build/*.jar<br/>
 * </code>
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.25
 * 
 */
public abstract class InputJob extends AbstractJob {

	protected final String CONF_PREFIX = "__AMALTHEA__";

	/**
	 * Construct the argument list for this Job.
	 */
	protected abstract void constructParameterList();


	/**
	 * Initialize all necessary configuration parameters from the 
	 * arguments list. 
	 */
	protected abstract void initializeConfigurationParameters();


	/**
	 * Print the "Job Running" header to standard output.
	 */
	protected final void printJobHeader() { 
		SimpleDateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		String timestamp = gmtDateFormat.format(new Date());

		System.out.println( );
		System.out.println( "========================" );
		System.out.println( "[INFO]: RUNNING JOB" );
		System.out.println( timestamp );
		System.out.println( "========================" );

		printConfiguredParameters();
	}


	/**
	 * Print configured parameter values to standard output.
	 */
	protected void printConfiguredParameters() { 
		;
	}


	/**
	 * Clean an input directory name by removing any final '/' characters.
	 * 
	 * @param dirName the directory name to clean
	 * @return the cleaned directory name
	 */
	protected String cleanDirectoryName(String dirName) { 
		while( dirName.endsWith("/") ) { 
			dirName = dirName.substring(0, dirName.length() - 1 );
		}
		return dirName;
	}


	/**
	 * This method allows the Job to act as a {@link ToolRunner} and 
	 * interface properly with the Driver.
	 * 
	 * @param args Configuration arguments
	 * @return Exit status
	 * @see ToolRunner
	 */
	@Override
	public int run(String[] arg0) throws Exception {
		throw new NotImplementedException();
	}


	/**
	 * Writes a timestamp to file.
	 * 
	 * @param conf the configuration object to work with
	 * @param filename the location to write to
	 * @param time the timestamp to write
	 * @return
	 */
	protected boolean writeTimestamp( 
			Configuration conf, 
			String filename, 
			long time ) { 

		boolean success = true;

		try { 
			URI localURI = new URI(conf.get("fs.default.name"));
			FileSystem localFS = FileSystem.get(localURI, conf);

			Path timestamp = new Path(filename);

			if (localFS.exists(timestamp)) {
				localFS.delete(timestamp, true);
			}

			FSDataOutputStream out = localFS.create(timestamp);

			out.writeBytes( "HAP duration: " + time + "\n" );

			Iterator <Map.Entry<String,String>> iter = conf.iterator();
			while( iter.hasNext() ) { 
				Map.Entry<String,String> entry = iter.next();
				String key = entry.getKey();
				String val = entry.getValue();

				if( key.startsWith( CONF_PREFIX ) ) { 
					key = key.substring( CONF_PREFIX.length() );
					out.writeBytes( key );
					out.writeBytes( "\t" );
					out.writeBytes( val );
					out.writeBytes( "\n" );
				}
			}

			out.flush();
			out.close();

			localFS.close();
		} catch( IOException e ) { 
			System.err.println( e );
			success = false;
		} catch( URISyntaxException e ) { 
			System.err.println( e );
			success = false;
		}

		return success;
	}


}
