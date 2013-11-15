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

import org.apache.hadoop.io.Text;

/**
 * <p>
 * This is a utilities class which provides tools relevant to key-based 
 * operations. A data key refers to a block of vector header data used in
 * most MapReduce tasks in this application.
 * </p>
 * 
 * <p>
 * All methods in this class are static and must be executed in a static context
 * on a String containing a data key.
 * </p>
 * 
 * @author Dillon Rose
 * @author Michel Rouly
 * 
 * @since 2013.06.02
 *
 */
public class KeyUtilities {

	public static final int INDEX = 0;
	public static final int LEVEL = 1;
	public static final int ID    = 2;
	public static final int VALUE = 3;


	/**
	 * <p>Split a key into its component parts.</p>
	 * 
	 * <p>Generally a key takes the form <code>(row,col,level,ID,val)</code></p>
	 * 
	 * @param key the key to explode
	 * @param expectsValue should this method expect a 4th field for value
	 * 
	 * @return a simple String array indexing the key's contents, generally
	 * 			in the format [row,col,ID,val]
	 */
	public static String[] explode( String key, boolean expectsValue ) { 
		String[] data = key.split("\t");

		data[INDEX] = Integer.valueOf(data[INDEX])+"";
		data[LEVEL] = Integer.valueOf(data[LEVEL])+"";
		data[ID   ] = data[ID].trim();
		
		if( expectsValue ) { 
			data[VALUE] = Double.valueOf( data[VALUE] ) + "";
		}

		return data;
	}


	/**
	 * <p>Split a key into its component parts.</p>
	 * 
	 * <p>Generally a key takes the form <code>(row,col,level,ID,val)</code></p>
	 * 
	 * @param key the key to explode
	 * @param expectsValue should this method expect a 4th field for value
	 * 
	 * @return a simple String array indexing the key's contents, generally
	 * 			in the format [row,col,ID,val]
	 */
	public static String[] explode( Text key, boolean expectsValue ) { 
		String cellWrapper = key.toString();
		String[] data = KeyUtilities.explode( cellWrapper, expectsValue );
		return data;
	}


//	/**
//	 * <p>Set a given property of a key to a new value.</p>
//	 * 
//	 * @param key the key to update
//	 * @param property the property index to update
//	 * @param value the new value of the property
//	 * @return a reference to the updated key
//	 */
//	public static Text setProperty( Text key, int property, String value ) { 
//
//		String cellString, newString;
//		Text newCell;
//
//		cellString = key.toString();
//		newString = KeyUtilities.setProperty( cellString, property, value );
//		newCell = new Text( newString );
//
//		return newCell;
//	}
//
//	/**
//	 * <p>Set a given property of a key to a new value.</p>
//	 * 
//	 * @param key the key to update
//	 * @param property the property index to update
//	 * @param value the new value of the property
//	 * @return a reference to the updated key
//	 */
//	public static String setProperty( String key, int property, String value ) { 
//
//		StringBuilder newCellBuilder = new StringBuilder();
//
//		String[] cellData = KeyUtilities.explode( key );
//		cellData[ property ] = value;
//
//		for( int i = 0; i < cellData.length; i++ ) { 
//			newCellBuilder.append( cellData[i] );
//			if( i < cellData.length - 1 ) { 
//				newCellBuilder.append( "\t" );
//			}
//		}
//
//		return newCellBuilder.toString();
//
//	}
//
//	/**
//	 * <p>Access the requested property of this key.</p>
//	 * 
//	 * @param key the key in question
//	 * @param property the property in question
//	 * @return value of the requested property
//	 */
//	public static String getProperty( Text key, int property ) { 
//
//		String[] cellData = KeyUtilities.explode( key );
//		return cellData[ property ];
//
//	}
//
//	/**
//	 * <p>Access the requested property of this key.</p>
//	 * 
//	 * @param key the key in question
//	 * @param property the property in question
//	 * @return value of the requested property
//	 */
//	public static String getProperty( String key, int property ) { 
//
//		String[] cellData = KeyUtilities.explode( key );
//		return cellData[ property ];
//
//	}


	/**
	 * <p>Construct a custom instance of a key. Assumes no value field.</p>
	 * 
	 * @param index row value
	 * @param level column value
	 * @param id key id (R, S, A)
	 * 
	 * @return custom instance of a key
	 */
	public static Text getInstance( String index, String level, String id ) { 

		StringBuilder cellBuilder = new StringBuilder();

		cellBuilder.append( index );
		cellBuilder.append( "\t" );
		cellBuilder.append( level );
		cellBuilder.append( "\t" );
		cellBuilder.append( id );

		String cellStr = cellBuilder.toString();
		Text key = new Text( cellStr );

		return key;

	}


	/**
	 * <p>Construct a custom instance of a key. Assumes no value field.</p>
	 * 
	 * @param index row value
	 * @param level column value
	 * @param id key id (R, S, A)
	 * 
	 * @return custom instance of a key
	 */
	public static Text getInstance( int index, int level, String id ) { 

		StringBuilder cellBuilder = new StringBuilder();

		cellBuilder.append( index );
		cellBuilder.append( "\t" );
		cellBuilder.append( level );
		cellBuilder.append( "\t" );
		cellBuilder.append( id );

		String cellStr = cellBuilder.toString();
		Text key = new Text( cellStr );

		return key;

	}

}
