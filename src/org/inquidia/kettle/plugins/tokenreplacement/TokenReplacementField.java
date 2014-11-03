/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.inquidia.kettle.plugins.tokenreplacement;


import org.pentaho.di.core.Const;

/**
 * Describes a single field in a text file
 *
 * @author Chris
 * @since 06-10-2014
 *
 */
public class TokenReplacementField implements Cloneable {
	

  private String name;
  
  private String tokenName;

 public TokenReplacementField( String name, String tokenName ) {
    this.name = name;
    this.tokenName = tokenName;
  }

  public TokenReplacementField() {
  }
  
  public int compare( Object obj ) {
    TokenReplacementField field = (TokenReplacementField) obj;

    return name.compareTo( field.getName() );
  }

  public boolean equals( Object obj ) {
    TokenReplacementField field = (TokenReplacementField) obj;

    return name.equals( field.getName() ) && tokenName.equals( field.tokenName );
  }

  public boolean equalsIgnoreCase( Object obj ) {
    TokenReplacementField field = (TokenReplacementField) obj;

    return name.equalsIgnoreCase( field.getName() ) && tokenName.equalsIgnoreCase( field.tokenName );
  }

  public Object clone() {
    try {
      return super.clone();
    } catch ( CloneNotSupportedException e ) {
      return null;
    }
  }

  /**
   * Get the stream field name.
   * @return name
   */
  public String getName() {
    return name;
  }

  /**
   * Set the stream field name.
   * @param fieldname
   */
  public void setName( String fieldname ) {
    this.name = fieldname;
  }

  /**
   * Get the name of the token
   * If the token name is null returns the stream field name
   * @return tokenName
   */
  public String getTokenName() {
	  return Const.NVL( tokenName, name );
  }

  /**
   * Set the name of the token
   * @param tokenName
   */
  public void setTokenName( String tokenName ) {
	  this.tokenName = tokenName;
  }

  public String toString() {
    return name;
  }


}