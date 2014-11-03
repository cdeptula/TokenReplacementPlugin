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
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This takes care of the external metadata injection into the TokenReplacementMeta class
 *
 * @author Chris
 */
public class TokenReplacementMetaInjection implements StepMetaInjectionInterface {

  public enum Entry {

    INPUT_TYPE( ValueMetaInterface.TYPE_STRING, "The input type (Text, Field, File)" ),
    INPUT_TEXT( ValueMetaInterface.TYPE_STRING, "The input text" ),
    INPUT_FIELD( ValueMetaInterface.TYPE_STRING, "The input field name" ),
    INPUT_FILENAME( ValueMetaInterface.TYPE_STRING, "The input filename" ),
    INPUT_FILENAME_IN_FIELD( ValueMetaInterface.TYPE_STRING, "The input filename is in a field? (Y/N)" ),
    INPUT_FILENAME_FIELD( ValueMetaInterface.TYPE_STRING, "The input filename field" ),
    ADD_INPUT_FILENAME_TO_RESULT( ValueMetaInterface.TYPE_STRING, "Add input filename to result? (Y/N)" ),

    OUTPUT_TYPE( ValueMetaInterface.TYPE_STRING, "The output type (Field, File)" ),
    OUTPUT_FIELD( ValueMetaInterface.TYPE_STRING, "The output field name" ),
    OUTPUT_FILENAME( ValueMetaInterface.TYPE_STRING, "The output filename" ),
    OUTPUT_FILENAME_IN_FIELD( ValueMetaInterface.TYPE_STRING, "The output filename is in a field? (Y/N)" ),
    OUTPUT_FILENAME_FIELD( ValueMetaInterface.TYPE_STRING, "The output filename field" ),
    APPEND_OUTPUT_FILE( ValueMetaInterface.TYPE_STRING, "Append the output file? (Y/N)" ),
    CREATE_PARENT_FOLDER( ValueMetaInterface.TYPE_STRING, "Create the parent folder? (Y/N)" ),
    OUTPUT_FORMAT( ValueMetaInterface.TYPE_STRING, "The output format (DOS, UNIX, CR, None)" ),
    OUTPUT_ENCODING( ValueMetaInterface.TYPE_STRING, "Encoding type (For allowed values see the step)" ),
    OUTPUT_SPLIT_EVERY( ValueMetaInterface.TYPE_STRING, "Split every n rows" ),
    OUTPUT_INCLUDE_STEPNR( ValueMetaInterface.TYPE_STRING, "Include step nr in filename? (Y/N)" ),
    OUTPUT_INCLUDE_PARTNR( ValueMetaInterface.TYPE_STRING, "Include partition nr in filename? (Y/N)" ),
    OUTPUT_INCLUDE_DATE( ValueMetaInterface.TYPE_STRING, "Include date in filename? (Y/N)" ),
    OUTPUT_INCLUDE_TIME( ValueMetaInterface.TYPE_STRING, "Include time in filename? (Y/N)" ),
    OUTPUT_SPECIFY_DATE_FORMAT( ValueMetaInterface.TYPE_STRING, "Specify date format for filename? (Y/N)" ),
    OUTPUT_DATE_FORMAT( ValueMetaInterface.TYPE_STRING, "Date format for filename" ),
    ADD_OUTPUT_FILENAME_TO_RESULT( ValueMetaInterface.TYPE_STRING, "Add output filename to result? (Y/N)" ),

    TOKEN_START_STRING( ValueMetaInterface.TYPE_STRING, "The token start string." ),
    TOKEN_END_STRING( ValueMetaInterface.TYPE_STRING, "The token end string." ),

    TOKEN_FIELDS( ValueMetaInterface.TYPE_NONE, "The tokens to use" ),
    TOKEN_FIELD( ValueMetaInterface.TYPE_NONE, "One token" ),
    TOKEN_FIELDNAME( ValueMetaInterface.TYPE_STRING, "Field to use for token value" ),
    TOKEN_NAME( ValueMetaInterface.TYPE_STRING, "Token name to use" );

    private int valueType;
    private String description;

    private Entry( int valueType, String description ) {
      this.valueType = valueType;
      this.description = description;
    }

    /**
     * @return the valueType
     */
    public int getValueType() {
      return valueType;
    }

    /**
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    public static Entry findEntry( String key ) {
      return Entry.valueOf( key );
    }
  }

  private TokenReplacementMeta meta;

  public TokenReplacementMetaInjection( TokenReplacementMeta meta ) {
    this.meta = meta;
  }

  @Override
  public List<StepInjectionMetaEntry> getStepInjectionMetadataEntries() throws KettleException {
    List<StepInjectionMetaEntry> all = new ArrayList<StepInjectionMetaEntry>();

    Entry[] topEntries =
      new Entry[] {
        Entry.INPUT_TYPE, Entry.INPUT_TEXT, Entry.INPUT_FIELD, Entry.INPUT_FILENAME, Entry.INPUT_FILENAME_IN_FIELD,
        Entry.INPUT_FILENAME_FIELD, Entry.ADD_INPUT_FILENAME_TO_RESULT,

        Entry.OUTPUT_TYPE, Entry.OUTPUT_FIELD, Entry.OUTPUT_FILENAME, Entry.OUTPUT_FILENAME_IN_FIELD,
        Entry.OUTPUT_FILENAME_FIELD, Entry.APPEND_OUTPUT_FILE, Entry.CREATE_PARENT_FOLDER, Entry.OUTPUT_FORMAT,
        Entry.OUTPUT_ENCODING, Entry.OUTPUT_SPLIT_EVERY, Entry.OUTPUT_INCLUDE_STEPNR, Entry.OUTPUT_INCLUDE_PARTNR,
        Entry.OUTPUT_INCLUDE_DATE, Entry.OUTPUT_INCLUDE_TIME, Entry.OUTPUT_SPECIFY_DATE_FORMAT,
        Entry.OUTPUT_DATE_FORMAT, Entry.ADD_OUTPUT_FILENAME_TO_RESULT,

        Entry.TOKEN_START_STRING, Entry.TOKEN_END_STRING, };
    for ( Entry topEntry : topEntries ) {
      all.add( new StepInjectionMetaEntry( topEntry.name(), topEntry.getValueType(), topEntry.getDescription() ) );
    }

    // The fields
    //
    StepInjectionMetaEntry fieldsEntry =
      new StepInjectionMetaEntry(
        Entry.TOKEN_FIELDS.name(), ValueMetaInterface.TYPE_NONE, Entry.TOKEN_FIELDS.description );
    all.add( fieldsEntry );
    StepInjectionMetaEntry fieldEntry =
      new StepInjectionMetaEntry(
        Entry.TOKEN_FIELD.name(), ValueMetaInterface.TYPE_NONE, Entry.TOKEN_FIELD.description );
    fieldsEntry.getDetails().add( fieldEntry );

    Entry[] fieldsEntries = new Entry[] { Entry.TOKEN_FIELDNAME, Entry.TOKEN_NAME, };
    for ( Entry entry : fieldsEntries ) {
      StepInjectionMetaEntry metaEntry =
        new StepInjectionMetaEntry( entry.name(), entry.getValueType(), entry.getDescription() );
      fieldEntry.getDetails().add( metaEntry );
    }

    return all;
  }

  @Override
  public void injectStepMetadataEntries( List<StepInjectionMetaEntry> all ) throws KettleException {

    List<String> tokenFieldNames = new ArrayList<String>();
    List<String> tokenNames = new ArrayList<String>();

    // Parse the fields, inject into the meta class..
    //
    for ( StepInjectionMetaEntry lookFields : all ) {
      Entry fieldsEntry = Entry.findEntry( lookFields.getKey() );
      if ( fieldsEntry == null ) {
        continue;
      }

      String lookValue = (String) lookFields.getValue();
      switch ( fieldsEntry ) {
        case TOKEN_FIELDS:
          for ( StepInjectionMetaEntry lookField : lookFields.getDetails() ) {
            Entry fieldEntry = Entry.findEntry( lookField.getKey() );
            if ( fieldEntry == Entry.TOKEN_FIELD ) {

              String tokenFieldName = null;
              String tokenName = null;

              List<StepInjectionMetaEntry> entries = lookField.getDetails();
              for ( StepInjectionMetaEntry entry : entries ) {
                Entry metaEntry = Entry.findEntry( entry.getKey() );
                if ( metaEntry != null ) {
                  String value = (String) entry.getValue();
                  switch ( metaEntry ) {
                    case TOKEN_FIELDNAME:
                      tokenFieldName = value;
                      break;
                    case TOKEN_NAME:
                      tokenName = value;
                      break;
                    default:
                      break;
                  }
                }
              }
              tokenFieldNames.add( tokenFieldName );
              tokenNames.add( tokenName );
            }
          }
          break;

        case INPUT_TYPE:
          meta.setInputType( lookValue );
          break;
        case INPUT_TEXT:
          meta.setInputText( lookValue );
          break;
        case INPUT_FIELD:
          meta.setInputFieldName( lookValue );
          break;
        case INPUT_FILENAME:
          meta.setInputFileName( lookValue );
          break;
        case INPUT_FILENAME_IN_FIELD:
          meta.setInputFileNameInField( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case INPUT_FILENAME_FIELD:
          meta.setInputFileNameField( lookValue );
          break;
        case ADD_INPUT_FILENAME_TO_RESULT:
          meta.setAddInputFileNameToResult( "Y".equalsIgnoreCase( lookValue ) );
          break;

        case OUTPUT_TYPE:
          meta.setOutputType( lookValue );
          break;
        case OUTPUT_FIELD:
          meta.setOutputFieldName( lookValue );
          break;
        case OUTPUT_FILENAME:
          meta.setOutputFileName( lookValue );
          break;
        case OUTPUT_FILENAME_IN_FIELD:
          meta.setOutputFileNameInField( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case OUTPUT_FILENAME_FIELD:
          meta.setOutputFileNameField( lookValue );
          break;
        case APPEND_OUTPUT_FILE:
          meta.setAppendOutputFileName( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case CREATE_PARENT_FOLDER:
          meta.setCreateParentFolder( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case OUTPUT_FORMAT:
          meta.setOutputFileFormat( lookValue );
          break;
        case OUTPUT_ENCODING:
          meta.setOutputFileEncoding( lookValue );
          break;
        case OUTPUT_SPLIT_EVERY:
          meta.setSplitEvery( Const.toInt( lookValue, 0 ) );
          break;
        case OUTPUT_INCLUDE_STEPNR:
          meta.setIncludeStepNrInOutputFileName( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case OUTPUT_INCLUDE_PARTNR:
          meta.setIncludePartNrInOutputFileName( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case OUTPUT_INCLUDE_DATE:
          meta.setIncludeDateInOutputFileName( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case OUTPUT_INCLUDE_TIME:
          meta.setIncludeTimeInOutputFileName( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case OUTPUT_SPECIFY_DATE_FORMAT:
          meta.setSpecifyDateFormatOutputFileName( "Y".equalsIgnoreCase( lookValue ) );
          break;
        case OUTPUT_DATE_FORMAT:
          meta.setDateFormatOutputFileName( lookValue );
          break;
        case ADD_OUTPUT_FILENAME_TO_RESULT:
          meta.setAddOutputFileNameToResult( "Y".equalsIgnoreCase( lookValue ) );
          break;

        case TOKEN_START_STRING:
          meta.setTokenStartString( lookValue );
          break;
        case TOKEN_END_STRING:
          meta.setTokenEndString( lookValue );
          break;
        default:
          break;
      }
    }

    // Pass the grid to the step metadata
    //
    if ( tokenFieldNames.size() > 0 ) {
      TokenReplacementField[] tff = new TokenReplacementField[tokenFieldNames.size()];
      Iterator<String> iTokenFieldNames = tokenFieldNames.iterator();
      Iterator<String> iTokenNames = tokenNames.iterator();

      int i = 0;
      while ( iTokenFieldNames.hasNext() ) {
        TokenReplacementField field = new TokenReplacementField();
        field.setName( iTokenFieldNames.next() );
        field.setTokenName( iTokenNames.next() );
        tff[i] = field;
        i++;
      }
      meta.setTokenReplacementFields( tff );
    }
  }


  public TokenReplacementMeta getMeta() {
    return meta;
  }
}