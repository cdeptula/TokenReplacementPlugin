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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInjectionInterface;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/*
 * Created on 4-apr-2003
 *
 */
public class TokenReplacementMeta extends BaseStepMeta implements StepMetaInterface {
  public static final String INPUT_TYPE = "input_type";
  public static final String INPUT_FIELD_NAME = "input_field_name";
  public static final String INPUT_FILENAME = "input_filename";
  public static final String INPUT_FILENAME_IN_FIELD = "input_filename_in_field";
  public static final String INPUT_FILENAME_FIELD = "input_filename_field";
  public static final String ADD_INPUT_FILENAME_TO_RESULT = "add_input_filename_to_result";
  public static final String OUTPUT_TYPE = "output_type";
  public static final String OUTPUT_FIELD_NAME = "output_field_name";
  public static final String OUTPUT_FILENAME = "output_filename";
  public static final String OUTPUT_FILENAME_IN_FIELD = "output_filename_in_field";
  public static final String OUTPUT_FILENAME_FIELD = "output_filename_field";
  public static final String APPEND_OUTPUT_FILENAME = "append_output_filename";
  public static final String CREATE_PARENT_FOLDER = "create_parent_folder";
  public static final String INCLUDE_STEP_NR_IN_OUTPUT_FILENAME = "include_step_nr_in_output_filename";
  public static final String INCLUDE_PART_NR_IN_OUTPUT_FILENAME = "include_part_nr_in_output_filename";
  public static final String INCLUDE_DATE_IN_OUTPUT_FILENAME = "include_date_in_output_filename";
  public static final String INCLUDE_TIME_IN_OUTPUT_FILENAME = "include_time_in_output_filename";
  public static final String SPECIFY_DATE_FORMAT_OUTPUT_FILENAME = "specify_date_format_output_filename";
  public static final String DATE_FORMAT_OUTPUT_FILENAME = "date_format_output_filename";
  public static final String ADD_OUTPUT_FILENAME_TO_RESULT = "add_output_filename_to_result";
  public static final String TOKEN_START_STRING = "token_start_string";
  public static final String TOKEN_END_STRING = "token_end_string";
  public static final String FIELD_NAME = "field_name";
  public static final String TOKEN_NAME = "token_name";
  public static final String INPUT_TEXT = "input_text";
  public static final String OUTPUT_FILE_ENCODING = "output_file_encoding";
  public static final String OUTPUT_SPLIT_EVERY = "output_split_every";
  public static final String OUTPUT_FILE_FORMAT = "output_file_format";
  private static Class<?> PKG = TokenReplacementMeta.class; // for i18n purposes, needed by Translator2!!


  public static final String[] INPUT_TYPES = { "text", "field", "file" };
  public static final String[] OUTPUT_TYPES = { "field", "file" };
  public static final String[] formatMapperLineTerminator = new String[] { "DOS", "UNIX", "CR", "None" };

  private String inputType;

  private String inputText;

  private String inputFieldName;

  private String inputFileName;

  private boolean inputFileNameInField;

  private String inputFileNameField;

  private boolean addInputFileNameToResult;

  private String outputType;

  private String outputFieldName;

  private String outputFileName;

  private boolean outputFileNameInField;

  private String outputFileNameField;

  private boolean appendOutputFileName;

  private boolean createParentFolder;

  private String outputFileFormat;

  private String outputFileEncoding;

  private int splitEvery;

  private boolean includeStepNrInOutputFileName;

  private boolean includePartNrInOutputFileName;

  private boolean includeDateInOutputFileName;

  private boolean includeTimeInOutputFileName;

  private boolean specifyDateFormatOutputFileName;

  private String dateFormatOutputFileName;

  private boolean addOutputFileNameToResult;

  private String tokenStartString;

  private String tokenEndString;

  private TokenReplacementField[] tokenReplacementFields;

  public TokenReplacementMeta() {
    super(); // allocate BaseStepMeta
    allocate(0);
  }

  public String getInputType() {
    return inputType;
  }

  public void setInputType( String inputType ) {
    this.inputType = inputType;
  }

  public String getInputText() {
    return inputText;
  }

  public void setInputText( String inputText ) {
    this.inputText = inputText;
  }

  public String getInputFieldName() {
    return inputFieldName;
  }

  public void setInputFieldName( String inputFieldName ) {
    this.inputFieldName = inputFieldName;
  }

  public String getInputFileName() {
    return inputFileName;
  }

  public void setInputFileName( String inputFileName ) {
    this.inputFileName = inputFileName;
  }

  public boolean isInputFileNameInField() {
    return inputFileNameInField;
  }

  public void setInputFileNameInField( boolean inputFileNameInField ) {
    this.inputFileNameInField = inputFileNameInField;
  }

  public String getInputFileNameField() {
    return inputFileNameField;
  }

  public void setInputFileNameField( String inputFileNameField ) {
    this.inputFileNameField = inputFileNameField;
  }

  public boolean isAddInputFileNameToResult() {
    return addInputFileNameToResult;
  }

  public void setAddInputFileNameToResult( boolean addInputFileNameToResult ) {
    this.addInputFileNameToResult = addInputFileNameToResult;
  }

  public String getOutputType() {
    return outputType;
  }

  public void setOutputType( String outputType ) {
    this.outputType = outputType;
  }

  public String getOutputFieldName() {
    return outputFieldName;
  }

  public void setOutputFieldName( String outputFieldName ) {
    this.outputFieldName = outputFieldName;
  }

  public String getOutputFileName() {
    return outputFileName;
  }

  public void setOutputFileName( String outputFileName ) {
    this.outputFileName = outputFileName;
  }

  public boolean isOutputFileNameInField() {
    return outputFileNameInField;
  }

  public void setOutputFileNameInField( boolean outputFileNameInField ) {
    this.outputFileNameInField = outputFileNameInField;
  }

  public String getOutputFileNameField() {
    return outputFileNameField;
  }

  public void setOutputFileNameField( String outputFileNameField ) {
    this.outputFileNameField = outputFileNameField;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setCreateParentFolder( boolean createParentFolder ) {
    this.createParentFolder = createParentFolder;
  }

  public String getOutputFileEncoding() {
    return Const.NVL( outputFileEncoding, Const.getEnvironmentVariable( "file.encoding", "UTF-8" ) );
  }

  public void setOutputFileEncoding( String outputFileEncoding ) {
    this.outputFileEncoding = outputFileEncoding;
  }

  public String getOutputFileFormat() {
    return outputFileFormat;
  }

  public String getOutputFileFormatString() {
    if( outputFileFormat == "DOS" )
    {
      return "\r\n";
    } else if ( outputFileFormat == "UNIX" )
    {
      return "\n";
    } else if ( outputFileFormat == "CR" )
    {
      return "\r";
    } else {
      return "";
    }
  }

  public void setOutputFileFormat( String outputFileFormat ) {
    this.outputFileFormat = outputFileFormat;
  }

  public int getSplitEvery() {
    return splitEvery;
  }

  public void setSplitEvery( int splitEvery ) {
    this.splitEvery = splitEvery;
  }

  public boolean isIncludeStepNrInOutputFileName() {
    return includeStepNrInOutputFileName;
  }

  public void setIncludeStepNrInOutputFileName( boolean includeStepNrInOutputFileName ) {
    this.includeStepNrInOutputFileName = includeStepNrInOutputFileName;
  }

  public boolean isIncludePartNrInOutputFileName() {
    return includePartNrInOutputFileName;
  }

  public void setIncludePartNrInOutputFileName( boolean includePartNrInOutputFileName ) {
    this.includePartNrInOutputFileName = includePartNrInOutputFileName;
  }

  public boolean isIncludeDateInOutputFileName() {
    return includeDateInOutputFileName;
  }

  public void setIncludeDateInOutputFileName( boolean includeDateInOutputFileName ) {
    this.includeDateInOutputFileName = includeDateInOutputFileName;
  }

  public boolean isIncludeTimeInOutputFileName() {
    return includeTimeInOutputFileName;
  }

  public void setIncludeTimeInOutputFileName( boolean includeTimeInOutputFileName ) {
    this.includeTimeInOutputFileName = includeTimeInOutputFileName;
  }

  public boolean isSpecifyDateFormatOutputFileName() {
    return specifyDateFormatOutputFileName;
  }

  public void setSpecifyDateFormatOutputFileName( boolean specifyDateFormatOutputFileName ) {
    this.specifyDateFormatOutputFileName = specifyDateFormatOutputFileName;
  }

  public String getDateFormatOutputFileName() {
    return dateFormatOutputFileName;
  }

  public void setDateFormatOutputFileName( String dateFormatOutputFileName ) {
    this.dateFormatOutputFileName = dateFormatOutputFileName;
  }

  public boolean isAddOutputFileNameToResult() {
    return addOutputFileNameToResult;
  }

  public void setAddOutputFileNameToResult( boolean addOutputFileNameToResult ) {
    this.addOutputFileNameToResult = addOutputFileNameToResult;
  }

  public String getTokenStartString() {
    return tokenStartString;
  }

  public void setTokenStartString( String tokenStartString ) {
    this.tokenStartString = tokenStartString;
  }

  public String getTokenEndString() {
    return tokenEndString;
  }

  public void setTokenEndString( String tokenEndString ) {
    this.tokenEndString = tokenEndString;
  }

  public TokenReplacementField[] getTokenReplacementFields() {
    return tokenReplacementFields;
  }

  public void setTokenReplacementFields( TokenReplacementField[] tokenReplacementFields ) {
    this.tokenReplacementFields = tokenReplacementFields;
  }

  public boolean isAppendOutputFileName() {
    return appendOutputFileName;
  }

  public void setAppendOutputFileName( boolean appendOutputFileName ) {
    this.appendOutputFileName = appendOutputFileName;
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
    throws KettleXMLException {
    readData( stepnode );
  }

  public void allocate( int nrfields ) {
    tokenReplacementFields = new TokenReplacementField[nrfields];
  }

  public Object clone() {
    TokenReplacementMeta retval = (TokenReplacementMeta) super.clone();
    int nrfields = tokenReplacementFields.length;

    retval.allocate( nrfields );

    for ( int i = 0; i < nrfields; i++ ) {
      retval.tokenReplacementFields[i] = (TokenReplacementField) tokenReplacementFields[i].clone();
    }

    return retval;
  }

  public void readData( Node stepnode ) throws KettleXMLException {
    try {

      inputType = XMLHandler.getTagValue( stepnode, INPUT_TYPE );
      inputText = XMLHandler.getTagValue( stepnode, INPUT_TEXT );
      inputFieldName = XMLHandler.getTagValue( stepnode, INPUT_FIELD_NAME );
      inputFileName = XMLHandler.getTagValue( stepnode, INPUT_FILENAME );
      inputFileNameInField = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, INPUT_FILENAME_IN_FIELD ), "" ) );
      inputFileNameField = XMLHandler.getTagValue( stepnode, INPUT_FILENAME_FIELD );
      addInputFileNameToResult = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, ADD_INPUT_FILENAME_TO_RESULT ), "" ) );

      outputType = XMLHandler.getTagValue( stepnode, OUTPUT_TYPE );
      outputFieldName = XMLHandler.getTagValue( stepnode, OUTPUT_FIELD_NAME );
      outputFileName = XMLHandler.getTagValue( stepnode, OUTPUT_FILENAME );
      outputFileNameInField = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, OUTPUT_FILENAME_IN_FIELD ), "" ) );
      outputFileNameField = XMLHandler.getTagValue( stepnode, OUTPUT_FILENAME_FIELD );
      appendOutputFileName = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, APPEND_OUTPUT_FILENAME ), "" ) );
      outputFileFormat = XMLHandler.getTagValue( stepnode, OUTPUT_FILE_FORMAT );
      outputFileEncoding = Const.NVL( XMLHandler.getTagValue( stepnode, OUTPUT_FILE_ENCODING ),
        Const.getEnvironmentVariable( "file.encoding", "UTF-8" ) );
      splitEvery = Const.toInt( XMLHandler.getTagValue( stepnode, OUTPUT_SPLIT_EVERY ), 0 );
      createParentFolder = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, CREATE_PARENT_FOLDER ), "" ) );
      includeStepNrInOutputFileName = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, INCLUDE_STEP_NR_IN_OUTPUT_FILENAME ), "" ) );
      includePartNrInOutputFileName = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, INCLUDE_PART_NR_IN_OUTPUT_FILENAME ), "" ) );
      includeDateInOutputFileName = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, INCLUDE_DATE_IN_OUTPUT_FILENAME ), "" ) );
      includeTimeInOutputFileName = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, INCLUDE_TIME_IN_OUTPUT_FILENAME ), "" ) );
      specifyDateFormatOutputFileName = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, SPECIFY_DATE_FORMAT_OUTPUT_FILENAME ), "" ) );
      dateFormatOutputFileName = XMLHandler.getTagValue( stepnode, DATE_FORMAT_OUTPUT_FILENAME );
      addOutputFileNameToResult = "Y".equalsIgnoreCase(
        Const.NVL( XMLHandler.getTagValue( stepnode, ADD_OUTPUT_FILENAME_TO_RESULT ), "" ) );

      tokenStartString = XMLHandler.getTagValue( stepnode, TOKEN_START_STRING );
      tokenEndString = XMLHandler.getTagValue( stepnode, TOKEN_END_STRING );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        tokenReplacementFields[i] = new TokenReplacementField();
        tokenReplacementFields[i].setName( XMLHandler.getTagValue( fnode, FIELD_NAME ) );
        tokenReplacementFields[i].setTokenName( XMLHandler.getTagValue( fnode, TOKEN_NAME ) );

      }
    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  public String getXML() {
    StringBuffer retval = new StringBuffer( 800 );

    retval.append( "    " + XMLHandler.addTagValue( INPUT_TYPE, inputType ) );
    retval.append( "    " + XMLHandler.addTagValue( INPUT_TEXT, inputText ) );
    retval.append( "    " + XMLHandler.addTagValue( INPUT_FIELD_NAME, inputFieldName) );
    retval.append( "    " + XMLHandler.addTagValue( INPUT_FILENAME, inputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( INPUT_FILENAME_IN_FIELD, inputFileNameInField ) );
    retval.append( "    " + XMLHandler.addTagValue( INPUT_FILENAME_FIELD, inputFileNameField ) );
    retval.append( "    " + XMLHandler.addTagValue( ADD_INPUT_FILENAME_TO_RESULT, addInputFileNameToResult ) );

    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_TYPE, outputType) );
    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_FIELD_NAME, outputFieldName ) );
    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_FILENAME, outputFileName) );
    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_FILENAME_IN_FIELD, outputFileNameInField ) );
    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_FILENAME_FIELD, outputFileNameField ) );
    retval.append( "    " + XMLHandler.addTagValue( APPEND_OUTPUT_FILENAME, appendOutputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( CREATE_PARENT_FOLDER, createParentFolder ) );
    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_FILE_FORMAT, outputFileFormat ) );
    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_FILE_ENCODING, outputFileEncoding ) );
    retval.append( "    " + XMLHandler.addTagValue( OUTPUT_SPLIT_EVERY, splitEvery ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_STEP_NR_IN_OUTPUT_FILENAME, includeStepNrInOutputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_PART_NR_IN_OUTPUT_FILENAME, includePartNrInOutputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_DATE_IN_OUTPUT_FILENAME, includeDateInOutputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( INCLUDE_TIME_IN_OUTPUT_FILENAME, includeTimeInOutputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( SPECIFY_DATE_FORMAT_OUTPUT_FILENAME, specifyDateFormatOutputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( DATE_FORMAT_OUTPUT_FILENAME, dateFormatOutputFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( ADD_OUTPUT_FILENAME_TO_RESULT, addOutputFileNameToResult ) );

    retval.append( "    " + XMLHandler.addTagValue( TOKEN_START_STRING, tokenStartString ) );
    retval.append( "    " + XMLHandler.addTagValue( TOKEN_END_STRING, tokenEndString ) );

    retval.append( "    <fields>" ).append( Const.CR );
    for ( int i = 0; i < tokenReplacementFields.length; i++ ) {
      TokenReplacementField field = tokenReplacementFields[i];

      if ( field.getName() != null && field.getName().length() != 0 ) {
        retval.append( "      <field>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( FIELD_NAME, field.getName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( TOKEN_NAME, field.getTokenName() ) );
        retval.append( "      </field>" ).append( Const.CR );
      }
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      inputType = rep.getStepAttributeString( id_step, INPUT_TYPE );
      inputText = rep.getStepAttributeString( id_step, INPUT_TEXT );
      inputFieldName = rep.getStepAttributeString( id_step, INPUT_FIELD_NAME );
      inputFileName = rep.getStepAttributeString( id_step, INPUT_FILENAME );
      inputFileNameInField = rep.getStepAttributeBoolean( id_step, INPUT_FILENAME_IN_FIELD );
      inputFileNameField = rep.getStepAttributeString( id_step, INPUT_FILENAME_FIELD );
      addInputFileNameToResult = rep.getStepAttributeBoolean( id_step, ADD_INPUT_FILENAME_TO_RESULT );

      outputType = rep.getStepAttributeString( id_step, OUTPUT_TYPE );
      outputFieldName = rep.getStepAttributeString( id_step, OUTPUT_FIELD_NAME );
      outputFileName = rep.getStepAttributeString( id_step, OUTPUT_FILENAME );
      outputFileNameInField = rep.getStepAttributeBoolean( id_step, OUTPUT_FILENAME_IN_FIELD );
      outputFileNameField = rep.getStepAttributeString( id_step, OUTPUT_FILENAME_FIELD );
      appendOutputFileName = rep.getStepAttributeBoolean( id_step, APPEND_OUTPUT_FILENAME );
      createParentFolder = rep.getStepAttributeBoolean( id_step, CREATE_PARENT_FOLDER );
      outputFileFormat = rep.getStepAttributeString( id_step, OUTPUT_FILE_FORMAT );
      outputFileEncoding = Const.NVL( rep.getStepAttributeString( id_step, OUTPUT_FILE_ENCODING ),
        Const.getEnvironmentVariable( "file.encoding", "UTF-8" ) );
      splitEvery = Const.toInt( rep.getStepAttributeString( id_step, OUTPUT_SPLIT_EVERY ), 0 );
      includeStepNrInOutputFileName = rep.getStepAttributeBoolean( id_step, INCLUDE_STEP_NR_IN_OUTPUT_FILENAME );
      includePartNrInOutputFileName = rep.getStepAttributeBoolean( id_step, INCLUDE_PART_NR_IN_OUTPUT_FILENAME );
      includeDateInOutputFileName = rep.getStepAttributeBoolean( id_step, INCLUDE_DATE_IN_OUTPUT_FILENAME );
      includeTimeInOutputFileName = rep.getStepAttributeBoolean( id_step, INCLUDE_TIME_IN_OUTPUT_FILENAME );
      specifyDateFormatOutputFileName = rep.getStepAttributeBoolean( id_step, SPECIFY_DATE_FORMAT_OUTPUT_FILENAME );
      dateFormatOutputFileName = rep.getStepAttributeString( id_step, DATE_FORMAT_OUTPUT_FILENAME );
      addOutputFileNameToResult = rep.getStepAttributeBoolean( id_step, ADD_OUTPUT_FILENAME_TO_RESULT );

      tokenStartString = rep.getStepAttributeString( id_step, TOKEN_START_STRING );
      tokenEndString = rep.getStepAttributeString( id_step, TOKEN_END_STRING );

      int nrfields = rep.countNrStepAttributes( id_step, FIELD_NAME );

      allocate( nrfields );

      for ( int i = 0; i < nrfields; i++ ) {
        tokenReplacementFields[i] = new TokenReplacementField();

        tokenReplacementFields[i].setName( rep.getStepAttributeString( id_step, i, FIELD_NAME ) );
        tokenReplacementFields[i].setTokenName( rep.getStepAttributeString( id_step, i, TOKEN_NAME ) );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, INPUT_TYPE, inputType );
      rep.saveStepAttribute( id_transformation, id_step, INPUT_TEXT, inputText );
      rep.saveStepAttribute( id_transformation, id_step, INPUT_FIELD_NAME, inputFieldName );
      rep.saveStepAttribute( id_transformation, id_step, INPUT_FILENAME, inputFileName );
      rep.saveStepAttribute( id_transformation, id_step, INPUT_FILENAME_IN_FIELD, inputFileNameInField );
      rep.saveStepAttribute( id_transformation, id_step, INPUT_FILENAME_FIELD, inputFileNameField );
      rep.saveStepAttribute( id_transformation, id_step, ADD_INPUT_FILENAME_TO_RESULT, addInputFileNameToResult );

      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_TYPE, outputType );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FIELD_NAME, outputFieldName );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FILENAME, outputFileName );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FILENAME_IN_FIELD, outputFileNameInField );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FILENAME_FIELD, outputFileNameField );
      rep.saveStepAttribute( id_transformation, id_step, APPEND_OUTPUT_FILENAME, appendOutputFileName );
      rep.saveStepAttribute( id_transformation, id_step, CREATE_PARENT_FOLDER, createParentFolder );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FILE_FORMAT, outputFileFormat );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_FILE_ENCODING, outputFileEncoding );
      rep.saveStepAttribute( id_transformation, id_step, OUTPUT_SPLIT_EVERY, splitEvery );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_STEP_NR_IN_OUTPUT_FILENAME, includeStepNrInOutputFileName );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_PART_NR_IN_OUTPUT_FILENAME, includePartNrInOutputFileName );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_DATE_IN_OUTPUT_FILENAME, includeDateInOutputFileName );
      rep.saveStepAttribute( id_transformation, id_step, INCLUDE_TIME_IN_OUTPUT_FILENAME, includeTimeInOutputFileName );
      rep.saveStepAttribute( id_transformation, id_step, SPECIFY_DATE_FORMAT_OUTPUT_FILENAME, specifyDateFormatOutputFileName );
      rep.saveStepAttribute( id_transformation, id_step, DATE_FORMAT_OUTPUT_FILENAME, dateFormatOutputFileName );
      rep.saveStepAttribute( id_transformation, id_step, ADD_OUTPUT_FILENAME_TO_RESULT, addOutputFileNameToResult );

      rep.saveStepAttribute( id_transformation, id_step, TOKEN_START_STRING, tokenStartString );
      rep.saveStepAttribute( id_transformation, id_step, TOKEN_END_STRING, tokenEndString );

      for ( int i = 0; i < tokenReplacementFields.length; i++ ) {
        TokenReplacementField field = tokenReplacementFields[i];

        rep.saveStepAttribute( id_transformation, id_step, i, FIELD_NAME, field.getName() );
        rep.saveStepAttribute( id_transformation, id_step, i, TOKEN_NAME, field.getTokenName() );
      }
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  public void setDefault() {

    inputType = "Text";
    inputFileNameInField = false;
    addInputFileNameToResult = false;

    outputType = "Field";
    outputFileNameInField = false;
    appendOutputFileName = false;
    createParentFolder = false;
    includeStepNrInOutputFileName = false;
    includePartNrInOutputFileName = false;
    includeDateInOutputFileName = false;
    includeTimeInOutputFileName = false;
    specifyDateFormatOutputFileName = false;
    addOutputFileNameToResult = false;
    outputFileEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
    outputFileFormat = Const.isWindows() ? "DOS" : "UNIX";
    splitEvery = 0;

    tokenStartString = "${";
    tokenEndString = "}";

  }

  public String buildFilename( String fileName, VariableSpace space, int stepnr, String partnr, int splitnr ) {
    return buildFilename( fileName, space, stepnr, partnr, splitnr, this );
  }

  public String buildFilename( String filename, VariableSpace space, int stepnr, String partnr,
    int splitnr, TokenReplacementMeta meta ) {
    SimpleDateFormat daf = new SimpleDateFormat();

    // Replace possible environment variables...
    String realFileName = space.environmentSubstitute( filename );
    String extension = "";
    String retval = "";
    if( realFileName.contains(".") )
    {
    	retval = realFileName.substring( 0 , realFileName.lastIndexOf(".") );
    	extension = realFileName.substring( realFileName.lastIndexOf(".") +1 ); 
    } else {
    	retval = realFileName;
    }
    
    
    Date now = new Date();

    if ( meta.isSpecifyDateFormatOutputFileName() && !Const.isEmpty( meta.getDateFormatOutputFileName() ) ) {
      daf.applyPattern( meta.getDateFormatOutputFileName() );
      String dt = daf.format( now );
      retval += dt;
    } else {
      if ( meta.isIncludeDateInOutputFileName() ) {
        daf.applyPattern( "yyyMMdd" );
        String d = daf.format( now );
        retval += "_" + d;
      }
      if ( meta.isIncludeTimeInOutputFileName() ) {
        daf.applyPattern( "HHmmss" );
        String t = daf.format( now );
        retval += "_" + t;
      }
    }
    if ( meta.isIncludeStepNrInOutputFileName() ) {
      retval += "_" + stepnr;
    }
    if ( meta.isIncludePartNrInOutputFileName() ) {
      retval += "_" + partnr;
    }

    if( meta.getSplitEvery() > 0 )
    {
      retval += "_" + splitnr;
    }

    if ( extension != null && extension.length() != 0 ) {
      retval += "." + extension;
    }
    return retval;
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    CheckResult cr;

    // Check output fields
    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "TokenReplacementMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
      remarks.add( cr );

      String error_message = "";
      boolean error_found = false;

      // Starting from selected fields in ...
      for ( int i = 0; i < tokenReplacementFields.length; i++ ) {
        int idx = prev.indexOfValue( tokenReplacementFields[i].getName() );
        if ( idx < 0 ) {
          error_message += "\t\t" + tokenReplacementFields[i].getName() + Const.CR;
          error_found = true;
        }
      }
      if ( error_found ) {
        error_message =
          BaseMessages.getString( PKG, "TokenReplacementMeta.CheckResult.FieldsNotFound", error_message );
        cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "TokenReplacementMeta.CheckResult.AllFieldsFound" ), stepMeta );
        remarks.add( cr );
      }
    }

    //Make sure token replacement is populated!
    if( Const.isEmpty( tokenStartString ) || Const.isEmpty( tokenEndString ) )
    {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
        PKG, "TokenReplacementMeta.CheckResult.ExpectedTokenReplacementError" ), stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
        PKG, "TokenReplacementMeta.CheckResult.ExpectedTokenReplacementOk" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "TokenReplacementMeta.CheckResult.ExpectedInputOk" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "TokenReplacementMeta.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }

    cr =
      new CheckResult( CheckResultInterface.TYPE_RESULT_COMMENT, BaseMessages.getString(
        PKG, "TokenReplacementMeta.CheckResult.FilesNotChecked" ), stepMeta );
    remarks.add( cr );
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans trans ) {
    return new TokenReplacement( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new TokenReplacementData();
  }

  public void getFields( RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    // change the case insensitive flag too

    if ( outputType.equalsIgnoreCase( "field" ) ) {
      ValueMetaInterface v = new ValueMeta( space.environmentSubstitute( outputFieldName ), ValueMetaInterface.TYPE_STRING );
      v.setOrigin( name );
      row.addValueMeta( v );
    }
  }

  @Override
  public StepMetaInjectionInterface getStepMetaInjectionInterface() {
    return new TokenReplacementMetaInjection( this );
  }


}
