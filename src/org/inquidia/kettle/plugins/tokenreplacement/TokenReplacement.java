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

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.vfs.FileObject;
import org.codehaus.groovy.runtime.StringBufferWriter;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ResultFile;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * @author Chris
 * @since 1-nov-2014
 */
public class TokenReplacement extends BaseStep implements StepInterface {
  private static Class<?> PKG = TokenReplacementMeta.class; // for i18n purposes, needed by Translator2!!

  public TokenReplacementMeta meta;

  public TokenReplacementData data;

  public TokenReplacement( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                           Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (TokenReplacementMeta) smi;
    data = (TokenReplacementData) sdi;

    boolean result = true;
    Object[] r = getRow(); // This also waits for a row to be finished.

    if ( first && r!=null ) {
      first = false;

      data.inputRowMeta = getInputRowMeta();
      data.outputRowMeta = getInputRowMeta().clone();

      if( meta.getOutputType().equalsIgnoreCase( "field" ) )
      {
        meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

      }
      if( meta.getOutputType().equalsIgnoreCase( "file" ) && !meta.isOutputFileNameInField() )
      {
        if( meta.getOutputFileName() != null )
        {
          String filename = meta.buildFilename( meta.getOutputFileName(), getTransMeta(), getCopy(), getPartitionID(), data.splitnr );
          openNewOutputFile( filename );
        } else {
          throw new KettleException( "Output file name cannot be null." );
        }
      }
    }

    if ( r == null ) {
      // no more input to be expected...
      closeAllOutputFiles();
      setOutputDone();
      return false;
    }

    if( meta.getOutputType().equalsIgnoreCase( "file" ) && !meta.isOutputFileNameInField() && meta.getSplitEvery() > 0
      && data.rowNumber % meta.getSplitEvery() == 0 )
    {
      if( data.rowNumber > 0 )
      {
        closeAllOutputFiles();
        data.splitnr++;
        String filename = meta.buildFilename( meta.getOutputFileName(), getTransMeta(), getCopy(), getPartitionID(), data.splitnr );
        openNewOutputFile( filename );
      }
    }

    String outputFilename = "";
    if( meta.getOutputType().equalsIgnoreCase( "file" ) && !meta.isOutputFileNameInField() )
    {
      outputFilename = meta.buildFilename( meta.getOutputFileName(), getTransMeta(), getCopy(), getPartitionID(), data.splitnr );
    } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) && meta.isOutputFileNameInField() ) {
      String filenameValue = data.inputRowMeta.getString( r, environmentSubstitute( meta.getOutputFileNameField() ), "" );
      if( !Const.isEmpty( filenameValue ) )
      {
        outputFilename = filenameValue;
      } else {
        throw new KettleException( "Filename cannot be empty." );
      }
    }

    //Create token resolver
    TokenResolver resolver = new TokenResolver();

    for( TokenReplacementField field : meta.getTokenReplacementFields() )
    {
      if( data.inputRowMeta.indexOfValue( field.getName() ) >= 0 ) {
        String fieldValue = environmentSubstitute( data.inputRowMeta.getString( r, field.getName(), null ) );
        if( fieldValue == null && !BooleanUtils.toBoolean( Const.getEnvironmentVariable( "KETTLE_EMPTY_STRING_DIFFERS_FROM_NULL", "N" ) ) )
        {
          fieldValue = Const.nullToEmpty( fieldValue );
        }
        resolver.addToken( field.getTokenName(), fieldValue );
      } else {
        throw new KettleValueException( "Field " + field.getName() + " not found on input stream." );
      }
    }

    Reader reader;
    String inputFilename = "";

    if( meta.getInputType().equalsIgnoreCase( "text" ) )
    {
      reader = new TokenReplacingReader( resolver, new StringReader( environmentSubstitute( meta.getInputText() ) ),
        environmentSubstitute( meta.getTokenStartString() ), environmentSubstitute( meta.getTokenEndString() ) );

    } else if ( meta.getInputType().equalsIgnoreCase( "field" ) )
    {
      if( data.inputRowMeta.indexOfValue( meta.getInputFieldName() ) >= 0 )
      {
        String inputString = data.inputRowMeta.getString( r, meta.getInputFieldName(), "" );
        reader = new TokenReplacingReader( resolver, new StringReader( inputString ),
          environmentSubstitute( meta.getTokenStartString() ), environmentSubstitute( meta.getTokenEndString() ) );

      } else {
        throw new KettleValueException( "Input field " + meta.getInputFieldName() + " not found on input stream." );
      }
    } else if ( meta.getInputType().equalsIgnoreCase( "file" ) )
    {
      if( meta.isInputFileNameInField() )
      {
        if( data.inputRowMeta.indexOfValue( environmentSubstitute( meta.getInputFileNameField() ) ) >= 0 )
        {
          inputFilename = data.inputRowMeta.getString( r, environmentSubstitute( meta.getInputFileNameField() ), "" );
        } else {
          throw new KettleValueException( "Input filename field " + environmentSubstitute( meta.getInputFileNameField() )
            + " not found on input stream." );
        }
      } else {
        inputFilename = environmentSubstitute( meta.getInputFileName() );
      }

      if( Const.isEmpty( inputFilename ) )
      {
        throw new KettleValueException( "Input filename cannot be empty" );
      }

      FileObject file = KettleVFS.getFileObject( inputFilename, getTransMeta() );
      reader = new TokenReplacingReader( resolver, new InputStreamReader( KettleVFS.getInputStream( inputFilename,
        getTransMeta() ) ), environmentSubstitute( meta.getTokenStartString() ),
        environmentSubstitute( meta.getTokenEndString() ) );

      if( meta.isAddInputFileNameToResult() )
      {
        ResultFile resultFile =
          new ResultFile( ResultFile.FILE_TYPE_GENERAL, KettleVFS.getFileObject( inputFilename, getTransMeta() ),
            getTransMeta().getName(), getStepname() );
        resultFile.setComment( BaseMessages.getString( PKG, "TokenReplacement.AddInputResultFile" ) );
        addResultFile( resultFile );
      }
    } else {
      throw new KettleException( "Unsupported input type " + meta.getInputType() );
    }

    Writer stringWriter = null;
    OutputStream bufferedWriter = null;

    if( meta.getOutputType().equalsIgnoreCase( "field" ) )
    {
      stringWriter = new StringBufferWriter( new StringBuffer( 5000 ) );
    } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) )
    {

      if( inputFilename.equals( outputFilename ) )
      {
        throw new KettleException( "Input and output filenames must not be the same " + inputFilename );
      }

      int fileIndex = data.openFiles.indexOf( outputFilename );
      if( fileIndex < 0 )
      {
        openNewOutputFile( outputFilename );
        fileIndex = data.openFiles.indexOf( outputFilename );
      }

      bufferedWriter = data.openBufferedWriters.get( fileIndex );

    } else {
      throw new KettleException( "Unsupported output type " + meta.getOutputType() );
    }

    String output = "";

    try {
      char[] cbuf = new char[ 5000 ];
      StringBuffer sb = new StringBuffer(  );
      int length = 0;
      while ( ( length = reader.read( cbuf ) ) > 0 )
      {
        if( meta.getOutputType().equalsIgnoreCase( "field" ) )
        {
          stringWriter.write( cbuf, 0, length );
        } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) )
        {
          CharBuffer cBuffer = CharBuffer.wrap( cbuf, 0, length );
          ByteBuffer bBuffer = Charset.forName( meta.getOutputFileEncoding() ).encode( cBuffer );
          byte[] bytes = new byte[ bBuffer.limit() ];
          bBuffer.get( bytes );
          bufferedWriter.write( bytes );

        } //No else.  Anything else will be thrown to a Kettle exception prior to getting here.
        cbuf = new char[ 5000 ];
      }

      if( meta.getOutputType().equalsIgnoreCase( "field" ) )
      {
        output += stringWriter.toString();
      } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) ) {
        bufferedWriter.write( meta.getOutputFileFormatString().getBytes() );
      }
    } catch ( IOException ex ) {
      throw new KettleException( ex.getMessage(), ex );
    } finally {
      try {
        reader.close();
        if( stringWriter != null )
        {
          stringWriter.close();
        }

        reader = null;
        stringWriter = null;

      } catch (IOException ex)
      {
        throw new KettleException( ex.getMessage(), ex );
      }

    }


    if( meta.getOutputType().equalsIgnoreCase( "field" ) ) {
      r = RowDataUtil.addValueData( r, data.outputRowMeta.size() - 1, output );
    } else if ( meta.getOutputType().equalsIgnoreCase( "file" ) )
    {
      incrementLinesWritten();
    }

    putRow( data.outputRowMeta, r ); // in case we want it to go further...
    data.rowNumber ++;
    if ( checkFeedback( getLinesOutput() ) ) {
      logBasic( "linenr " + getLinesOutput() );
    }

    return result;
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (TokenReplacementMeta) smi;
    data = (TokenReplacementData) sdi;

    if ( super.init( smi, sdi ) ) {
      return true;
    }

    return false;
  }


  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (TokenReplacementMeta) smi;
    data = (TokenReplacementData) sdi;

    try {
      closeAllOutputFiles();
    } catch( KettleException ex ) {}

	  super.dispose( smi, sdi );
  }

  public void openNewOutputFile( String filename ) throws KettleException
  {
    if( data.openFiles.contains( filename ) )
    {
      logDetailed( "File " + filename + " is already open." );
      return;
    }
    if( meta.isCreateParentFolder() )
    {
      try {
        createParentFolder( filename );
      } catch ( Exception ex ) {
        throw new KettleException( ex.getMessage(), ex );
      }
    }

    boolean fileExists = KettleVFS.fileExists( filename );

    OutputStream writer = KettleVFS.getOutputStream( filename, meta.isAppendOutputFileName() );
    OutputStream bufferedWriter = new BufferedOutputStream( writer, 5000 );

    try {
      if ( fileExists && meta.isAppendOutputFileName() ) {
        bufferedWriter.write( meta.getOutputFileFormatString().getBytes() );
      }
    } catch ( IOException ex )
    {
      throw new KettleException( ex.getMessage(), ex );
    }

    data.openFiles.add( filename );
    data.openWriters.add( writer );
    data.openBufferedWriters.add( bufferedWriter );

  }

  public void closeAllOutputFiles() throws KettleException
  {
    try {
      Iterator<OutputStream> itWriter = data.openWriters.iterator();
      Iterator<OutputStream> itBufferedWriter = data.openBufferedWriters.iterator();
      Iterator<String> itFilename = data.openFiles.iterator();
      if(  meta.isAddOutputFileNameToResult() )
      {
        while( itFilename.hasNext() ) {
          ResultFile resultFile =
            new ResultFile( ResultFile.FILE_TYPE_GENERAL, KettleVFS.getFileObject( itFilename.next(), getTransMeta() ),
              getTransMeta()
                .getName(), getStepname() );
          resultFile.setComment( BaseMessages.getString( PKG, "TokenReplacement.AddOutputResultFile" ) );
          addResultFile( resultFile );
        }
      }


      while( itBufferedWriter.hasNext() )
      {
        OutputStream bufferedWriter = itBufferedWriter.next();
        if( bufferedWriter != null )
        {
          bufferedWriter.flush();
          bufferedWriter.close();
        }
      }

      while( itWriter.hasNext() )
      {
        OutputStream writer = itWriter.next();
        if( writer != null )
        {
          writer.close();
        }
      }

      data.openBufferedWriters.clear();
      data.openWriters.clear();
      data.openFiles.clear();
    } catch ( IOException ex )
    {
      throw new KettleException( ex.getMessage(), ex );
    }
  }


  private void createParentFolder( String filename ) throws Exception {
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = KettleVFS.getFileObject( filename ).getParent();
      if ( parentfolder.exists() ) {
        if ( isDetailed() ) {
          logDetailed( BaseMessages
            .getString( PKG, "TokenReplacement.Log.ParentFolderExist", parentfolder.getName() ) );
        }
      } else {
        if ( isDetailed() ) {
          logDetailed( BaseMessages.getString( PKG, "TokenReplacement.Log.ParentFolderNotExist", parentfolder
            .getName() ) );
        }
        if ( meta.isCreateParentFolder() ) {
          parentfolder.createFolder();
          if ( isDetailed() ) {
            logDetailed( BaseMessages.getString( PKG, "TokenReplacement.Log.ParentFolderCreated", parentfolder
              .getName() ) );
          }
        } else {
          throw new KettleException( BaseMessages.getString(
            PKG, "TokenReplacement.Log.ParentFolderNotExistCreateIt", parentfolder.getName(), filename ) );
        }
      }
      
    } finally {
      if ( parentfolder != null ) {
        try {
          parentfolder.close();
        } catch ( Exception ex ) {
          // Ignore
        }
      }
    }
  }


}
