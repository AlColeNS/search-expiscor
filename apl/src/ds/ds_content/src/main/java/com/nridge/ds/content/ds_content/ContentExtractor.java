/*
 * NorthRidge Software, LLC - Copyright (c) 2015.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.nridge.ds.content.ds_content;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tika.Tika;
import org.apache.tika.fork.ForkParser;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.slf4j.Logger;

import java.io.*;
import java.net.URL;
import java.nio.file.Paths;

/**
 * The ContentExtractor class is responsible for extracting textual content
 * from a file.  The Apache Tika™ toolkit detects and extracts metadata and
 * text content from various documents - from PPT to CSV to PDF - using
 * existing parser libraries. Tika unifies these parsers under a single
 * interface to allow you to easily parse over a thousand different file
 * types. Tika is useful for search engine indexing, content analysis,
 * translation, and much more.
 *
 * @see <a href="http://tika.apache.org/">Apache Tika</a>
 * @see <a href="http://www.massapi.com/class/org/apache/tika/fork/ForkParser.html">Apache Tika ForkParser</a>
 * @see <a href="http://www.tutorialspoint.com/tika/tika_quick_guide.htm">Apache Tika Tutorial</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class ContentExtractor
{
    private DataBag mBag;
    private final AppMgr mAppMgr;
    private String mCfgPropertyPrefix = Content.CFG_PROPERTY_PREFIX;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the content extractor accordingly.
     *
     * @param anAppMgr Application manager instance.
     */
    public ContentExtractor(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
    }

    /**
     * Constructor accepts an application manager parameter and initializes
     * the content extractor accordingly.
     *
     * @param anAppMgr Application manager instance.
     * @param aBag Data bag instance to populate with meta data.
     */
    public ContentExtractor(AppMgr anAppMgr, DataBag aBag)
    {
        mAppMgr = anAppMgr;
        mBag = aBag;
    }

    /**
     * Returns the configuration property prefix string.
     *
     * @return Property prefix string.
     */
    public String getCfgPropertyPrefix()
    {
        return mCfgPropertyPrefix;
    }

    /**
     * Assigns the configuration property prefix to the document data source.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     * @return Matching property value.
     */
    public String getCfgString(String aSuffix)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.  If the property is
     * not found, then the default value parameter will be returned.
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    public String getCfgString(String aSuffix, String aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the application manager configuration
     * property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    /**
     * Quick test to determine if the file is valid for content
     * extraction.
     *
     * @param aFile File instance.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFileValid(File aFile)
    {
        if ((aFile != null) && (aFile.exists()))
        {
            long fileSize = aFile.length();
            if (fileSize > 0L)
                return true;
        }

        return false;
    }

    /**
     * Quick test to determine if the file is valid for content
     * extraction.
     *
     * @param aPathFileName Path/File name.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFileValid(String aPathFileName)
    {
        return isFileValid(new File(aPathFileName));
    }

    /**
     * Uses the Tika subsystem to detect the file type.  The details of
     * that detection approach are described on the Content Detection
     * web page.
     *
     * @param aFile File instance.
     *
     * @return String representation of the file type.
     *
     * @see <a href="http://tika.apache.org/1.6/detection.html">Content Detection</a>
     *
     */
    public String detectType(File aFile)
    {
        Logger appLogger = mAppMgr.getLogger(this, "detectType");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String contentType = Content.CONTENT_TYPE_DEFAULT;

        if (isFileValid(aFile))
        {
            Tika tikaFacade = new Tika();
            try
            {
                contentType = tikaFacade.detect(aFile);
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s: %s", aFile.getAbsolutePath(), e.getMessage());
                appLogger.error(msgStr, e);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return contentType;
    }

    /**
     * Uses the Tika subsystem to detect the file type.  The details of
     * that detection approach are described on the Content Detection
     * web page.
     *
     * @param aURL URL of the resource.
     *
     * @return String representation of the file type.
     *
     * @see <a href="http://tika.apache.org/1.6/detection.html">Content Detection</a>
     */
    public String detectType(URL aURL)
    {
        Logger appLogger = mAppMgr.getLogger(this, "detectType");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String contentType = Content.CONTENT_TYPE_DEFAULT;

        if (aURL != null)
        {
            Tika tikaFacade = new Tika();
            try
            {
                contentType = tikaFacade.detect(aURL);
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s: %s", aURL.toString(), e.getMessage());
                appLogger.error(msgStr, e);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return contentType;
    }

    /**
     * Uses the Tika subsystem to detect the file type.  The details of
     * that detection approach are described on the Content Detection
     * web page.
     * The type detection is based on known file name extensions.
     * <p>
     * The given name can also be a URL or a full file path. In such cases
     * only the file name part of the string is used for type detection.
     * </p>
     *
     * @param aName Name of the document.
     *
     * @return String representation of the file type.
     *
     * @see <a href="http://tika.apache.org/1.6/detection.html">Content Detection</a>
     */
    public String detectType(String aName)
    {
        Logger appLogger = mAppMgr.getLogger(this, "detectType");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String contentType = Content.CONTENT_TYPE_DEFAULT;

        if (StringUtils.isNotEmpty(aName))
        {
            Tika tikaFacade = new Tika();
            contentType = tikaFacade.detect(aName);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return contentType;
    }

    private void addAssignField(String aFieldName, String aFieldValue)
    {
        if (mBag != null)
        {
            DataField dataField = mBag.getFieldByName(aFieldName);
            if (dataField == null)
            {
                dataField = new DataTextField(aFieldName, Field.nameToTitle(aFieldName), aFieldValue);
                mBag.add(dataField);
            }
            else
                dataField.setValue(aFieldValue);
        }
    }

    /**
     * This method will extract the textual content from the input file
     * and write it to the writer stream.  If a bag instance has been
     * registered with the class, then meta data fields will dynamically
     * be assigned as they are discovered.
     *
     * @param anInFile Input file instance.
     * @param aWriter Output writer stream.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    @SuppressWarnings("deprecation")
    public void process(File anInFile, Writer aWriter)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isFileValid(anInFile))
        {
            appLogger.debug(String.format("[%s] %s", detectType(anInFile), anInFile.getAbsolutePath()));

            ForkParser forkParser = null;
            Metadata tikaMetaData = new Metadata();
            tikaMetaData.set(Metadata.RESOURCE_NAME_KEY, anInFile.getName());
            int contentLimit = getCfgInteger("content_limit", Content.CONTENT_LIMIT_DEFAULT);

            InputStream inputStream = null;
            try
            {
                Parser tikaParser;
                ParseContext parseContext;

                inputStream = TikaInputStream.get(anInFile.toPath());
                if (isCfgStringTrue("tika_fork_parser"))
                {
                    forkParser = new ForkParser(ContentExtractor.class.getClassLoader(), new AutoDetectParser());
                    String javaCmdStr = getCfgString("tika_fork_java_cmd");
                    if (StringUtils.isNotEmpty(javaCmdStr))
                        forkParser.setJavaCommand(javaCmdStr);
                    int poolSize = getCfgInteger("tika_fork_pool_size", 5);
                    if (poolSize > 0)
                        forkParser.setPoolSize(poolSize);
                    tikaParser = forkParser;
                    parseContext = new ParseContext();
                }
                else
                {
                    tikaParser = new AutoDetectParser();
                    parseContext = new ParseContext();
                    Parser recursiveMetadataParser = new RecursiveMetadataParser(tikaParser);
                    parseContext.set(Parser.class, recursiveMetadataParser);
                }

                WriteOutContentHandler writeOutContentHandler = new WriteOutContentHandler(aWriter, contentLimit);
                tikaParser.parse(inputStream, writeOutContentHandler, tikaMetaData, parseContext);
            }
            catch (Exception e)
            {
                String eMsg = e.getMessage();
                String msgStr = String.format("%s: %s", anInFile.getAbsolutePath(), eMsg);

/* The following logic checks to see if this exception was triggered simply because
the total character limit threshold was hit.  If that is all it was, then return true. */

                if (StringUtils.startsWith(eMsg, "Your document contained more than"))
                    appLogger.warn(msgStr);
                else
                    throw new NSException(msgStr);
            }
            finally
            {
                if (inputStream != null)
                    IOUtils.closeQuietly(inputStream);
            }

            if ((mBag != null) && (isCfgStringTrue("content_metadata")))
            {
                String mdValue;
                String[] metaDataNames = tikaMetaData.names();
                for (String mdName : metaDataNames)
                {
                    mdValue = tikaMetaData.get(mdName);
                    if (StringUtils.isNotEmpty(mdValue))
                        addAssignField(Content.CONTENT_FIELD_METADATA + mdName, mdValue);
                }
            }

            if (forkParser != null)
                forkParser.close();
        }
        else
        {
            String msgStr = String.format("%s: Does not exist or is empty.", anInFile.getAbsolutePath());
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * This method will extract the textual content from the URL
     * and write it to the writer stream.  If a bag instance has been
     * registered with the class, then meta data fields will dynamically
     * be assigned as they are discovered.
     *
     * @param aURL URL of the resource.
     * @param aWriter Output writer stream.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    @SuppressWarnings("deprecation")
    public void process(URL aURL, Writer aWriter)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aURL == null) || (aWriter == null))
            throw new NSException("One or more parameters are null.");

        String documentName = aURL.toString();
        appLogger.debug(String.format("[%s] %s", detectType(aURL), documentName));

        Metadata tikaMetaData = new Metadata();
        int contentLimit = getCfgInteger("content_limit", Content.CONTENT_LIMIT_DEFAULT);

        InputStream inputStream = null;
        try
        {
            Parser tikaParser;
            ParseContext parseContext;

            inputStream = TikaInputStream.get(aURL);
            if (isCfgStringTrue("tika_fork_parser"))
            {
                ForkParser forkParser = new ForkParser(ContentExtractor.class.getClassLoader(), new AutoDetectParser());
                String javaCmdStr = getCfgString("tika_fork_java_cmd");
                if (StringUtils.isNotEmpty(javaCmdStr))
                    forkParser.setJavaCommand(javaCmdStr);
                int poolSize = getCfgInteger("tika_fork_pool_size", 5);
                if (poolSize > 0)
                    forkParser.setPoolSize(poolSize);
                tikaParser = forkParser;
                parseContext = new ParseContext();
            }
            else
            {
                tikaParser = new AutoDetectParser();
                parseContext = new ParseContext();
                Parser recursiveMetadataParser = new RecursiveMetadataParser(tikaParser);
                parseContext.set(Parser.class, recursiveMetadataParser);
            }

            WriteOutContentHandler writeOutContentHandler = new WriteOutContentHandler(aWriter, contentLimit);
            tikaParser.parse(inputStream, writeOutContentHandler, tikaMetaData, parseContext);
        }
        catch (Exception e)
        {
            String eMsg = e.getMessage();
            String msgStr = String.format("%s: %s", documentName, eMsg);

/* The following logic checks to see if this exception was triggered simply because
the total character limit threshold was hit.  If that is all it was, then return true. */

            if (StringUtils.startsWith(eMsg, "Your document contained more than"))
                appLogger.warn(msgStr);
            else
                throw new NSException(msgStr);
        }
        finally
        {
            if (inputStream != null)
                IOUtils.closeQuietly(inputStream);
        }

        if ((mBag != null) && (isCfgStringTrue("content_metadata")))
        {
            String mdValue;
            String[] metaDataNames = tikaMetaData.names();
            for (String mdName : metaDataNames)
            {
                mdValue = tikaMetaData.get(mdName);
                if (StringUtils.isNotEmpty(mdValue))
                    addAssignField(Content.CONTENT_FIELD_METADATA + mdName, mdValue);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * This method will extract the textual content from the input file
     * and write it to the output file.  If a bag instance has been
     * registered with the class, then meta data fields will dynamically
     * be assigned as they are discovered.
     *
     * @param anInFile Input file instance.
     * @param anOutFile Output file instance.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    public void process(File anInFile, File anOutFile)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        BufferedWriter bufferedWriter;
        String contentEncoding = getCfgString("content_encoding", StrUtl.CHARSET_UTF_8);
        try (FileOutputStream fileOutputStream = new FileOutputStream(anOutFile))
        {
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileOutputStream, contentEncoding));
            process(anInFile, bufferedWriter);
        }
        catch (IOException e)
        {
            String msgStr = String.format("%s: %s", anInFile.getAbsolutePath(), e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * This method will extract the textual content from the input file
     * name and write it to the output file name.  If a bag instance has been
     * registered with the class, then meta data fields will dynamically
     * be assigned as they are discovered.
     *
     * @param anInputPathFileName Input path/file name.
     * @param anOutputPathFileName Output path/file name.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    public void process(String anInputPathFileName, String anOutputPathFileName)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        process(new File(anInputPathFileName), new File(anOutputPathFileName));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * This method will extract the textual content from the input file
     * and capture it in a string.  If a bag instance has been registered
     * with the class, then meta data fields will dynamically be assigned
     * as they are discovered.
     *
     * @param anInFile Input file instance.
     *
     * @return String representation of the textual content.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    public String process(File anInFile)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            process(anInFile, printWriter);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringWriter.toString();
    }

    /**
     * This method will extract the textual content from the URL and
     * capture it in a string.  If a bag instance has been registered
     * with the class, then meta data fields will dynamically be assigned
     * as they are discovered.
     *
     * @param aURL URL of the resource.
     *
     * @return String representation of the textual content.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    public String process(URL aURL)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            process(aURL, printWriter);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringWriter.toString();
    }

    /**
     * This method will extract the textual content from the input file
     * and capture it in a string.  If a bag instance has been registered
     * with the class, then meta data fields will dynamically be assigned
     * as they are discovered.
     *
     * @param anInputPathFileName Input path/file name.
     *
     * @return String representation of the textual content.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    public String process(String anInputPathFileName)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String contentString = process(new File(anInputPathFileName));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return contentString;
    }

    /**
     * This method will extract the textual content from the input file
     * and capture it in the content field.  If a bag instance has been
     * registered with the class, then meta data fields will dynamically
     * be assigned as they are discovered.
     *
     * @param anInputPathFileName Input path/file name.
     * @param aContentField Content data field instance.
     *
     * @throws NSException Thrown when IOExceptions are detected.
     */
    public void process(String anInputPathFileName, DataField aContentField)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aContentField == null)
            throw new NSException("Content data field is null.");

        String contentString = process(anInputPathFileName);
        if (StringUtils.isNotEmpty(contentString))
            aContentField.setValue(contentString);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
