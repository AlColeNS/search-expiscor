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

package com.nridge.connector.fs.con_fs.core;

import com.nridge.connector.common.con_com.Connector;
import com.nridge.connector.common.con_com.crawl.CrawlFollow;
import com.nridge.connector.common.con_com.crawl.CrawlIgnore;
import com.nridge.connector.common.con_com.crawl.CrawlQueue;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.xml.DocumentXML;
import com.nridge.core.base.std.NSException;
import com.nridge.ds.content.ds_content.Content;
import com.nridge.ds.content.ds_content.ContentExtractor;
import com.nridge.ds.solr.Solr;
import com.nridge.ds.solr.SolrDS;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

/**
 * The FileCrawler uses the Visitor design pattern to traverse a
 * file system hierarchy.  Please note that not all of the overridden
 * methods are used - they are kept here in case there is need in the
 * future for overriding them.
 *
 * @see <a href="http://docs.oracle.com/javase/tutorial/essential/io/walk.html">Walking the File Tree</a>
 * @see <a href="http://www.concretepage.com/java/jdk7/traverse-directory-structure-using-files-walkfiletree-java-nio2">Traverse a Directory Structure Using Files.walkFileTree in Java NIO 2</a>
 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html">JavaDoc BlockingQueue</a>
 * @see <a href="http://www.ibm.com/developerworks/library/j-jtp05236/">Dealing with InterruptedException</a>
 */
@SuppressWarnings("unchecked")
public class FileCrawler extends SimpleFileVisitor<Path>
{
    private DataBag mBag;
    private SolrDS mSolrDS;
    private final AppMgr mAppMgr;
    private CrawlFollow mCrawlFollow;
    private CrawlIgnore mCrawlIgnore;
    private boolean mIsValidationOnly;
    private boolean mIsCSVRowToDocument;
    private BlockingQueue mExtractQueue;
    private final CrawlQueue mCrawlQueue;
    private String mIdValuePrefix = StringUtils.EMPTY;

    /**
     * Creates and instance of the class and initializes it with
     * the application manager reference.  In addition, this
     * constructor will create instances of CrawlFollow and
     * CrawlIgnore objects for use during the visit process.
     *
     * @param anAppMgr Application manager instance.
     * @param aCrawlQueue Crawl queue instance.
     * @param aSolrDS Solr data source instance.
     *
     * @throws IOException Identifies an I/O error condition.
     * @throws NSException Identifies an initialization failure.
     */
    public FileCrawler(final AppMgr anAppMgr, CrawlQueue aCrawlQueue, SolrDS aSolrDS)
        throws IOException, NSException
    {
        super();

        mSolrDS = aSolrDS;
        mAppMgr = anAppMgr;
        mCrawlQueue = aCrawlQueue;

        mBag = (DataBag) mAppMgr.getProperty(Connector.PROPERTY_SCHEMA_NAME);

        String propertyName = Constants.CFG_PROPERTY_PREFIX + ".extract.id_value_prefix";
        String propertyValue = mAppMgr.getString(propertyName);
        if (StringUtils.isNotEmpty(propertyValue))
            mIdValuePrefix = propertyValue;

        propertyName = Constants.CFG_PROPERTY_PREFIX + ".extract.csv_row_to_document";
        mIsCSVRowToDocument = mAppMgr.getBoolean(propertyName, false);

        propertyName = Constants.CFG_PROPERTY_PREFIX + ".extract.validation_only";
        mIsValidationOnly = mAppMgr.getBoolean(propertyName, false);

        mCrawlFollow = new CrawlFollow(mAppMgr);
        mCrawlFollow.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
        mCrawlFollow.load();

        mCrawlIgnore = new CrawlIgnore(mAppMgr);
        mCrawlIgnore.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
        mCrawlIgnore.load();

        mExtractQueue = (BlockingQueue) mAppMgr.getProperty(Connector.QUEUE_EXTRACT_NAME);
    }

    /**
     * Invoked for a directory before entries in the directory are visited.
     * Unless overridden, this method returns {@link java.nio.file.FileVisitResult#CONTINUE}
     *
     * @param aDirectory Directory instance.
     * @param aFileAttributes File attribute instance.
     */
    @Override
    public FileVisitResult preVisitDirectory(Path aDirectory, BasicFileAttributes aFileAttributes)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "preVisitDirectory");

        if (mAppMgr.isAlive())
        {
            String pathName = aDirectory.toAbsolutePath().toString();
            if (mCrawlFollow.isMatchedNormalized(pathName))
            {
                appLogger.debug(String.format("Following Path: %s", pathName));
                return FileVisitResult.CONTINUE;
            }
            else
            {
                appLogger.debug(String.format("Skipping Path: %s", pathName));
                return FileVisitResult.SKIP_SUBTREE;
            }
        }
        else
            return FileVisitResult.TERMINATE;
    }

    private boolean documentExistsInIndex(String aDocId)
    {
        Logger appLogger = mAppMgr.getLogger(this, "documentExistsInIndex");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        boolean docExists = false;
        String propertyName = Constants.CFG_PROPERTY_PREFIX + ".publish.upload_enabled";
        if (mAppMgr.getBoolean(propertyName))
        {
            propertyName = Constants.CFG_PROPERTY_PREFIX + ".solr.request_uri";
            String solrURI = mAppMgr.getString(propertyName);
            if (StringUtils.isNotEmpty(solrURI))
            {
                propertyName = Constants.CFG_PROPERTY_PREFIX + ".solr.request_handler";
                String propertyValue = mAppMgr.getString(propertyName, Constants.SOLR_REQUEST_HANDLER_DEFAULT);
                String requestHandler = StringUtils.removeStart(propertyValue, "/");
                String solrURL = String.format("%s/%s?q=nsd_id%%3A%s&fl=nsd_doc_hash&wt=xml&echoParams=none",
                                               solrURI, requestHandler, aDocId);
                DSCriteria dsCriteria = new DSCriteria("Solr Document Exists");
                dsCriteria.add(Solr.FIELD_URL_NAME, Field.Operator.EQUAL, solrURL);
                try
                {
                    int docCount = mSolrDS.count(dsCriteria);
                    docExists = docCount > 0;
                    appLogger.debug(String.format("[%d] %s", docCount, solrURL));
                }
                catch (DSException e)
                {
                    appLogger.error(String.format("%s: %s", solrURL, e.getMessage()));
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return docExists;
    }

    private String createViewURL(String aDocId)
    {
        Logger appLogger = mAppMgr.getLogger(this, "createViewURL");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        int portNumber = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".restlet.port_number",
                                        Constants.APPLICATION_PORT_NUMBER_DEFAULT);
        String propertyName = Constants.CFG_PROPERTY_PREFIX + ".restlet.host_names";
        String hostNames = mAppMgr.getString(propertyName);
        if (StringUtils.isEmpty(hostNames))
            hostNames = Constants.HOST_NAME_DEFAULT;
        else
        {
            if (mAppMgr.isPropertyMultiValue(propertyName))
            {
                String[] hostNameList = mAppMgr.getStringArray(propertyName);
                hostNames = hostNameList[0];
            }
        }
        String docViewURL = String.format("http://%s:%d/fs/view?id=%s", hostNames, portNumber, aDocId);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return docViewURL;
    }

    private boolean isExpandableCSVFile(String aMimeType)
    {
        return ((mIsCSVRowToDocument) &&
                ((StringUtils.equals(aMimeType, Content.CONTENT_TYPE_TXT_CSV)) ||
                (StringUtils.equals(aMimeType, Content.CONTENT_TYPE_TXT_CSV))));
    }

    private void saveAddQueueDocument(Document aDocument, StopWatch aStopWatch)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "saveAddQueueDocument");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (! mIsValidationOnly)
        {
            DataBag dataBag = aDocument.getBag();
            DataField dataField = dataBag.getPrimaryKeyField();
            if (dataField == null)
                appLogger.error("Primary key field is missing from bag - cannot add to queue.");
            else
            {
                String docId = dataField.getValueAsString();

                String queueBagPathFileName = mCrawlQueue.docPathFileName(Connector.QUEUE_EXTRACT_NAME, docId);
                DocumentXML documentXML = new DocumentXML(aDocument);
                documentXML.save(queueBagPathFileName);

                aStopWatch.stop();
                String queueItem = Connector.queueItemIdPhaseTime(docId, Connector.PHASE_EXTRACT, aStopWatch.getTime());
                try
                {
                    // If queue is full, this thread may block.
                    mExtractQueue.put(queueItem);
                }
                catch (InterruptedException e)
                {
                    // Restore the interrupted status so parent can handle (if it wants to).
                    Thread.currentThread().interrupt();
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void processCSVFile(Path aPath, BasicFileAttributes aFileAttributes, String aViewURL)
        throws IOException
    {
        String docId;
        StopWatch stopWatch;
        Document fsDocument;
        Logger appLogger = mAppMgr.getLogger(this, "processCSVFile");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        File fsFile = aPath.toFile();
        String pathFileName = aPath.toAbsolutePath().toString();

        appLogger.debug(String.format("Processing CSV File: %s", pathFileName));

        CSVDocument csvDocument = new CSVDocument(mAppMgr, mBag);
        csvDocument.open(pathFileName);

        int row = 1;
        DataBag csvBag = csvDocument.extractNext();
        while (csvBag != null)
        {
            stopWatch = new StopWatch();
            stopWatch.start();

            docId = csvBag.generateUniqueHash(true);
            appLogger.debug(String.format(" Expanding Row [%d]: %s", row++, docId));

            csvBag.setValueByName("nsd_id", mIdValuePrefix + docId);
            csvBag.setValueByName("nsd_url", fsFile.toURI().toURL().toString());
            csvBag.setValueByName("nsd_url_view", aViewURL);
            csvBag.setValueByName("nsd_url_display", aViewURL);
            csvBag.setValueByName("nsd_file_name", fsFile.getName());
            csvBag.setValueByName("nsd_mime_type", Content.CONTENT_TYPE_TXT_CSV);
            FileTime creationTime = aFileAttributes.creationTime();
            Date cDate = new Date(creationTime.toMillis());
            csvBag.setValueByName("nsd_doc_created_ts", cDate);
            FileTime lastModifiedTime = aFileAttributes.lastModifiedTime();
            Date lmDate = new Date(lastModifiedTime.toMillis());
            csvBag.setValueByName("nsd_doc_modified_ts", lmDate);
            csvBag.setValueByName("nsd_crawl_type", mCrawlQueue.getCrawlType());
            fsDocument = new Document(Constants.FS_DOCUMENT_TYPE, csvBag);
            csvBag.setValueByName("nsd_doc_hash", fsDocument.generateUniqueHash(false));

            saveAddQueueDocument(fsDocument, stopWatch);

            csvBag = csvDocument.extractNext();
        }

        csvDocument.close();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private String generateDocumentId(Path aPath)
    {
        String pathFileName = aPath.toAbsolutePath().toString();
        return  mIdValuePrefix + Content.hashId(pathFileName);
    }

    private void processFile(Path aPath, BasicFileAttributes aFileAttributes)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "processFile");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        File fsFile = aPath.toFile();
        String docId = generateDocumentId(aPath);
        String pathFileName = aPath.toAbsolutePath().toString();
        appLogger.debug(String.format("Processing File (%s): %s", docId, pathFileName));

        boolean isFileFlat = true;
        Document fsDocument = new Document(Constants.FS_DOCUMENT_TYPE, mBag);
        DataBag fileBag = fsDocument.getBag();
        fileBag.resetValuesWithDefaults();
        fileBag.setValueByName("nsd_id", docId);
        String fileName = fsFile.getName();
        fileBag.setValueByName("nsd_url", fsFile.toURI().toURL().toString());
        String viewURL = createViewURL(docId);
        fileBag.setValueByName("nsd_url_view", viewURL);
        fileBag.setValueByName("nsd_url_display", viewURL);
        fileBag.setValueByName("nsd_name", fileName);
        fileBag.setValueByName("nsd_file_name", fileName);
        fileBag.setValueByName("nsd_file_size", aFileAttributes.size());
        FileTime creationTime = aFileAttributes.creationTime();
        Date cDate = new Date(creationTime.toMillis());
        fileBag.setValueByName("nsd_doc_created_ts", cDate);
        FileTime lastModifiedTime = aFileAttributes.lastModifiedTime();
        Date lmDate = new Date(lastModifiedTime.toMillis());
        fileBag.setValueByName("nsd_doc_modified_ts", lmDate);
        fileBag.setValueByName("nsd_crawl_type", mCrawlQueue.getCrawlType());

        DataField dataField = fileBag.getFirstFieldByFeatureName(Field.FEATURE_IS_CONTENT);
        if (dataField != null)
        {
            ContentExtractor contentExtractor = new ContentExtractor(mAppMgr);
            contentExtractor.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
            try
            {
                String mimeType = contentExtractor.detectType(fsFile);
                if (StringUtils.isNotEmpty(mimeType))
                    fileBag.setValueByName("nsd_mime_type", mimeType);
                if (isExpandableCSVFile(mimeType))
                {
                    isFileFlat = false;
                    processCSVFile(aPath, aFileAttributes, viewURL);
                }
                else
                    contentExtractor.process(pathFileName, dataField);
            }
            catch (NSException e)
            {
                String msgStr = String.format("%s: %s", pathFileName, e.getMessage());
                appLogger.error(msgStr);
            }
        }

        if (isFileFlat)
        {
            fileBag.setValueByName("nsd_doc_hash", fsDocument.generateUniqueHash(false));
            saveAddQueueDocument(fsDocument, stopWatch);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Invoked for a file in a directory.
     * Unless overridden, this method returns {@link java.nio.file.FileVisitResult#CONTINUE
     * CONTINUE}.
     *
     * @param aPath Path instance.
     * @param aFileAttributes File attribute instance.
     */
    @Override
    public FileVisitResult visitFile(Path aPath, BasicFileAttributes aFileAttributes)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "visitFile");

        String pathFileName = aPath.toAbsolutePath().toString();
        if (mCrawlIgnore.isMatchedNormalized(pathFileName))
            appLogger.debug(String.format("Ignoring File: %s", pathFileName));
        else
        {
            File fsFile = aPath.toFile();
            if ((fsFile.canRead()) && (mBag != null))
            {
                String crawlType = mCrawlQueue.getCrawlType();
                if (StringUtils.equals(crawlType, Connector.CRAWL_TYPE_INCREMENTAL))
                {
                    String docId = generateDocumentId(aPath);
                    boolean docExistsInIndex = documentExistsInIndex(docId);
                    if (docExistsInIndex)
                    {
                        Date incDate = mCrawlQueue.getCrawlLastModified();
                        FileTime lastModifiedTime = aFileAttributes.lastModifiedTime();
                        Date lmDate = new Date(lastModifiedTime.toMillis());
                        if (lmDate.after(incDate))
                            processFile(aPath, aFileAttributes);
                    }
                    else
                        processFile(aPath, aFileAttributes);
                }
                else
                    processFile(aPath, aFileAttributes);
            }
            else
                appLogger.warn(String.format("Access Failed: %s", pathFileName));
        }

        if (mAppMgr.isAlive())
            return FileVisitResult.CONTINUE;
        else
            return FileVisitResult.TERMINATE;
    }

    /**
     * Invoked for a file that could not be visited.
     * Unless overridden, this method re-throws the I/O exception that prevented
     * the file from being visited.
     *
     * @param aPathFile Path file instance.
     * @param anException Identifies an I/O error condition.
     */
    @Override
    public FileVisitResult visitFileFailed(Path aPathFile, IOException anException)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "visitFileFailed");

        String pathFileName = aPathFile.toAbsolutePath().toString();
        appLogger.warn(String.format("%s: %s", pathFileName, anException.getMessage()));

        return FileVisitResult.CONTINUE;
    }

    /**
     * Invoked for a directory after entries in the directory, and all of their
     * descendants, have been visited.
     * Unless overridden, this method returns {@link java.nio.file.FileVisitResult#CONTINUE}
     * if the directory iteration completes without an I/O exception;
     * otherwise this method re-throws the I/O exception that caused the iteration
     * of the directory to terminate prematurely.
     *
     * @param aDirectory Directory instance.
     * @param anException Identifies an I/O error condition.
     */
    @Override
    public FileVisitResult postVisitDirectory(Path aDirectory, IOException anException)
        throws IOException
    {
        return super.postVisitDirectory(aDirectory, anException);
    }
}
