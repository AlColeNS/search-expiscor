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

package com.nridge.connector.ws.con_ws.core;

import com.nridge.connector.common.con_com.Connector;
import com.nridge.connector.common.con_com.crawl.CrawlQueue;
import com.nridge.connector.common.con_com.publish.Publishers;
import com.nridge.core.app.mail.MailManager;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.io.xml.DocumentXML;
import com.nridge.core.base.std.NSException;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The RunPublishWS is responsible for taking items off the
 * transform queue and publishing their related documents
 * to the search index service.
 *
 * @see <a href="https://github.com/kamranzafar/JCL">Jar Class Loader</a>
 * @see <a href="http://tutorials.jenkov.com/java-reflection/dynamic-class-loading-reloading.html">Dynamic Class Loading and Reloading</a>
 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html">JavaDoc BlockingQueue</a>
 * @see <a href="http://tutorials.jenkov.com/java-util-concurrent/blockingqueue.html">Java BlockingQueue Tutorial</a>
 * @see <a href="http://www.ibm.com/developerworks/library/j-jtp05236/">Dealing with InterruptedException</a>
 */
@SuppressWarnings("unchecked")
public class RunPublishWS implements Runnable
{
    private final AppMgr mAppMgr;
    private final CrawlQueue mCrawlQueue;

    public RunPublishWS(final AppMgr anAppMgr, CrawlQueue aCrawlQueue)
    {
        mAppMgr = anAppMgr;
        mCrawlQueue = aCrawlQueue;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * 
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run()
    {
        Document conDoc;
        DocumentXML documentXML;
        String docId, queueItem, srcPathFileName;
        Logger appLogger = mAppMgr.getLogger(this, "run");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        boolean isPublisherInitialized = true;
        DataBag schemaBag = (DataBag) mAppMgr.getProperty(Connector.PROPERTY_SCHEMA_NAME);
        Publishers indexPublishers = new Publishers(mAppMgr, mCrawlQueue, Constants.CFG_PROPERTY_PREFIX);
        try
        {
            indexPublishers.initialize(schemaBag);
        }
        catch (NSException e)
        {
            isPublisherInitialized = false;
            appLogger.error("Publisher initialization: " + e.getMessage());
        }

        if (isPublisherInitialized)
        {
            BlockingQueue transformQueue = (BlockingQueue) mAppMgr.getProperty(Connector.QUEUE_TRANSFORM_NAME);
            BlockingQueue publishQueue = (BlockingQueue) mAppMgr.getProperty(Connector.QUEUE_PUBLISH_NAME);

            long queueWaitTimeout = mAppMgr.getLong(Constants.CFG_PROPERTY_PREFIX + ".queue.wait_timeout",
                                                    Constants.QUEUE_POLL_TIMEOUT_DEFAULT);
            do
            {
                try
                {
                    queueItem = (String) transformQueue.poll(queueWaitTimeout, TimeUnit.SECONDS);
                    if (mCrawlQueue.isQueueItemDocument(queueItem))
                    {
                        StopWatch stopWatch = new StopWatch();
                        stopWatch.start();

                        docId = Connector.docIdFromQueueItem(queueItem);

                        appLogger.debug(String.format("Transform Queue Item: %s", docId));
                        srcPathFileName = mCrawlQueue.docPathFileName(Connector.QUEUE_TRANSFORM_NAME, docId);
                        try
                        {
                            documentXML = new DocumentXML();
                            documentXML.load(srcPathFileName);
                            conDoc = documentXML.getDocument();

                            indexPublishers.send(conDoc);

                            File srcFile = new File(srcPathFileName);
                            if (! srcFile.delete())
                                appLogger.warn(String.format("%s: Unable to delete.", srcPathFileName));

                            stopWatch.stop();
                            queueItem = Connector.queueItemIdPhaseTime(queueItem, Connector.PHASE_PUBLISH, stopWatch.getTime());
                            try
                            {
                                // If queue is full, this thread may block.
                                publishQueue.put(queueItem);
                            }
                            catch (InterruptedException e)
                            {
                                // Restore the interrupted status so parent can handle (if it wants to).
                                Thread.currentThread().interrupt();
                            }
                        }
                        catch (Exception e)
                        {
                            String msgStr = String.format("%s: %s", docId, e.getMessage());
                            appLogger.error(msgStr, e);
                            MailManager mailManager = (MailManager) mAppMgr.getProperty(Connector.PROPERTY_MAIL_NAME);
                            mailManager.addMessage(Connector.PHASE_PUBLISH, Connector.STATUS_MAIL_ERROR,
                                                    msgStr, Constants.MAIL_DETAIL_MESSAGE);
                        }
                    }
                }
                catch (InterruptedException e)
                {
                    queueItem = StringUtils.EMPTY;
                }
            }
            while (! mCrawlQueue.isPhaseComplete(Connector.PHASE_TRANSFORM, queueItem));

// Forward the marker queue item to the next queue.

            if (mCrawlQueue.isQueueItemMarker(queueItem))
            {
                try
                {
                    // If queue is full, this thread may block.
                    publishQueue.put(queueItem);
                }
                catch (InterruptedException e)
                {
                    // Restore the interrupted status so parent can handle (if it wants to).
                    Thread.currentThread().interrupt();
                }
            }

// Now we can shutdown our search indexer publisher.

            try
            {
                indexPublishers.shutdown();
            }
            catch (NSException e)
            {
                appLogger.error("Publisher shutdown: " + e.getMessage());
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
