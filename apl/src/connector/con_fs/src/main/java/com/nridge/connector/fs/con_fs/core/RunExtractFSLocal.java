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
import com.nridge.connector.common.con_com.crawl.CrawlQueue;
import com.nridge.core.app.mail.MailManager;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.std.NSException;
import com.nridge.ds.solr.SolrDS;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The RunExtractFSLocal is responsible traversing a hierarchy
 * of folder and files with the goal of extracting their
 * content into the crawl queue.  This class is intended to
 * support local file system crawls.
 *
 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html">JavaDoc BlockingQueue</a>
 * @see <a href="http://tutorials.jenkov.com/java-util-concurrent/blockingqueue.html">Java BlockingQueue Tutorial</a>
 * @see <a href="http://docs.oracle.com/javase/tutorial/essential/io/walk.html">Walking the File Tree</a>
 * @see <a href="http://www.concretepage.com/java/jdk7/traverse-directory-structure-using-files-walkfiletree-java-nio2">Traverse a Directory Structure Using Files.walkFileTree in Java NIO 2</a>
 */
public class RunExtractFSLocal implements Runnable
{
    private final AppMgr mAppMgr;
    private String mPathFileName;
    private final CrawlQueue mCrawlQueue;

    /**
     * Constructor initializes the internal state of the class with
     * the parameters provided.
     *
     * @param anAppMgr Application manager instance.
     * @param aCrawlQueue Crawl queue instance.
     * @param aPathFileName Path/File name the extraction process should start at.
     */
    public RunExtractFSLocal(final AppMgr anAppMgr, CrawlQueue aCrawlQueue,
                             String aPathFileName)
    {
        mAppMgr = anAppMgr;
        mCrawlQueue = aCrawlQueue;
        mPathFileName = aPathFileName;
    }

    /**
     * Initiates the local file system crawl and the corresponding
     * document content extraction.
     *
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
        Logger appLogger = mAppMgr.getLogger(this, "run");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SolrDS solrDS = new SolrDS(mAppMgr);
        solrDS.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".solr");

        try
        {
            FileCrawler fileCrawler = new FileCrawler(mAppMgr, mCrawlQueue, solrDS);
            Path crawlPathFile = Paths.get(mPathFileName);
            Files.walkFileTree(crawlPathFile, fileCrawler);
        }
        catch (IOException | NSException e)
        {
            String msgStr = String.format("%s: %s", mPathFileName, e.getMessage());
            appLogger.error(msgStr, e);
            MailManager mailManager = (MailManager) mAppMgr.getProperty(Connector.PROPERTY_MAIL_NAME);
            mailManager.addMessage(Connector.PHASE_EXTRACT, Connector.STATUS_MAIL_ERROR,
                                    msgStr, Constants.MAIL_DETAIL_MESSAGE);
        }
        finally
        {
            solrDS.shutdown();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
