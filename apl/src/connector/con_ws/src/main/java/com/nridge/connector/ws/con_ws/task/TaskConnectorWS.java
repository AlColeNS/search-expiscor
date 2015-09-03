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

package com.nridge.connector.ws.con_ws.task;

import com.nridge.connector.common.con_com.Connector;
import com.nridge.connector.common.con_com.crawl.CrawlFollow;
import com.nridge.connector.common.con_com.crawl.CrawlIgnore;
import com.nridge.connector.common.con_com.crawl.CrawlQueue;
import com.nridge.connector.common.con_com.crawl.CrawlStart;
import com.nridge.connector.common.con_com.publish.Publishers;
import com.nridge.connector.common.con_com.transform.Pipeline;
import com.nridge.connector.ws.con_ws.core.*;
import com.nridge.core.app.mail.MailManager;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.app.mgr.ServiceTimer;
import com.nridge.core.app.mgr.Task;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.io.xml.DataBagXML;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.Platform;
import com.nridge.core.base.std.Sleep;
import com.nridge.ds.content.ds_content.Content;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The ConnectorWSTask is responsible for driving the web site
 * crawl process for the connector.
 *
 * @see <a href="http://www.mkyong.com/java/find-out-your-java-heap-memory-size/">JVM Heap Sizes</a>
 * @see <a href="http://tutorials.jenkov.com/java-util-concurrent/blockingqueue.html">BlockingQueue Tutorial</a>
 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html">JavaDoc BlockingQueue</a>
 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ThreadPoolExecutor.html">JavaDoc ThreadPoolExecutor</a>
 * @see <a href="http://www.journaldev.com/1069/java-thread-pool-example-using-executors-and-threadpoolexecutor">Java Thread Pool Example</a>
 *
 */
@SuppressWarnings({"FieldCanBeLocal", "unchecked"})
public class TaskConnectorWS implements Task
{
    private final String mRunName = "cws";
    private final String mTestName = "cws";

    private AppMgr mAppMgr;
    private AtomicBoolean mIsAlive;
    private int mSleepTimeInMinutes;
    private String[] mPhases = {"All"};
    private ServiceTimer mServiceTimer;
    private ExecutorService mMetricExecutor;
    private ExecutorService mPublishExecutor;
    private CrawlController mCrawlController;
    private ExecutorService mTransformExecutor;

	/**
     * Returns the name of the run task.  This name will be used
     * by the application manager to identify which task in the
     * list to run (based on command line arguments).
     *
     * @return Name of the run task.
     */
    @Override
    public String getRunName()
    {
        return mRunName;
    }

	/**
     * Returns the name of the test task.  This name will be used
     * by the application manager to identify which task in the
     * list to test (based on command line arguments).
     *
     * @return Name of the test task.
     */
    @Override
    public String getTestName()
    {
        return mTestName;
    }

    /**
     * Returns <i>true</i> if this task was properly initialized
     * and is currently executing.
     *
     * @return <i>true</i> or <i>false</i>
     */
    @Override
    public boolean isAlive()
    {
        if (mIsAlive == null)
            mIsAlive = new AtomicBoolean(false);

        return mIsAlive.get();
    }

    /**
     * Creates a <i>DataBag</i> containing a list of fields
     * representing a schema for this object.
     *
     * @return DataBag instance.
     */
    public DataBag schemaBag()
    {
        DataBag dataBag = new DataBag("NSD File Share Connector Schema");

        DataTextField dataTextField = new DataTextField("nsd_id", "NSD Id");
        dataTextField.enableFeature(Field.FEATURE_IS_REQUIRED);
        dataTextField.enableFeature(Field.FEATURE_IS_PRIMARY_KEY);
        dataTextField.enableFeature(Field.FEATURE_IS_UNIQUE);
        dataBag.add(dataTextField);
        dataBag.add(new DataTextField("nsd_name", "NSD Name"));
        dataBag.add(new DataTextField("nsd_url", "NSD URL"));
        dataBag.add(new DataTextField("nsd_url_view", "NSD URL View"));
        dataBag.add(new DataTextField("nsd_url_display", "NSD URL Display"));
        dataBag.add(new DataTextField("nsd_title", "NSD Title"));
        dataBag.add(new DataTextField("nsd_description", "NSD Description"));
        dataTextField = new DataTextField("nsd_content", "NSD Content");
        dataTextField.setMultiValueFlag(true);
        dataTextField.enableFeature(Field.FEATURE_IS_CONTENT);
        dataBag.add(dataTextField);
        dataBag.add(new DataTextField("nsd_file_name", "NSD File Name"));
        dataBag.add(new DataLongField("nsd_file_size", "NSD File Size"));
        dataBag.add(new DataDateTimeField("nsd_doc_created_ts", "NSD Document Created On"));
        dataBag.add(new DataDateTimeField("nsd_doc_modified_ts", "NSD Document Modified On"));
        dataTextField = new DataTextField("nsd_mime_type", "NSD MIME Type");
        dataTextField.setDefaultValue(Content.CONTENT_TYPE_UNKNOWN);
        dataBag.add(dataTextField);
        dataTextField = new DataTextField("nsd_doc_type", "NSD Document Type");
        dataTextField.setDefaultValue(Content.CONTENT_TYPE_UNKNOWN);
        dataBag.add(dataTextField);
        dataTextField = new DataTextField("nsd_acl_view", "NSD ACL View");
        dataTextField.setMultiValueFlag(true);
        dataTextField.setDefaultValue("public");
        dataBag.add(dataTextField);
        dataTextField = new DataTextField("nsd_acl_preview", "NSD ACL Preview");
        dataTextField.setMultiValueFlag(true);
        dataTextField.setDefaultValue("public");
        dataBag.add(dataTextField);
        DataDateTimeField dateTimeField = new DataDateTimeField("nsd_crawl_ts", "NSD Crawled On");
        dateTimeField.setDefaultValue(Field.VALUE_DATETIME_TODAY);
        dataBag.add(dateTimeField);
        dataTextField = new DataTextField("nsd_crawl_type", "NSD Crawl Type");
        dataTextField.setDefaultValue("Full");
        dataBag.add(dataTextField);
        dataTextField = new DataTextField("nsd_con_name", "NSD Connector Name");
        dataTextField.setDefaultValue("NSD WS");
        dataBag.add(dataTextField);
        dataTextField = new DataTextField("nsd_con_type", "NSD Connector Type");
        dataTextField.setDefaultValue("WS");
        dataBag.add(dataTextField);
        DataBooleanField dataBooleanField = new DataBooleanField("nsd_is_latest", "NSD Is Latest");
        dataBooleanField.setDefaultValue(true);
        dataBag.add(dataBooleanField);
        DataIntegerField dataIntegerField = new DataIntegerField("nsd_version", "NSD Version Number");
        dataIntegerField.setDefaultValue(1);
        dataBag.add(dataIntegerField);

        return dataBag;
    }

	/**
     * If this task is scheduled to be executed (e.g. its run/test
     * name matches the command line arguments), then this method
     * is guaranteed to be executed prior to the thread being
     * started.
     *
     * @param anAppMgr Application manager instance.
     *
     * @throws com.nridge.core.base.std.NSException Application specific exception.
     */
    @Override
    public void init(AppMgr anAppMgr)
        throws NSException
    {
        mAppMgr = anAppMgr;
        Logger appLogger = mAppMgr.getLogger(this, "init");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mIsAlive = new AtomicBoolean(false);

// Write our configuration properties for troubleshooting purposes.

        mAppMgr.writeCfgProperties(appLogger);

// Assign our between crawl sleep time.

        mSleepTimeInMinutes = 15;
        String sleepTimeString = mAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".run_sleep_between");
        if (StringUtils.endsWithIgnoreCase(sleepTimeString, "m"))
        {
            String minuteString = StringUtils.stripEnd(sleepTimeString, "m");
            if ((StringUtils.isNotEmpty(minuteString)) && (StringUtils.isNumeric(minuteString)))
                mSleepTimeInMinutes = Integer.parseInt(minuteString);
        }
        else if ((StringUtils.isNotEmpty(sleepTimeString)) && (StringUtils.isNumeric(sleepTimeString)))
            mSleepTimeInMinutes = Integer.parseInt(sleepTimeString);

// The extract queue holds documents that have been extracted from the content source.

        int extractQueueSize = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".extract.queue_length",
                                              Connector.QUEUE_LENGTH_DEFAULT);
        BlockingQueue extractQueue = new ArrayBlockingQueue(extractQueueSize);
        mAppMgr.addProperty(Connector.QUEUE_EXTRACT_NAME, extractQueue);

// The transform queue holds documents that have been transformed after extraction.

        int transformQueueSize = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".transform.queue_length",
                                                Connector.QUEUE_LENGTH_DEFAULT);
        BlockingQueue transformQueue = new ArrayBlockingQueue(transformQueueSize);
        mAppMgr.addProperty(Connector.QUEUE_TRANSFORM_NAME, transformQueue);

// The publish queue holds documents that have been published to the search index.

        int publishQueueSize = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".publish.queue_length",
                                              Connector.QUEUE_LENGTH_DEFAULT);
        BlockingQueue publishQueue = new ArrayBlockingQueue(publishQueueSize);
        mAppMgr.addProperty(Connector.QUEUE_PUBLISH_NAME, publishQueue);

// Load our schema definition from the data source folder.

        DataBag schemaBag;
        String schemaPathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_DS_PATH),
                                                  File.separatorChar, Constants.SCHEMA_FILE_NAME);
        DataBagXML dataBagXML = new DataBagXML();
        try
        {
            dataBagXML.load(schemaPathFileName);
            schemaBag = dataBagXML.getBag();
        }
        catch (Exception e)
        {
            String msgStr = String.format("%s: %s", schemaPathFileName, e.getMessage());
            appLogger.error(msgStr);
            appLogger.warn("Using internal document schema as alternative - data source schema ignored.");
            schemaBag = schemaBag();
        }

        mAppMgr.addProperty(Connector.PROPERTY_SCHEMA_NAME, schemaBag);

// Create our mail manager instance.

        MailManager mailManager = new MailManager(mAppMgr, Constants.CFG_PROPERTY_PREFIX + ".mail");
        mAppMgr.addProperty(Connector.PROPERTY_MAIL_NAME, mailManager);

// Create/Load service time tracking file.

        mServiceTimer = new ServiceTimer(mAppMgr);
        mServiceTimer.setPropertyPrefix(Constants.CFG_PROPERTY_PREFIX);
        String stPathFileName = mServiceTimer.createServicePathFileName();
        File stFile = new File(stPathFileName);
        if (stFile.exists())
            mServiceTimer.load();

// Is there an explicit list of phases to execute?

        String propertyName = Constants.CFG_PROPERTY_PREFIX + ".phase_list";
        String phaseProperty = mAppMgr.getString(propertyName);
        if (StringUtils.isNotEmpty(phaseProperty))
        {
            if (mAppMgr.isPropertyMultiValue(propertyName))
                mPhases = mAppMgr.getStringArray(propertyName);
            else
            {
                mPhases = new String[1];
                mPhases[0] = phaseProperty;
            }
        }

// Load and assign our crawl follow and ignore instances.

        CrawlFollow crawlFollow = new CrawlFollow(mAppMgr);
        crawlFollow.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
        try
        {
            crawlFollow.load();
        }
        catch (NSException | IOException e)
        {
            String msgStr = String.format("Crawl Follow: %s", e.getMessage());
            appLogger.error(msgStr);
        }
        mAppMgr.addProperty(Constants.PROPERTY_CRAWL_FOLLOW, crawlFollow);

        CrawlIgnore crawlIgnore = new CrawlIgnore(mAppMgr);
        crawlIgnore.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
        try
        {
            crawlIgnore.load();
        }
        catch (NSException | IOException e)
        {
            String msgStr = String.format("Crawl Ignore: %s", e.getMessage());
            appLogger.error(msgStr);
        }
        mAppMgr.addProperty(Constants.PROPERTY_CRAWL_IGNORE, crawlIgnore);

// Clear out crawl queue from previous service sessions.

        CrawlQueue crawlQueue = new CrawlQueue(mAppMgr);
        crawlQueue.reset();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        mIsAlive.set(true);
    }

	/**
     * Each task supports a method dedicated to testing or exercising
     * a subset of application features without having to run the
     * mainline thread of task logic.
     *
     * @throws com.nridge.core.base.std.NSException Application specific exception.
     */
    @Override
    public void test()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "test");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (! isAlive())
        {
            appLogger.error("Initialization failed - must abort test method.");
            return;
        }

        appLogger.info("The test method was invoked.");
        Sleep.forSeconds(1);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private boolean isPipelineReady(CrawlQueue aCrawlQueue)
    {
        boolean isCrawlQueueEmpty;
        Logger appLogger = mAppMgr.getLogger(this, "isPipelineReady");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aCrawlQueue.isActive())
            isCrawlQueueEmpty = false;
        else
            isCrawlQueueEmpty = true;

        String dataCrawlerPathFileName = String.format("%s%cdata%ccrawler",
                                                        mAppMgr.getString(mAppMgr.APP_PROPERTY_INS_PATH),
                                                        File.separatorChar, File.separatorChar);
        File dataFile = new File(dataCrawlerPathFileName);
        if (! dataFile.exists())
            dataFile.mkdirs();
        boolean isCrawlStartValid = dataFile.exists();
        CrawlStart crawlStart = new CrawlStart(mAppMgr);
        crawlStart.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
        try
        {
            crawlStart.load();
            crawlStart.validate();
        }
        catch (IOException | NSException e)
        {
            appLogger.error(e.getMessage(), e);
            isCrawlStartValid = false;
        }

        boolean isTransformValid = true;
        Pipeline transformPipeline = new Pipeline(mAppMgr, Constants.CFG_PROPERTY_PREFIX);
        try
        {
            transformPipeline.validate();
        }
        catch (NSException e)
        {
            appLogger.error(e.getMessage(), e);
            isTransformValid = false;
        }

        boolean isPublishValid = true;
        Publishers indexPublishers = new Publishers(mAppMgr, aCrawlQueue, Constants.CFG_PROPERTY_PREFIX);
        try
        {
            indexPublishers.validate();
        }
        catch (NSException e)
        {
            appLogger.error(e.getMessage(), e);
            isPublishValid = false;
        }

        String msgStr = String.format("isCrawlQueueEmpty = %s, isCrawlStartValid = %s, isTransformValid = %s, isPublishValid = %s",
                                      isCrawlQueueEmpty, isCrawlStartValid, isTransformValid, isPublishValid);
        appLogger.debug(msgStr);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return ((isCrawlQueueEmpty) && (isCrawlStartValid) && (isTransformValid) &&
                (isPublishValid));
    }

    private void createSnapshot(CrawlQueue aCrawlQueue)
    {
        Logger appLogger = mAppMgr.getLogger(this, "createSnapshot");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// Update Solr with new crawl and snapshot information.

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void resolveSnapshot(CrawlQueue aCrawlQueue)
    {
        Logger appLogger = mAppMgr.getLogger(this, "resolveSnapshot");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// Update Solr with new crawl and snapshot information.

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void executeExtract(CrawlQueue aCrawlQueue)
        throws NSException, IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "executeExtract");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        CrawlStart crawlStart = new CrawlStart(mAppMgr);
        crawlStart.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
        crawlStart.load();

        int extractThreadCount = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".extract.thread_count", 1);
        int politenessDelay = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".extract.politeness_delay", 100);
        int maxPagesToFetch = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".extract.crawl_max_pages", -1);
        boolean isRedirectsToBeFollowed = mAppMgr.getBoolean(Constants.CFG_PROPERTY_PREFIX + ".extract.follow_redirects", false);
        String userAgentString = mAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.crawl_agent_string");
        String dataCrawlerPathFileName = String.format("%s%cdata%ccrawler",
                                                        mAppMgr.getString(mAppMgr.APP_PROPERTY_INS_PATH),
                                                        File.separatorChar, File.separatorChar);
        CrawlConfig crawlConfig = new CrawlConfig();
        crawlConfig.setCrawlStorageFolder(dataCrawlerPathFileName);
        crawlConfig.setIncludeBinaryContentInCrawling(true);
        crawlConfig.setIncludeHttpsPages(true);
        crawlConfig.setPolitenessDelay(politenessDelay);
        crawlConfig.setMaxPagesToFetch(maxPagesToFetch);
        crawlConfig.setFollowRedirects(isRedirectsToBeFollowed);
        if (StringUtils.isNotEmpty(userAgentString))
            crawlConfig.setUserAgentString(userAgentString);

        PageFetcher pageFetcher = new PageFetcher(crawlConfig);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);

        String proxyHostName = mAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.proxy_host_name");
        if (StringUtils.isNotEmpty(proxyHostName))
        {
            crawlConfig.setProxyHost(proxyHostName);
            String proxyPortNumber = mAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.proxy_port_number");
            if (StringUtils.isNotEmpty(proxyPortNumber))
                crawlConfig.setProxyPort(Integer.parseInt(proxyPortNumber));
            String proxyAccountName = mAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.proxy_account");
            if (StringUtils.isNotEmpty(proxyAccountName))
                crawlConfig.setProxyUsername(proxyAccountName);
            String proxyAccountPassword = mAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.proxy_password");
            if (StringUtils.isNotEmpty(proxyAccountPassword))
                crawlConfig.setProxyPassword(proxyAccountPassword);
        }
        appLogger.debug(crawlConfig.toString());

        mAppMgr.addProperty(Connector.PROPERTY_CRAWL_QUEUE, aCrawlQueue);

        mCrawlController = null;
        try
        {
            mCrawlController = new CrawlController(crawlConfig, pageFetcher, robotstxtServer);
            mCrawlController.setCustomData(mAppMgr);
            ArrayList<String> crawlStartList = crawlStart.getList();
            for (String crawlStartName : crawlStartList)
                mCrawlController.addSeed(crawlStartName);
            mCrawlController.start(SiteCrawler.class, extractThreadCount);
            mCrawlController.waitUntilFinish();

// Place a queue item marker into the queue to mark the end of the document extraction process.

            aCrawlQueue.putMarkerIntoQueue(Connector.QUEUE_EXTRACT_NAME, Connector.QUEUE_ITEM_CRAWL_FINISH);
        }
        catch (Exception e)
        {
            String msgStr = String.format("Crawl Controller Error: %s", e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }
        finally
        {
            if (mCrawlController != null)
            {
                mCrawlController.shutdown();
                mCrawlController = null;
            }
        }

        mAppMgr.removeProperty(Connector.PROPERTY_CRAWL_QUEUE);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void executeTransform(CrawlQueue aCrawlQueue)
        throws NSException, IOException
    {
        Runnable queueThread;
        Logger appLogger = mAppMgr.getLogger(this, "executeTransform");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

/* Executing a thread with the Executor does not guarantee when it will be started.
It only ensures that the threads are in the queue for processing.  The Executor
will control how many threads run concurrently based on how it was configured. */

        int transformThreadCount = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".transform.thread_count", 1);
        mTransformExecutor = Executors.newFixedThreadPool(transformThreadCount);

        for (int thread = 0; thread < transformThreadCount; thread++)
        {
            queueThread = new RunTransformWS(mAppMgr, aCrawlQueue);
            mTransformExecutor.execute(queueThread);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void executePublish(CrawlQueue aCrawlQueue)
        throws NSException, IOException
    {
        Runnable queueThread;
        Logger appLogger = mAppMgr.getLogger(this, "executePublish");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

/* Executing a thread with the Executor does not guarantee when it will be started.
It only ensures that the threads are in the queue for processing.  The Executor
will control how many threads run concurrently based on how it was configured. */

        int publishThreadCount = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".publish.thread_count", 1);
        mPublishExecutor = Executors.newFixedThreadPool(publishThreadCount);

        for (int thread = 0; thread < publishThreadCount; thread++)
        {
            queueThread = new RunPublishWS(mAppMgr, aCrawlQueue);
            mPublishExecutor.execute(queueThread);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void executeMetricReport(CrawlQueue aCrawlQueue)
        throws NSException, IOException
    {
        Runnable queueThread;
        Logger appLogger = mAppMgr.getLogger(this, "executeMetricReport");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

/* Executing a thread with the Executor does not guarantee when it will be started.
It only ensures that the threads are in the queue for processing.  The Executor
will control how many threads run concurrently based on how it was configured. */

        mMetricExecutor = Executors.newFixedThreadPool(1);
        queueThread = new RunMetricReport(mAppMgr, aCrawlQueue);
        mMetricExecutor.execute(queueThread);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void waitForCompletion(CrawlQueue aCrawlQueue)
    {
        Logger appLogger = mAppMgr.getLogger(this, "waitForCompletion");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mTransformExecutor != null)
        {
            mTransformExecutor.shutdown();
            while (! mTransformExecutor.isTerminated())
                Sleep.forSeconds(1);
        }

        if (mPublishExecutor != null)
        {
            mPublishExecutor.shutdown();
            while (! mPublishExecutor.isTerminated())
                Sleep.forSeconds(1);
        }

        if (mMetricExecutor != null)
        {
            mMetricExecutor.shutdown();
            while (! mMetricExecutor.isTerminated())
                Sleep.forSeconds(1);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void emptyQueue(String aQueueName)
    {
        Logger appLogger = mAppMgr.getLogger(this, "emptyQueue");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        BlockingQueue blockingQueue = (BlockingQueue) mAppMgr.getProperty(aQueueName);
        if (blockingQueue != null)
        {
            ArrayList<String> drainToList = new ArrayList<>();
            blockingQueue.drainTo(drainToList);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private boolean isExecuteSinglePass()
    {
        return ((mPhases.length != 1) || (! mPhases[0].equals(Connector.PHASE_ALL)));
    }

    private boolean executePhase(String aName)
    {
        for (String phaseName : mPhases)
        {
            if ((StringUtils.equalsIgnoreCase(aName, phaseName)) ||
                (StringUtils.equalsIgnoreCase(phaseName, Connector.PHASE_ALL)))
                return true;
        }

        return false;
    }

    private void executeReset()
    {
        Logger appLogger = mAppMgr.getLogger(this, "executeReset");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mCrawlController != null)
        {
            emptyQueue(Connector.QUEUE_EXTRACT_NAME);
            mCrawlController.shutdown();
            mCrawlController = null;
        }
        if (mTransformExecutor != null)
        {
            emptyQueue(Connector.QUEUE_TRANSFORM_NAME);
            mTransformExecutor.shutdownNow();
            mTransformExecutor = null;
        }
        if (mPublishExecutor != null)
        {
            emptyQueue(Connector.QUEUE_PUBLISH_NAME);
            mPublishExecutor.shutdownNow();
            mPublishExecutor = null;
        }
        if (mMetricExecutor != null)
        {
            mMetricExecutor.shutdownNow();
            mMetricExecutor = null;
        }

        MailManager mailManager = (MailManager) mAppMgr.getProperty(Connector.PROPERTY_MAIL_NAME);
        if (mailManager != null)
            mailManager.reset();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void executeCrawl(CrawlQueue aCrawlQueue)
        throws NSException, IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "executeCrawl");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String crawlType = aCrawlQueue.getCrawlType();
        appLogger.debug(Platform.jvmLogMessage(String.format("Crawl Start [%s]", crawlType)));

        if (StringUtils.equals(crawlType, Connector.CRAWL_TYPE_FULL))
            appLogger.info(String.format("%s Crawl Started: Id = %s", crawlType, aCrawlQueue.getCrawlId()));
        else
            appLogger.info(String.format("%s Crawl Started Id = %s, Last Modified Date = %s", crawlType,
                                         aCrawlQueue.getCrawlId(), aCrawlQueue.getCrawlLastModified()));

        if (executePhase(Connector.PHASE_SNAPSHOT))
            createSnapshot(aCrawlQueue);
        if (executePhase(Connector.PHASE_TRANSFORM))
            executeTransform(aCrawlQueue);
        if (executePhase(Connector.PHASE_PUBLISH))
        {
            executePublish(aCrawlQueue);
            executeMetricReport(aCrawlQueue);
        }
        if (executePhase(Connector.PHASE_EXTRACT))
            executeExtract(aCrawlQueue);
        waitForCompletion(aCrawlQueue);
        if (executePhase(Connector.PHASE_SNAPSHOT))
            resolveSnapshot(aCrawlQueue);

        appLogger.info(String.format("%s Crawl Completed: Id = %s", crawlType, aCrawlQueue.getCrawlId()));

// Good time for a garbage collection event.

        System.gc();
        appLogger.debug(Platform.jvmLogMessage(String.format("Crawl Complete [%s]", crawlType)));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

	/**
     * The {@link Runnable}.run() will be executed after the task
     * has been successfully initialized.  This method is where
     * the application specific logic should be concentrated for
     * the task.
     */
    @Override
    public void run()
    {
        CrawlQueue crawlQueue;
        Date tsNow, lastIncrementalServiceTS;
        Logger appLogger = mAppMgr.getLogger(this, "run");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (! isAlive())
        {
            appLogger.error("Initialization failed - must abort run method.");
            return;
        }

        if (! isExecuteSinglePass())
        {
            String runSleepPropertyName = Constants.CFG_PROPERTY_PREFIX + ".run_sleep_startup_delay";
            int startupDelayInSeconds = Constants.getCfgSleepValue(mAppMgr, runSleepPropertyName,
                                                                   Connector.RUN_STARTUP_SLEEP_DELAY);
            if (startupDelayInSeconds > 0L)
            {
                String msgStr = String.format("Waiting %d seconds before executing first crawl.",
                                              startupDelayInSeconds);
                appLogger.info(msgStr);
                Sleep.forSeconds(startupDelayInSeconds);
            }
        }

        MailManager mailManager = (MailManager) mAppMgr.getProperty(Connector.PROPERTY_MAIL_NAME);
        boolean isQueueNeeded = isExecuteSinglePass() || mAppMgr.getBoolean(Constants.CFG_PROPERTY_PREFIX + ".publish.save_files", false);

        while (isAlive())
        {
            tsNow = new Date();
            crawlQueue = new CrawlQueue(mAppMgr);

            try
            {
                if (isPipelineReady(crawlQueue))
                {
                    if (mServiceTimer.isTimeForFullService())
                    {
                        crawlQueue.start(Connector.CRAWL_TYPE_FULL);
                        executeCrawl(crawlQueue);
                        crawlQueue.finish(isQueueNeeded);
                        if (! isExecuteSinglePass())
                        {
                            mServiceTimer.setLastFullServiceTS(tsNow);
                            mServiceTimer.save();
                        }
                    }
                    else if (mServiceTimer.isTimeForIncrementalService())
                    {
                        lastIncrementalServiceTS = mServiceTimer.getLastIncrementalServiceTS();
                        crawlQueue.start(Connector.CRAWL_TYPE_INCREMENTAL, lastIncrementalServiceTS);
                        executeCrawl(crawlQueue);
                        crawlQueue.finish(isQueueNeeded);
                        if (! isExecuteSinglePass())
                        {
                            mServiceTimer.setLastIncrementalServiceTS(tsNow);
                            mServiceTimer.save();
                        }
                    }
                }
                else
                {
                    String msgStr = "Unable to crawl content because pipeline is not ready.";
                    appLogger.error(msgStr);
                    if (mailManager != null)
                    {
                        mailManager.addMessage(Connector.PHASE_ALL, Connector.STATUS_MAIL_ERROR,
                                                msgStr, Constants.MAIL_DETAIL_MESSAGE);
                    }
                }
            }
            catch (Exception e)
            {
                String msgStr = String.format("Crawl %d: %s", crawlQueue.getCrawlId(), e.getMessage());
                appLogger.error(msgStr);
                if (mailManager != null)
                {
                    mailManager.addMessage(Connector.PHASE_ALL, Connector.STATUS_MAIL_ERROR,
                                            msgStr, Constants.MAIL_DETAIL_MESSAGE);
                }
            }

            if ((mailManager != null) && (mailManager.messageCount() > 0))
            {
                try
                {
                    mailManager.sendMessageTable(Constants.MAIL_SUBJECT_INFO);
                }
                catch (Exception e)
                {
                    appLogger.error(e.getMessage(), e);
                }
                finally
                {
                    mailManager.reset();
                }
            }

            if (crawlQueue.isActive())
            {
                executeReset();
                crawlQueue.finish(isQueueNeeded);
            }

            if (isExecuteSinglePass())
            {
                appLogger.info("Custom phase list assigned - stopping after one pass.");
                break;
            }
            else
            {
                appLogger.debug(String.format("Sleeping for %d minutes before next crawl review.", mSleepTimeInMinutes));
                Sleep.forMinutes(mSleepTimeInMinutes);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

	/**
     * Once the task has completed its run, then this method
     * will be invoked by the Application Manager to enable
     * the task to release any resources it was holding.
     * <p>
     * <b>Note:</b>If the JVM detects and external shutdown
     * event (e.g. service is being stopped), then the
     * Application Manager will asynchronously invoke this
     * in hopes that the task can save its state prior to
     * the process exiting.
	 * </p>
     */
    @Override
    public void shutdown()
    {
        Logger appLogger = mAppMgr.getLogger(this, "shutdown");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isAlive())
        {
            appLogger.info("The shutdown method was invoked.");
            executeReset();
            Sleep.forSeconds(1);
            mIsAlive.set(false);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
