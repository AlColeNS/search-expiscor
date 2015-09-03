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
import com.nridge.connector.common.con_com.transform.Pipeline;
import com.nridge.core.app.mail.MailManager;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.io.xml.DocumentXML;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The RunMetricReport is responsible for taking items off
 * the publish queue and captures the processing metrics
 * in the application log file.
 *
 * @see <a href="http://commons.apache.org/proper/commons-math/userguide/stat.html">Apache Commons Mathematics Library</a>
 * @see <a href="http://tutorials.jenkov.com/java-util-concurrent/blockingqueue.html">Java BlockingQueue Tutorial</a>
 * @see <a href="http://www.ibm.com/developerworks/library/j-jtp05236/">Dealing with InterruptedException</a>
 */
public class RunMetricReport implements Runnable
{
    private final double MILLISECONDS_IN_A_SECOND = 1000.0;

    private final AppMgr mAppMgr;
    private final CrawlQueue mCrawlQueue;

    public RunMetricReport(final AppMgr anAppMgr, CrawlQueue aCrawlQueue)
    {
        mAppMgr = anAppMgr;
        mCrawlQueue = aCrawlQueue;
    }

    private void writePhaseMetric(String aName, long aCount, double aSum)
    {
        double docsPerSecond;
        Logger appLogger = mAppMgr.getLogger(this, "writePhaseMetric");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = String.format("%s.%s.thread_count", Constants.CFG_PROPERTY_PREFIX, aName);
        int threadCount = mAppMgr.getInt(propertyName, 1);

        if ((aCount > 0L) && (aSum > 0.0))
            docsPerSecond = aCount / aSum;
        else
            docsPerSecond = 0.0;
        String msgStr = String.format("Phase metric for %s (%d threads): %d documents, %.2f seconds (%.2f docs/sec avg)",
                                      aName,threadCount,  aCount, aSum, docsPerSecond);
        appLogger.info(msgStr);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p/>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run()
    {
        long msTime;
        String[] phaseTimes;
        double secondsTime, docsPerSecond;
        String docId, queueItem, phaseName;
        Logger appLogger = mAppMgr.getLogger(this, "run");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        long extractCount = 0;
        DescriptiveStatistics dsExtract = new DescriptiveStatistics();
        long transformCount = 0;
        DescriptiveStatistics dsTransform = new DescriptiveStatistics();
        long publishCount = 0;
        DescriptiveStatistics dsPublish = new DescriptiveStatistics();

        BlockingQueue publishQueue = (BlockingQueue) mAppMgr.getProperty(Connector.QUEUE_PUBLISH_NAME);

        do
        {
            try
            {
                queueItem = (String) publishQueue.poll(Constants.QUEUE_POLL_TIMEOUT_DEFAULT, TimeUnit.SECONDS);
                if (mCrawlQueue.isQueueItemDocument(queueItem))
                {
                    StopWatch stopWatch = new StopWatch();
                    stopWatch.start();

                    docId = Connector.docIdFromQueueItem(queueItem);

                    appLogger.debug(String.format("Publish Queue Item: %s", docId));

                    phaseTimes = Connector.phaseTimeFromQueueItem(queueItem);
                    if (phaseTimes != null)
                    {
                        for (String phaseTime : phaseTimes)
                        {
                            phaseName = Connector.phaseFromPhaseTime(phaseTime);
                            msTime = Connector.timeFromPhaseTime(phaseTime);
                            if (StringUtils.equals(phaseName, Connector.PHASE_EXTRACT))
                            {
                                extractCount++;
                                secondsTime = msTime / MILLISECONDS_IN_A_SECOND;
                                dsExtract.addValue(secondsTime);
                            }
                            else if (StringUtils.equals(phaseName, Connector.PHASE_TRANSFORM))
                            {
                                transformCount++;
                                secondsTime = msTime / MILLISECONDS_IN_A_SECOND;
                                dsTransform.addValue(secondsTime);
                            }
                            else if (StringUtils.equals(phaseName, Connector.PHASE_PUBLISH))
                            {
                                publishCount++;
                                secondsTime = msTime / MILLISECONDS_IN_A_SECOND;
                                dsPublish.addValue(secondsTime);
                            }
                        }
                    }
                }
            }
            catch (InterruptedException e)
            {
                queueItem = StringUtils.EMPTY;
            }
        }
        while (! mCrawlQueue.isPhaseComplete(Connector.PHASE_PUBLISH, queueItem));

// Note: This is the end of the queue processing pipeline, so we will not pass on queue item markers.

// Generate our metrics summary for the log file.

        writePhaseMetric(Connector.PHASE_EXTRACT, extractCount, dsExtract.getSum());
        writePhaseMetric(Connector.PHASE_TRANSFORM, transformCount, dsTransform.getSum());
        writePhaseMetric(Connector.PHASE_PUBLISH, publishCount, dsPublish.getSum());

        double totalTime = dsExtract.getSum() + dsTransform.getSum() + dsPublish.getSum();
        if ((publishCount > 0L) && (totalTime > 0.0))
            docsPerSecond = publishCount / totalTime;
        else
            docsPerSecond = 0.0;
        String msgStr = String.format("Total metric summary: %d documents, %.2f seconds (%.2f docs/sec avg)",
                                      publishCount, totalTime, docsPerSecond);
        appLogger.info(msgStr);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
