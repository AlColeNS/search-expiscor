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

package com.nridge.connector.common.con_com.crawl;

import com.nridge.connector.common.con_com.Connector;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.io.xml.DocumentXML;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * The CrawlQueue is responsible for managing the files associated
 * with the ETL process and the queues those documents fall within.
 *
 * @since 1.0
 * @author Al Cole
 */
@SuppressWarnings("unchecked")
public class CrawlQueue
{
	private final int CRAWL_BEGINNING_OF_TIME = -100;

	private long mCrawlId;
	private String mCrawlType;
	private final AppMgr mAppMgr;
	private Date mCrawlLastModified;
	private HashMap<String,AtomicBoolean> mPhaseComplete;
	private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

	/**
     * Constructor accepts an application manager parameter and initializes
	 * the object accordingly.
	 *
	 * @param anAppMgr Application manager.
	*/
	public CrawlQueue(final AppMgr anAppMgr)
	{
		mAppMgr = anAppMgr;
		mCrawlType = StringUtils.EMPTY;
		mPhaseComplete = new HashMap<String,AtomicBoolean>();
		mPhaseComplete.put(Connector.PHASE_EXTRACT, new AtomicBoolean(false));
		mPhaseComplete.put(Connector.PHASE_TRANSFORM, new AtomicBoolean(false));
		mPhaseComplete.put(Connector.PHASE_PUBLISH, new AtomicBoolean(false));
		mCrawlLastModified = DateUtils.addYears(new Date(), CRAWL_BEGINNING_OF_TIME);
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
	 * Assigns a configuration property prefix string.
	 *
	 * @param aPropertyPrefix Property prefix.
	 */
	public void setCfgPropertyPrefix(String aPropertyPrefix)
	{
		mCfgPropertyPrefix = aPropertyPrefix;
	}

	/**
	 * Convenience method that returns the value of a property using
	 * the concatenation of the property prefix and suffix values.
	 *
	 * @param aSuffix Property name suffix.
	 * @return Matching property value.
	 */
	private String getCfgString(String aSuffix)
	{
		String propertyName;

		if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
			propertyName = mCfgPropertyPrefix + aSuffix;
		else
			propertyName = mCfgPropertyPrefix + "." + aSuffix;

		return mAppMgr.getString(propertyName);
	}

	/**
	 * Convenience method that returns the value of a property using
	 * the concatenation of the property prefix and suffix values.
	 * If the property is not found, then the default value parameter
	 * will be returned.
	 *
	 * @param aSuffix Property name suffix.
	 * @param aDefaultValue Default value.
	 *
	 * @return Matching property value or the default value.
	 */
	private String getCfgString(String aSuffix, String aDefaultValue)
	{
		String propertyName;

		if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
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
	private int getCfgInteger(String aSuffix, int aDefaultValue)
	{
		String propertyName;

		if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
			propertyName = mCfgPropertyPrefix + aSuffix;
		else
			propertyName = mCfgPropertyPrefix + "." + aSuffix;

		return mAppMgr.getInt(propertyName, aDefaultValue);
	}

	/**
	 * Returns <i>true</i> if the a property value evaluates to <i>true</i>.
	 *
	 * @param aSuffix Property name suffix.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	private boolean isCfgStringTrue(String aSuffix)
	{
		String propertyValue = getCfgString(aSuffix);
		return StrUtl.stringToBoolean(propertyValue);
	}

	/**
	 * Returns the current crawler id.  A non-zero value implies
	 * that ETL process is active.
	 *
	 * @return Crawler id.
	 */
	public long getCrawlId()
	{
		return mCrawlId;
	}

	/**
	 * Returns the current crawler type (full or incremental)
	 * or an empty string if the ETL process is not active.
	 *
	 * @return Crawler type.
	 */
	public String getCrawlType()
	{
		return mCrawlType;
	}

	/**
	 * The crawl queue (as a convenience to the parent application)
	 * is used to capture the last modified time for the incremental
	 * crawl extraction phase.
	 *
	 * @return Last modified timestamp to use for comparison.
	 */
	public Date getCrawlLastModified()
	{
		return mCrawlLastModified;
	}

	/**
	 * Composes and returns the root queue path name for phase document storage.
	 *
	 * @return Root queue path name.
	 */
	public String queuePathName()
	{
		return String.format("%s%cqueue", mAppMgr.getString(mAppMgr.APP_PROPERTY_INS_PATH), File.separatorChar);
	}

	/**
	 * Composes and returns the parent crawl queue path name for phase document storage.
	 *
	 * @return Parent crawl queue path name.
	 */
	public String crawlPathName()
	{
		return String.format("%s%c%d", queuePathName(), File.separatorChar, mCrawlId);
	}

	/**
	 * Composes and returns the parent crawl queue path name for phase document storage.
	 *
	 * @param aQueueName Root queue path name.
	 *
	 * @return Parent crawl queue path name.
	 */
	public String crawlPathName(String aQueueName)
	{
		return String.format("%s%c%s", crawlPathName(), File.separatorChar, aQueueName);
	}

	/**
	 * Composes and returns the path/file name where the lock file should be written to.
	 *
	 * @return Lock file path/file name.
	 */
	public String lockPathFileName()
	{
		return String.format("%s%c%s", queuePathName(), File.separatorChar, Connector.LOCK_FILE_NAME);
	}

	/**
	 * Creates the folder for the path name parameter.
	 *
	 * @param aPathName Name of folder to create.
	 *
	 * @throws NSException Indicates an I/O error condition.
	 */
	public void createPathName(String aPathName)
		throws NSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "createPathName");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		File pathFile = new File(aPathName);
		if (! pathFile.exists())
		{
			if (! pathFile.mkdir())
				throw new NSException(String.format("%s: Unable to create folder.", aPathName));
			appLogger.debug(String.format("%s: Folder created.", aPathName));
		}
		else
			appLogger.debug(String.format("%s: Folder exists.", aPathName));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	private boolean acquireLock()
	{
		Logger appLogger = mAppMgr.getLogger(this, "acquireLock");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		String lckPathFileName = lockPathFileName();
		File lockFile = new File(lckPathFileName);
		if (lockFile.exists())
		{
			appLogger.debug(String.format("%s: Lock exists.", lckPathFileName));
			return false;
		}
		else
		{
			try
			{
				FileUtils.write(lockFile, Long.toString(mCrawlId), StrUtl.CHARSET_UTF_8);
			}
			catch (IOException e)
			{
				appLogger.debug(String.format("%s: %s.", lckPathFileName, e.getMessage()));
				return false;
			}
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return true;
	}

	/**
	 * Returns <i>true</i> if the lock file exists indicating an active
	 * crawl process is underway and <i>false</i> otherwise.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isLockActive()
	{
		File lockFile = new File(lockPathFileName());
		return lockFile.exists();
	}

	private void readLock()
	{
		Logger appLogger = mAppMgr.getLogger(this, "readLock");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		String lckPathFileName = lockPathFileName();
		File lockFile = new File(lckPathFileName);
		if (lockFile.exists())
		{
			try
			{
				String crawlIdString = FileUtils.readFileToString(lockFile, StrUtl.CHARSET_UTF_8);
				mCrawlId = Long.parseLong(crawlIdString);
			}
			catch (IOException e)
			{
				appLogger.debug(String.format("%s: %s.", lckPathFileName, e.getMessage()));
			}
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	private boolean releaseLock()
	{
		boolean isReleased;
		Logger appLogger = mAppMgr.getLogger(this, "releaseLock");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		File lockFile = new File(lockPathFileName());
		if (lockFile.exists())
			isReleased = lockFile.delete();
		else
			isReleased = true;

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return isReleased;
	}

	/**
	 * Returns <i>true</i> indicating that a crawl id is non-zero
	 * and the that a lock file exists for the queue folder.
	 * Otherwise a <i>false</i> value will be returned.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isActive()
	{
		return (mCrawlId != 0) && (isLockActive());
	}

	private long nextCrawlId()
	{
		UUID uniqueId = UUID.randomUUID();
		byte idBytes[] = uniqueId.toString().getBytes();
		Checksum checksumValue = new CRC32();
		checksumValue.update(idBytes, 0, idBytes.length);

		return checksumValue.getValue();
	}

	private void start(long aCrawlId)
		throws NSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "start");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (aCrawlId != 0L)
		{
			if (isActive())
				throw new NSException("Crawler lock is active.");
			else
			{
				mCrawlId = aCrawlId;
				createPathName(queuePathName());
				createPathName(crawlPathName());
				createPathName(crawlPathName(Connector.QUEUE_EXTRACT_NAME));
				createPathName(crawlPathName(Connector.QUEUE_TRANSFORM_NAME));
				createPathName(crawlPathName(Connector.QUEUE_PUBLISH_NAME));
				acquireLock();
			}
		}
		else
			throw new NSException("Crawler id value is zero.");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Initiates a new crawl queue in the file system. As part of the
	 * process, new crawl id, lock file, date last modified timestamp
	 * and crawl type values will be assigned to internal variables.
	 * This method should be called at the start of the ETL process.
	 *
	 * @param aCrawlType Crawl type (full, incremental)
	 * @param aDateLastModified Date last modified.
	 *
	 * @throws NSException Indicates an I/O error or configuration issue.
	 */
	public void start(String aCrawlType, Date aDateLastModified)
		throws NSException
	{
		mCrawlType = aCrawlType;
		mCrawlLastModified = aDateLastModified;
		start(nextCrawlId());
	}

	/**
	 * Initiates a new crawl queue in the file system. As part of the
	 * process, new crawl id, lock file, date last modified timestamp
	 * and crawl type values will be assigned to internal variables.
	 * This method should be called at the start of the ETL process.
	 *
	 * @param aCrawlType Crawl type (full, incremental)
	 * @throws NSException Indicates and I/O error condition.
	 */
	public void start(String aCrawlType)
		throws NSException
	{
		Date wayBackDate = DateUtils.addYears(new Date(), CRAWL_BEGINNING_OF_TIME);
		start(aCrawlType, wayBackDate);
	}

	/**
	 * Places the queue item marker into the queue identified by queue name.
	 *
	 * @param aQueueName Queue name (e.g. extract, transform, publish)
	 * @param aQueueItemMarker Queue item marker (e.g. NSD-CrawlFinish)
	 */
	public void putMarkerIntoQueue(String aQueueName, String aQueueItemMarker)
	{
		Logger appLogger = mAppMgr.getLogger(this, "putMarkerIntoQueue");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		BlockingQueue blockingQueue = (BlockingQueue) mAppMgr.getProperty(aQueueName);
		if (blockingQueue == null)
			appLogger.error(String.format("Queue name '%s' from AppMgr is null.", aQueueName));
		else if (! isQueueItemMarker(aQueueItemMarker))
			appLogger.error(String.format("Queue marker '%s' is not valid - cannot put in queue.",
										  aQueueItemMarker));
		else
		{
			try
			{
				blockingQueue.put(aQueueItemMarker);
			}
			catch (InterruptedException e)
			{
				// Restore the interrupted status so parent can handle (if it wants to).
				Thread.currentThread().interrupt();
			}
			appLogger.debug(String.format("Queue '%s' had a marker of '%s' placed into it.",
										  aQueueName, aQueueItemMarker));
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Performs a test to determine if the queue item is valid for processing.
	 *
	 * @param aQueueItem Queue item.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isQueueItemValid(String aQueueItem)
	{
		return StringUtils.isNotEmpty(aQueueItem);
	}

	/**
	 * Performs a test to determine if the queue item represents a marker.
	 *
	 * @param aQueueItem Queue item.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isQueueItemMarker(String aQueueItem)
	{
		if (isQueueItemValid(aQueueItem))
		{
			boolean queueIsAborted = StringUtils.equals(aQueueItem, Connector.QUEUE_ITEM_CRAWL_ABORT);
			boolean queueIsStarted = StringUtils.equals(aQueueItem, Connector.QUEUE_ITEM_CRAWL_START);
			boolean queueIsFinished = StringUtils.equals(aQueueItem, Connector.QUEUE_ITEM_CRAWL_FINISH);

			return ((queueIsAborted) || (queueIsStarted) || (queueIsFinished));
		}
		else
			return false;
	}

	/**
	 * Performs a test to determine if the queue item represents a document.
	 *
	 * @param aQueueItem Queue item.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isQueueItemDocument(String aQueueItem)
	{
		if ((isQueueItemValid(aQueueItem)) && (! isQueueItemMarker(aQueueItem)))
			return true;
		else
			return false;
	}

	/**
	 * Evaluates if the phase has completed its processing cycle.  A phase
	 * is considered complete if the application is no longer alive or the
	 * queue item represents a crawl finish or abort marker.
	 *
	 * @param aPhase Name of the phase being evaluated (used for logging).
	 * @param aQueueItem Queue item.
	 *
	 * @return <i>true</i> or <i>false</i>
	 */
	public boolean isPhaseComplete(String aPhase, String aQueueItem)
	{
		boolean isPhaseAlreadyComplete;
		Logger appLogger = mAppMgr.getLogger(this, "isPhaseComplete");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		AtomicBoolean atomicBoolean = mPhaseComplete.get(aPhase);
		if (atomicBoolean == null)
		{
			isPhaseAlreadyComplete = true;
			appLogger.error(String.format("Phase name '%s' atomic boolean is null.", aPhase));
		}
		else
			isPhaseAlreadyComplete = atomicBoolean.get();

		boolean appMgrIsAlive = mAppMgr.isAlive();
		boolean queueItemIsValid = isQueueItemValid(aQueueItem);
		boolean queueIsAborted = StringUtils.equals(aQueueItem, Connector.QUEUE_ITEM_CRAWL_ABORT);
		boolean queueIsFinished = StringUtils.equals(aQueueItem, Connector.QUEUE_ITEM_CRAWL_FINISH);

		boolean isComplete = ((! appMgrIsAlive) || (isPhaseAlreadyComplete) || (queueIsAborted) || (queueIsFinished));

		if (isComplete)
		{
			if ((atomicBoolean != null) && (! atomicBoolean.get()))
				atomicBoolean.set(true);
			appLogger.debug(String.format("Phase Complete %s: queueItemIsValid = %s, isPhaseAlreadyComplete = %s, appMgrIsAlive = %s, queueIsAborted = %s, queueIsFinished = %s",
											aPhase, queueItemIsValid, isPhaseAlreadyComplete, appMgrIsAlive, queueIsAborted, queueIsFinished));
		}
		else
			appLogger.debug(String.format("Phase Continue %s: queueItemIsValid = %s, isPhaseAlreadyComplete = %s, appMgrIsAlive = %s, queueIsAborted = %s, queueIsFinished = %s",
										  aPhase, queueItemIsValid, isPhaseAlreadyComplete, appMgrIsAlive, queueIsAborted, queueIsFinished));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return isComplete;
	}

	/**
	 * Composes a path/file name for documents stored in the queue folder
	 * identified by the parameter.
	 *
	 * @param aQueueName Queue name (e.g. extract, transform, publish)
	 * @param aDocId Unique document identifier.
	 *
	 * @return Queue document path/file name.
	 */
	public String docPathFileName(String aQueueName, String aDocId)
	{
		return String.format("%s%c%s.xml", crawlPathName(aQueueName), File.separatorChar, aDocId);
	}

	private void renameQueuePathFileName(String aSrcPathFileName, String aDstPathFileName,
										 boolean aIsSrcRequired)
		throws NSException
	{
		File srcFile = new File(aSrcPathFileName);
		File dstFile = new File(aDstPathFileName);
		if (srcFile.exists())
		{
			if (! srcFile.renameTo(dstFile))
				throw new NSException(String.format("'%s' to '%s': Unable to rename file.", aSrcPathFileName, aDstPathFileName));
		}
		else
		{
			if (aIsSrcRequired)
				throw new NSException(String.format("%s: Does not exist.", aSrcPathFileName));
		}
	}

	/**
	 * Using the file system, this method will perform an atomic rename
	 * of the document from one queue folder to another.
	 *
	 * @param aSrcQueueName Source queue name (e.g. extract, transform)
	 * @param aDstQueueName Destination queue name (e.g. transform, publish)
	 * @param aDocId Unique document identifier.
	 *
	 * @throws NSException Indicates and I/O error condition.
	 */
	public void transition(String aSrcQueueName, String aDstQueueName, String aDocId)
		throws NSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "transition");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		String srcDocPathFileName = docPathFileName(aSrcQueueName, aDocId);
		String dstDocPathFileName = docPathFileName(aDstQueueName, aDocId);

		renameQueuePathFileName(srcDocPathFileName, dstDocPathFileName, true);

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Using the file system, this method will transition the source document
	 * from its source queue folder to the destination.  The document
	 * parameter will be used to represent the document in its destination
	 * queue.  The source document will simply be deleted from the file
	 * system.
	 *
	 * @param aSrcQueueName Source queue name (e.g. extract, transform)
	 * @param aDstQueueName Destination queue name (e.g. transform, publish)
	 * @param aDocument Document of fields to store.
	 * @param aDocId Unique document identifier.
	 * @throws NSException Indicates a configuration issue.
	 * @throws IOException Indicates and I/O error condition.
	 */
	public void transition(String aSrcQueueName, String aDstQueueName,
						   Document aDocument, String aDocId)
		throws NSException, IOException
	{
		Logger appLogger = mAppMgr.getLogger(this, "transition");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		String srcDocPathFileName = docPathFileName(aSrcQueueName, aDocId);
		String dstDocPathFileName = docPathFileName(aDstQueueName, aDocId);

		DocumentXML documentXML = new DocumentXML(aDocument);
		documentXML.save(dstDocPathFileName);

		File srcFile = new File(srcDocPathFileName);
		if (! srcFile.delete())
			throw new NSException(String.format("%s: Unable to delete.", srcDocPathFileName));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Clears the active state from the queue file system.  This involves
	 * clearing the crawl id, extraction complete flag, crawl type and
	 * date last modified.  Finally, this method will remove the queue
	 * lock file.
	 *
	 * @return <i>true</i> if lock file is successfully removed or
	 * 		   <i>false</i> otherwise.
	 */
	public boolean clear()
	{
		Logger appLogger = mAppMgr.getLogger(this, "clear");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		mCrawlId = 0L;
		mCrawlType = StringUtils.EMPTY;
		mPhaseComplete.put(Connector.PHASE_EXTRACT, new AtomicBoolean(false));
		mPhaseComplete.put(Connector.PHASE_TRANSFORM, new AtomicBoolean(false));
		mPhaseComplete.put(Connector.PHASE_PUBLISH, new AtomicBoolean(false));
		mCrawlLastModified = DateUtils.addYears(new Date(), CRAWL_BEGINNING_OF_TIME);

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return releaseLock();
	}

	/**
	 * Performs a <code>clear()</code> on the queue followed by the
	 * removal of any residual queue files and folders.
	 */
	public void reset()
	{
		Logger appLogger = mAppMgr.getLogger(this, "reset");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (clear())
		{
			appLogger.debug("Lock file released.");

			String queuePathName = queuePathName();
			File queueFile = new File(queuePathName);
			if (queueFile.exists())
			{
				File[] queueFolders = queueFile.listFiles();
				for (File subFolder : queueFolders)
				{
					if (subFolder.isDirectory())
					{
						try
						{
							FileUtils.deleteDirectory(subFolder);
						}
						catch (IOException e)
						{
							appLogger.warn("%s: %s", subFolder.getAbsolutePath(), e.getMessage());
						}
					}
				}
			}
		}
		else
			appLogger.error(String.format("%s: Unable to release lock file.", lockPathFileName()));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Completes a previous crawl queue process by performing a
	 * <code>clear()</code> or <code>reset()</code> on the queue
	 * file system depending on the parameter flag.
	 *
	 * This method should be called at the end of the ETL process.
	 *
	 * @param aIsQueueNeeded If <i>true</i>, then the queue state will
	 *                       only be cleared and not reset.  <i>false</i>
	 *                       will perform a complete reset of the queue
	 *                       file system.
	 */
	public void finish(boolean aIsQueueNeeded)
	{
		Logger appLogger = mAppMgr.getLogger(this, "finish");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (isActive())
		{
			if (aIsQueueNeeded)
				clear();
			else
				reset();
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}
}
