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

package com.nridge.connector.common.con_com;

import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang.time.DateUtils;

/**
 * The Connector class captures the constants, enumerated types
 * and utility methods for the content text extraction package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Connector
{
    public static final int QUEUE_LENGTH_DEFAULT = 5120;
    public static final String QUEUE_EXTRACT_NAME = "extract";
    public static final String QUEUE_TRANSFORM_NAME = "transform";
    public static final String QUEUE_PUBLISH_NAME = "publish";

// Queue item markers to indicate state changes in the phase processing.

    public static final String QUEUE_ITEM_CRAWL_ABORT = "NSD-CrawlAbort";
    public static final String QUEUE_ITEM_CRAWL_START = "NSD-CrawlStart";
    public static final String QUEUE_ITEM_CRAWL_FINISH = "NSD-CrawlFinish";

    public static final String PROPERTY_MAIL_NAME = "Mail";
    public static final String PROPERTY_SCHEMA_NAME = "Schema";
    public static final String PROPERTY_CRAWL_QUEUE = "CrawlQueue";

    public static final String STATUS_MAIL_ERROR = "Error";

    public static final String LOCK_FILE_NAME = "CrawlActive.lck";

    public static final String CFG_PROPERTY_PREFIX = "connector";

    public static final String TRANSFORMER_BAG_COPY = "bag_copy";

    public static final String PHASE_EXTRACT = "extract";
    public static final String PHASE_TRANSFORM = "transform";
    public static final String PHASE_PUBLISH = "publish";
    public static final String PHASE_SNAPSHOT = "snapshot";
    public static final String PHASE_ALL = "all";

    public static final String CRAWL_TYPE_FULL = "Full";
    public static final String CRAWL_TYPE_INCREMENTAL = "Incremental";

    public static final int PUBLISH_BATCH_DOC_COUNT = 100;
    public static final int PUBLISH_COMMIT_DOC_COUNT = 1000;
    public static final long PUBLISH_MAX_DOC_COUNT = 500000;

    public static final int RUN_STARTUP_SLEEP_DELAY = 2 * 60; // 2 minutes

    public static final String DOCUMENT_TYPE_UNKNOWN = "Unknown";

    public static final String TYPE_ARAS_INNOVATOR = "Aras Innovator";

    private Connector()
    {
    }

    /**
     * Used for the connector metric reporting module, this method will
     * encode a queue item string based on the parameters provided.
     *
     * @param aName Unique document identifier.
     * @param aPhase ETL phase (extract, transform, publish).
     * @param aTime Duration time (in milliseconds).
     *
     * @return Queue item string.
     */
    public static String queueItemIdPhaseTime(String aName, String aPhase, long aTime)
    {
        return String.format("%s%c%s%c%d", aName, StrUtl.CHAR_PIPE,
                             aPhase, StrUtl.CHAR_COLON, aTime);
    }

    /**
     * Extracts the document identifier from the previously encoded
     * queue item.
     *
     * @param aQueueItem Queue item string.
     *
     * @return Unique document identifier.
     */
    public static String docIdFromQueueItem(String aQueueItem)
    {
        int offset = aQueueItem.indexOf(StrUtl.CHAR_PIPE);
        if (offset == -1)
            return aQueueItem;
        else
            return aQueueItem.substring(0, offset);
    }

    /**
     * Extracts an array of ETL phase/time components from the previously
     * encoded queue item.
     *
     * @param aQueueItem Queue item string.
     *
     * @return Phase/Time array.
     */
    public static String[] phaseTimeFromQueueItem(String aQueueItem)
    {
        int offset = aQueueItem.indexOf(StrUtl.CHAR_PIPE);
        if (offset != -1)
        {
            String phaseString = aQueueItem.substring(offset+1);
            String delimiterString = String.format("%c%c", StrUtl.CHAR_BACKSLASH, StrUtl.CHAR_PIPE);
            String regExPattern = String.format("(?<!\\\\)%s", delimiterString);

            return phaseString.split(regExPattern);
        }

        return new String[0];
    }

    /**
     * Extracts an ETL phase (e.g. extract, transform, publish) from the
     * phase/time string.
     *
     * @param aPhaseTime Phase/time string.
     *
     * @return Phase name.
     */
    public static String phaseFromPhaseTime(String aPhaseTime)
    {
        int offset = aPhaseTime.indexOf(StrUtl.CHAR_COLON);
        if (offset == -1)
            return aPhaseTime;
        else
            return aPhaseTime.substring(0, offset);
    }

    /**
     * Extracts the duration time (in milliseconds) from the phase/time
     * string.
     *
     * @param aPhaseTime Phase/time string.
     *
     * @return Duration time.
     */
    public static long timeFromPhaseTime(String aPhaseTime)
    {
        int offset = aPhaseTime.indexOf(StrUtl.CHAR_COLON);
        if (offset != -1)
        {
            String timeString = aPhaseTime.substring(offset+1);
            return Long.parseLong(timeString);
        }

        return -1L;
    }
}
