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

import com.nridge.core.app.mgr.AppMgr;
import org.apache.commons.lang3.StringUtils;

/**
 * The Constants class is responsible for centralizing the constant
 * values used throughout the application.
 */
public class Constants
{
    public static final int HTTP_PORT_NUMBER_DEFAULT = 80;
    public static final int APPLICATION_PORT_NUMBER_DEFAULT = 8211;

    public static final String CFG_PROPERTY_PREFIX = "connector.ws";

    public static final String PROPERTY_CRAWL_FOLLOW = "CrawlFollow";
    public static final String PROPERTY_CRAWL_IGNORE = "CrawlIgnore";
    public static final String PROPERTY_RESTLET_APPLICATION = "RestletApplication";


    public static final String OPERATION_QUERY = "query";
    public static final String OPERATION_FETCH = "fetch";
    public static final String OPERATION_UPDATE = "update";

    public static final int STATUS_CODE_FAILURE = 1;
    public static final int STATUS_CODE_SUCCESS = 0;
    public static final String FEATURE_STATUS_CODE = "statusCode";
    public static final String FEATURE_STATUS_MESSAGE = "statusMessage";

    public static final String REPLY_MESSAGE_FAILURE = "The operation failed to complete.";
    public static final String REPLY_MESSAGE_SUCCESS = "The operation successfully completed.";

    public static final String HOST_NAME_DEFAULT = "localhost";

    public static final String SCHEMA_FILE_NAME = "ds_ws_schema.xml";

    public static final String WS_DOCUMENT_TYPE = "WS Document";

    public static final long QUEUE_POLL_TIMEOUT_DEFAULT = 60L;

    public static final String MAIL_SERVICE_NAME = "NSD WS Connector Service";
    public static final String MAIL_SUBJECT_INFO = "NSD WS Connector Service Report";
    public static final String MAIL_DETAIL_MESSAGE = "Please refer to the 'con_ws.log' file for details.";

    private Constants()
    {

    }

    public static int getCfgSleepValue(AppMgr anAppMgr, String aCfgName, int aDefaultValue)
    {
        int sleepInSeconds = aDefaultValue;

        String timeString = anAppMgr.getString(aCfgName);
        if (StringUtils.isNotEmpty(timeString))
        {
            if (StringUtils.endsWithIgnoreCase(timeString, "m"))
            {
                String minuteString = StringUtils.stripEnd(timeString, "m");
                if ((StringUtils.isNotEmpty(minuteString)) && (StringUtils.isNumeric(minuteString)))
                {
                    int minuteAmount = Integer.parseInt(minuteString);
                    sleepInSeconds = minuteAmount * 60;
                }
            }
            else if (StringUtils.endsWithIgnoreCase(timeString, "h"))
            {
                String hourString = StringUtils.stripEnd(timeString, "h");
                if ((StringUtils.isNotEmpty(hourString)) && (StringUtils.isNumeric(hourString)))
                {
                    int hourAmount = Integer.parseInt(hourString);
                    sleepInSeconds = hourAmount * 60 * 60;
                }
            }
            else if (StringUtils.endsWithIgnoreCase(timeString, "d"))
            {
                String dayString = StringUtils.stripEnd(timeString, "d");
                if ((StringUtils.isNotEmpty(dayString)) && (StringUtils.isNumeric(dayString)))
                {
                    int dayAmount = Integer.parseInt(dayString);
                    sleepInSeconds = dayAmount * 60 * 60 * 24;
                }
            }
            else    // we assume seconds
            {
                String secondString = StringUtils.stripEnd(timeString, "s");
                if ((StringUtils.isNotEmpty(secondString)) && (StringUtils.isNumeric(secondString)))
                {
                    sleepInSeconds = Integer.parseInt(secondString);
                }
            }
        }

        return sleepInSeconds;
    }
}
