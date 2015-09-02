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

package com.nridge.core.app.mgr;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataDateTimeField;
import com.nridge.core.base.io.xml.DataBagXML;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;

import java.io.File;
import java.util.Date;

/**
 * The Service Timer offers a collection of methods that can assist
 * an application in managing its sleep and execute periods based
 * on an external property file.  The logic recognizes full and
 * incremental service wake-up periods.
 *
 * @since 1.0
 * @author Al Cole
 */
public class ServiceTimer
{
    private final String PROPERTY_PREFIX_DEFAULT = "app";

    private AppMgr mAppMgr;
    private DataBag mTimeBag;
    private String mPropertyPrefix;
    private boolean mIsFirstService;

    /**
     * Constructor that accepts an application manager instance for
     * property list access.
     *
     * @param anAppMgr Application manager instance.
     */
    public ServiceTimer(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;

        Date tsNow = new Date();
        String tsValue = Field.dateValueFormatted(tsNow, Field.FORMAT_DATETIME_DEFAULT);

        mTimeBag = new DataBag("Service TS");
        DataDateTimeField tsFullField = new DataDateTimeField("full_service_ts", "Last Full Service TS", tsValue);
        mTimeBag.add(tsFullField);
        DataDateTimeField tsIncrementField = new DataDateTimeField("incremental_service_ts", "Last Incremental Service TS", tsValue);
        mTimeBag.add(tsIncrementField);

        setPropertyPrefix(PROPERTY_PREFIX_DEFAULT);

        mIsFirstService = true;
    }

    /**
     * Returns the property prefix string assigned previously
     * in the constructor.
     *
     * @return Property prefix string.
     */
    public String getPropertyPrefix()
    {
        return mPropertyPrefix;
    }

    /**
     * Assigns the property prefix to the service timer.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setPropertyPrefix(String aPropertyPrefix)
    {
        mPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Convenience method that returns the value of an application
     * manager property using the concatenation of the property
     * prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     *
     * @return Matching property value.
     */
    public String getAppString(String aSuffix)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mPropertyPrefix + aSuffix;
        else
            propertyName = mPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of an application
     * manager property using the concatenation of the property
     * prefix and suffix values.  If the property is not found,
     * then the default value parameter will be returned.
     *
     * @param aSuffix       Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    public String getAppString(String aSuffix, String aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mPropertyPrefix + aSuffix;
        else
            propertyName = mPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName, aDefaultValue);
    }

    /**
     * Returns a data bag responsible for managing the time-related fields.
     *
     * @return Data bag instance.
     */
    public DataBag getBag()
    {
        return mTimeBag;
    }

    /**
     * Assigns a <i>Date</i> timestamp representing the time of the last
     * full service execution.
     *
     * @param aTS Timestamp.
     */
    public void setLastFullServiceTS(Date aTS)
    {
        mTimeBag.setValueByName("full_service_ts", aTS);
    }

    /**
     * Returns the <i>Date</i> timestamp of the last full service
     * execution.
     *
     * @return Timestamp.
     */
    public Date getLastFullServiceTS()
    {
        Date tsValue = mTimeBag.getValueAsDate("full_service_ts");
        if (tsValue == null)
            return new Date();
        else
            return tsValue;
    }

    /**
     * Assigns a <i>Date</i> timestamp representing the time of the last
     * incremental service execution.
     *
     * @param aTS Timestamp.
     */
    public void setLastIncrementalServiceTS(Date aTS)
    {
        mTimeBag.setValueByName("incremental_service_ts", aTS);
    }

    /**
     * Returns the <i>Date</i> timestamp of the last incremental service
     * execution.
     *
     * @return Timestamp.
     */
    public Date getLastIncrementalServiceTS()
    {
        Date tsValue = mTimeBag.getValueAsDate("incremental_service_ts");
        if (tsValue == null)
            return new Date();
        else
            return tsValue;
    }

    private Date getNextTS(String aFieldName, Date aTS)
    {
        String timeString = getAppString(aFieldName);
        if (StringUtils.isNotEmpty(timeString))
        {
            if (StringUtils.endsWithIgnoreCase(timeString, "m"))
            {
                String minuteString = StringUtils.stripEnd(timeString, "m");
                if ((StringUtils.isNotEmpty(minuteString)) && (StringUtils.isNumeric(minuteString)))
                {
                    int minuteAmount = Integer.parseInt(minuteString);
                    return DateUtils.addMinutes(aTS, minuteAmount);
                }
            }
            else if (StringUtils.endsWithIgnoreCase(timeString, "h"))
            {
                String hourString = StringUtils.stripEnd(timeString, "h");
                if ((StringUtils.isNotEmpty(hourString)) && (StringUtils.isNumeric(hourString)))
                {
                    int hourAmount = Integer.parseInt(hourString);
                    return DateUtils.addHours(aTS, hourAmount);
                }
            }
            else    // we assume days
            {
                String dayString = StringUtils.stripEnd(timeString, "d");
                if ((StringUtils.isNotEmpty(dayString)) && (StringUtils.isNumeric(dayString)))
                {
                    int dayAmount = Integer.parseInt(dayString);
                    return DateUtils.addDays(aTS, dayAmount);
                }
            }
        }

// Push 1 hour ahead to avoid triggering a match with TS

        return DateUtils.addHours(new Date(), 1);
    }

    /**
     * Returns <i>true</i> if the time for a full service wake-up has
     * arrived.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isTimeForFullService()
    {
        boolean isTime = false;
        Logger appLogger = mAppMgr.getLogger(this, "isTimeForFullService");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mIsFirstService)
        {
            Date tsNow = new Date();
            String msgStr = String.format("The full service time of '%s' has arrived (first time event).",
                                          Field.dateValueFormatted(tsNow, Field.FORMAT_DATETIME_DEFAULT));
            appLogger.debug(msgStr);

            isTime = true;
            mIsFirstService = false;
        }
        else
        {
            Date tsNow = new Date();
            Date tsFullService = getNextTS("run_full_interval", getLastFullServiceTS());
            if (tsNow.after(tsFullService))
            {
                String msgStr = String.format("The full service time of '%s' has arrived.",
                                              Field.dateValueFormatted(tsFullService, Field.FORMAT_DATETIME_DEFAULT));
                appLogger.debug(msgStr);
                isTime = true;
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return isTime;
    }

    /**
     * Returns <i>true</i> if the time for an incremental service wake-up has
     * arrived.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isTimeForIncrementalService()
    {
        boolean isTime = false;
        Logger appLogger = mAppMgr.getLogger(this, "isTimeForIncrementalService");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Date tsNow = new Date();
        Date tsIncrementalService = getNextTS("run_incremental_interval", getLastIncrementalServiceTS());
        if (tsNow.after(tsIncrementalService))
        {
            String msgStr = String.format("The incremental service time of '%s' has arrived.",
                                          Field.dateValueFormatted(tsIncrementalService, Field.FORMAT_DATETIME_DEFAULT));
            appLogger.debug(msgStr);
            isTime = true;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return isTime;
    }

    /**
     * Returns the absolute path/file of the service tracker file based
     * on the property settings of the application manager.
     *
     * @return Service tracker path/file name.
     */
    public String createServicePathFileName()
    {
        Logger appLogger = mAppMgr.getLogger(this, "createServicePathFileName");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = mAppMgr.APP_PROPERTY_LOG_PATH;
        String logPathName = mAppMgr.getString(propertyName);
        if (StringUtils.isEmpty(logPathName))
        {
            appLogger.error(propertyName + ": Is undefined");
            logPathName = "log";
        }
        String stPathFileName = String.format("%s%cservice-tracker.xml", logPathName,
            File.separatorChar);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stPathFileName;
    }

    /**
     * Saves the state of the full and incremental timestamps to an external
     * XML file called <b>service-tracker.xml</b>.  By default, this file
     * is stored in the application logging folder.
     */
    public void save()
    {
        Logger appLogger = mAppMgr.getLogger(this, "save");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String stPathFileName = createServicePathFileName();
        DataBagXML bagXML = new DataBagXML(mTimeBag);
        try
        {
            bagXML.save(stPathFileName);
        }
        catch (Exception e)
        {
            appLogger.error(stPathFileName + ": " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Retrieves the state of the full and incremental timestamps from an
     * external XML file called <b>service-tracker.xml</b>.  By default,
     * this file is located in the application logging folder.
     */
    public void load()
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String stPathFileName = createServicePathFileName();
        DataBagXML bagXML = new DataBagXML();
        try
        {
            bagXML.load(stPathFileName);
            DataBag complexBag = bagXML.getBag();
            if (complexBag.count() == mTimeBag.count())
            {
                mTimeBag = complexBag;
                mIsFirstService = false;
            }
        }
        catch (Exception e)
        {
            appLogger.error(stPathFileName + ": " + e.getMessage(), e);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
