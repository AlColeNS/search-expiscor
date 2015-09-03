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
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The CrawlIgnore class is responsible for managing a list of
 * content URIs that a connector could not crawl (ignore).
 *
 * @see <a href="http://www.regexplanet.com/advanced/java/index.html">Regular Expression Test Page</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class CrawlIgnore
{
    private final AppMgr mAppMgr;
    private ArrayList<Pattern> mPatternList;
    private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the object accordingly.
     *
     * @param anAppMgr Application manager.
     */
    public CrawlIgnore(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mPatternList = new ArrayList<Pattern>();
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
     * Determines if the name parameter matches the list of URIs
     * that should not be followed (ignored).
     *
     * @param aName File name (could be a URL).
     *
     * @return <i>true</i> if it matches, <i>false</i> otherwise.
     */
    public boolean isMatched(String aName)
    {
        Matcher urlMatcher;
        Logger appLogger = mAppMgr.getLogger(this, "isMatched");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (StringUtils.isNotEmpty(aName))
        {
            for (Pattern ignorePattern : mPatternList)
            {
                urlMatcher = ignorePattern.matcher(aName);
                if (urlMatcher.find())
                    return true;
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return false;
    }

    /**
     * Determines if the name parameter matches the list of URIs
     * that should not be followed (ignored). Prior to the lookup,
     * the name will be normalized (e.g. stripped of Window device
     * names and Window backslashes converted to forward slashes).
     *
     * @param aName File name (could be a URL).
     *
     * @return <i>true</i> if it matches, <i>false</i> otherwise.
     */
    public boolean isMatchedNormalized(String aName)
    {
        String pathFileName;
        boolean isNameMatched;
        Logger appLogger = mAppMgr.getLogger(this, "isMatchedNormalized");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (StringUtils.isNotEmpty(aName))
        {

/* Prepare the file name for evaluation - strip any device name and replace
backslashes with forward slashes. */

            if ((aName.length() > 2) && (aName.charAt(1) == StrUtl.CHAR_COLON))
                pathFileName = aName.substring(2);
            else
                pathFileName = aName;

            isNameMatched = isMatched(StringUtils.replaceChars(pathFileName, StrUtl.CHAR_BACKSLASH,
                                                               StrUtl.CHAR_FORWARDSLASH));
        }
        else
            isNameMatched = false;

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return isNameMatched;
    }

    /**
     * Parses a file identified by the path/file name parameter
     * and loads it into an internally managed ignore URI list.
     *
     * @param aPathFileName Absolute file name (e.g. 'crawl_ignore.txt').
     *
     * @throws IOException I/O related exception.
     */
    public void load(String aPathFileName)
        throws IOException
    {
        Pattern regexPattern;
        List<String> lineList;
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try (FileReader fileReader = new FileReader(aPathFileName))
        {
            lineList = IOUtils.readLines(fileReader);
        }

        for (String patternString : lineList)
        {
            if (! StringUtils.startsWith(patternString, "#"))
            {
                regexPattern = Pattern.compile(patternString);
                mPatternList.add(regexPattern);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Parses a file identified by the <i>crawl_ignore_file</i>
     * configuration property and loads it into an internally
     * managed ignore URI list.
     *
     * @throws IOException I/O related exception.
     * @throws NSException Missing configuration property.
     */
    public void load()
        throws IOException, NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = "crawl_ignore_file";
        String crawlIgnoreFileName = getCfgString(propertyName);
        if (StringUtils.isEmpty(crawlIgnoreFileName))
        {
            String msgStr = String.format("Connector property '%s' is undefined.",
                                          mCfgPropertyPrefix + "." + propertyName);
            throw new NSException(msgStr);
        }

        String crawlIgnorePathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                       File.separatorChar, crawlIgnoreFileName);
        load(crawlIgnorePathFileName);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
