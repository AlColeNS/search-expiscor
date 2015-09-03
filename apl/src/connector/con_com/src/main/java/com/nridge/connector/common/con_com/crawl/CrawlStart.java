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
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.std.NSException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

/**
 * The CrawlStart class is responsible for managing a list of
 * content URIs that a connector could crawl.
 *
 * @see <a href="http://hc.apache.org/">Apache HttpComponents</a>
 * @see <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html">HTTP Status Codes</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class CrawlStart
{
    private final int HTTP_STATUS_OK_START = 200;
    private final int HTTP_STATUS_OK_END = 206;
    private final int HTTP_STATUS_REDIRECTION_START = 300;
    private final int HTTP_STATUS_REDIRECTION_END = 304;

    private final AppMgr mAppMgr;
    private ArrayList<String> mStartList;
    private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the object accordingly.
     *
     * @param anAppMgr Application manager.
     */
    public CrawlStart(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mStartList = new ArrayList<String>();
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
     * Returns a list of of URIs to base a connector crawl process
     * on.
     *
     * @return List of URIs to crawl.
     */
    public ArrayList<String> getList()
    {
        return mStartList;
    }


    private CloseableHttpClient createHttpClient()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createHttpClient");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// http://hc.apache.org/httpcomponents-client-4.3.x/httpclient/examples/org/apache/http/examples/client/ClientCustomSSL.java
// http://stackoverflow.com/questions/19517538/ignoring-ssl-certificate-in-apache-httpclient-4-3

        CloseableHttpClient httpClient = null;
        SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
        try
        {

// Note: This logic will trust CA and self-signed certificates.

            sslContextBuilder.loadTrustMaterial(null, new TrustStrategy()
            {
                @Override
                public boolean isTrusted(X509Certificate[] aChain, String anAuthType)
                    throws CertificateException
                {
                    return true;
                }
            });
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build());
            httpClient = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
        }
        catch (Exception e)
        {
            String msgStr = String.format("HTTP Client Error: %s", e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return httpClient;
    }

    /**
     * Validates that there are files to crawl in the list and that each
     * file exists.  This method is not appropriate for web site URLs.
     *
     * @throws NSException On empty list or access failures.
     */
    public void validate()
        throws NSException
    {
        File startFile;
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mStartList.size() == 0)
            throw new NSException("The starting crawl list is empty.");

        for (String startName : mStartList)
        {
            if (StringUtils.startsWith(startName, "http"))
            {
                HttpGet httpGet = new HttpGet(startName);
                CloseableHttpResponse httpResponse = null;
                CloseableHttpClient httpClient = createHttpClient();
                try
                {
                    httpResponse = httpClient.execute(httpGet);
                    int statusCode = httpResponse.getStatusLine().getStatusCode();
                    if ((statusCode >= HTTP_STATUS_OK_START) && (statusCode <= HTTP_STATUS_OK_END) ||
                        (statusCode >= HTTP_STATUS_REDIRECTION_START) && (statusCode <= HTTP_STATUS_REDIRECTION_END))
                    {
                        HttpEntity httpEntity = httpResponse.getEntity();
                        EntityUtils.consume(httpEntity);
                    }
                    else
                    {
                        String msgStr = String.format("%s (status code %d): %s", startName, statusCode,
                                                      httpResponse.getStatusLine());
                        throw new NSException(msgStr);
                    }
                }
                catch (IOException e)
                {
                    String msgStr = String.format("%s: %s", startName, e.getMessage());
                    throw new NSException(msgStr);
                }
                finally
                {
                    if (httpResponse != null)
                        IOUtils.closeQuietly(httpResponse);
                }
            }
            else
            {
                startFile = new File(startName);
                if ((!startFile.exists()) || (!startFile.canRead()))
                    throw new NSException(String.format("%s: Cannot be accessed for crawl.", startName));
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Parses a file identified by the path/file name parameter
     * and loads it into an internally managed crawler start URI
     * list.
     *
     * @param aPathFileName Absolute file name (e.g. 'crawl_start.txt').
     *
     * @throws IOException I/O related exception.
     */
    public void load(String aPathFileName)
        throws IOException
    {
        List<String> lineList;
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        try (FileReader fileReader = new FileReader(aPathFileName))
        {
            lineList = IOUtils.readLines(fileReader);
        }

        for (String followString : lineList)
        {
            if (! StringUtils.startsWith(followString, "#"))
                mStartList.add(followString);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Parses a file identified by the <i>crawl_start_file</i>
     * configuration property and loads it into an internally
     * managed crawler start URI list.
     *
     * @throws IOException I/O related exception.
     * @throws NSException Missing configuration property.
     */
    public void load()
        throws IOException, NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = "crawl_start_file";
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
