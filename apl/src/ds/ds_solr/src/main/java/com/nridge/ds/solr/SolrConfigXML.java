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

package com.nridge.ds.solr;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.io.xml.IOXML;
import com.nridge.core.base.std.NSException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;

import java.io.*;

/**
 * The SolrConfigXML provides a collection of methods that can generate
 * an XML representation of a Solr "solrconfig.xml" file.  Specifically,
 * this class will focus on the request handler section of the file.
 * The developer should use this output as a reference copy and update
 * each entry manually as needed for the specific project.
 *
 * @author Al Cole
 * @since 1.0
 */
public class SolrConfigXML
{
    private Document mDocument;
    private final AppMgr mAppMgr;

    public SolrConfigXML(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mDocument = new Document("Solr Schema");
    }

    public SolrConfigXML(AppMgr anAppMgr, Document aDocument)
    {
        mAppMgr = anAppMgr;
        mDocument = aDocument;
    }

    public Document getDocument()
    {
        return mDocument;
    }

    public void setDocument(Document aDocument)
    {
        mDocument = aDocument;
    }

// http://wiki.apache.org/solr/SolrConfigXml

    public void save(PrintWriter aPW, int anIndentAmount)
    {
        String fieldName;
        DataField dataField;

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<!-- Start Request Handler for Expiscor -->%n");

        anIndentAmount++;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<requestHandler name=\"/nsd\" class=\"solr.SearchHandler\">%n");
        anIndentAmount++;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<lst name=\"defaults\">%n");
        anIndentAmount++;

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<!-- General Settings -->%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"echoParams\">explicit</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"wt\">xml</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"title\">NS Discover</str>%n");

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<!-- Query Settings -->%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"defType\">edismax</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"df\">text</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"rows\">10</str>%n");

        DataBag dataBag = mDocument.getBag();
        if (dataBag.featureNameCount(Solr.FEATURE_IS_RESULT) == 0)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<!-- <str name=\"fl\">");
            for (int i = 0; i < dataBag.count(); i++)
            {
                dataField = dataBag.getByOffset(i);

                fieldName = dataField.getName();
                if (i == 0)
                    aPW.printf("%s", fieldName);
                else
                    aPW.printf(",%s", fieldName);
            }
            aPW.printf("</str> -->%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"fl\">*,score</str>%n");
        }
        else
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"fl\">");
            for (int i = 0; i < dataBag.count(); i++)
            {
                dataField = dataBag.getByOffset(i);
                if (dataField.isFeatureTrue(Solr.FEATURE_IS_RESULT))
                {
                    fieldName = dataField.getName();
                    if (i == 0)
                        aPW.printf("%s", fieldName);
                    else
                        aPW.printf(",%s", fieldName);
                }
            }

            aPW.printf(",score</str>%n");
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"fl\">*,score</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"q.alt\">*:*</str>%n");
        if (dataBag.featureNameCount(Solr.FEATURE_FIELD_BOOST) == 0)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<!-- <str name=\"qf\">");
            for (int i = 0; i < dataBag.count(); i++)
            {
                dataField = dataBag.getByOffset(i);

                fieldName = dataField.getName();
                if (i == 0)
                    aPW.printf("%s^1.0", fieldName);
                else
                    aPW.printf(" %s^1.0", fieldName);
            }
            aPW.printf("</str> -->%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"qf\">all</str>%n");
        }
        else
        {
            String boostValue;
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"qf\">");
            for (int i = 0; i < dataBag.count(); i++)
            {
                dataField = dataBag.getByOffset(i);
                boostValue = dataBag.getFeature(Solr.FEATURE_FIELD_BOOST);
                if (StringUtils.isNotEmpty(boostValue))
                {
                    fieldName = dataField.getName();
                    if (i == 0)
                        aPW.printf("%s^%s", fieldName, boostValue);
                    else
                        aPW.printf(" %s^%s", fieldName, boostValue);
                }
            }
            aPW.printf("</str>%n");
        }

        if (dataBag.featureNameCount(Solr.FEATURE_IS_FACET) > 0)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<!-- Facet Settings -->%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"facet\">on</str>%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"facet.mincount\">2</str>%n");

            for (int i = 0; i < dataBag.count(); i++)
            {
                dataField = dataBag.getByOffset(i);

                if ((dataField.isFeatureTrue(Solr.FEATURE_IS_FACET)) &&
                    (dataField.isFeatureFalse(Field.FEATURE_IS_PRIMARY_KEY)))
                {
                    IOXML.indentLine(aPW, anIndentAmount);
                    aPW.printf("<str name=\"facet.field\">%s</str>%n", dataField.getName());
                }
            }
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<!-- Highlighting Defaults -->%n");
        if (dataBag.featureNameCount(Solr.FEATURE_IS_HIGHLIGHTED) > 0)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"hl\">on</str>%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"hl.fl\">");
            for (int i = 0; i < dataBag.count(); i++)
            {
                dataField = dataBag.getByOffset(i);

                fieldName = dataField.getName();
                if (i == 0)
                    aPW.printf("%s", fieldName);
                else
                    aPW.printf(" %s", fieldName);
            }
            aPW.printf("</str>%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"hl.encoder\">html</str>%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"hl.simple.pre\">&lt;b&gt;</str>%n");
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"hl.simple.post\">&lt;/b&gt;</str>%n");
            IOXML.indentLine(aPW, anIndentAmount);
        }
        else
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<str name=\"hl\">off</str>%n");
        }

        aPW.printf("<!-- Spell Checking Defaults -->%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck\">on</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.extendedResults\">false</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.count\">5</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.alternativeTermCount\">2</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.maxResultsForSuggest\">5</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.collate\">true</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.collateExtendedResults\">true</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.maxCollationTries\">5</str>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str name=\"spellcheck.maxCollations\">3</str>%n");

        anIndentAmount--;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</lst>%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<!-- Append Spell Checking to Our List of Components -->%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<arr name=\"last-components\">%n");
        anIndentAmount++;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<str>spellcheck</str>%n");
        anIndentAmount--;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</arr>%n");
        anIndentAmount--;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</requestHandler>%n");
    }

    public void save(PrintWriter aPW)
        throws IOException
    {
        save(aPW, 2);
    }

    public void save(String aPathFileName)
        throws IOException
    {
        PrintWriter printWriter = new PrintWriter(aPathFileName, "UTF-8");
        save(printWriter);
        printWriter.close();
    }

    /**
     * Downloads the Solr config file identified via the URL parameter
     * and store it to the path/file name specified.  The Solr Dashboard
     * exposes the URL to a schema file.
     *
     * @param aURL Uniform Resource Location of schema file.
     * @param aPathFileName Path/Name where file should be stored.
     *
     * @throws com.nridge.core.base.std.NSException Thrown when I/O errors are detected.
     */
    public void downloadAndSave(String aURL, String aPathFileName)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "downloadAndSave");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((StringUtils.isNotEmpty(aURL))  && (StringUtils.isNotEmpty(aPathFileName)))
        {
            InputStream inputStream = null;
            OutputStream outputStream = null;
            CloseableHttpResponse httpResponse = null;
            File solrConfigFile = new File(aPathFileName);
            HttpGet httpGet = new HttpGet(aURL);
            CloseableHttpClient httpClient = HttpClients.createDefault();

            try
            {
                httpResponse = httpClient.execute(httpGet);
                HttpEntity httpEntity = httpResponse.getEntity();
                inputStream = httpEntity.getContent();
                outputStream = new FileOutputStream(solrConfigFile);
                IOUtils.copy(inputStream, outputStream);
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s (%s): %s", aURL, aPathFileName, e.getMessage());
                appLogger.error(msgStr, e);
                throw new NSException(msgStr);
            }
            finally
            {
                if (inputStream != null)
                    IO.closeQuietly(inputStream);
                if (outputStream != null)
                    IO.closeQuietly(outputStream);
                if (httpResponse != null)
                    IO.closeQuietly(httpResponse);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
