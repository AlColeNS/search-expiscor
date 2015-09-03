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

import com.nridge.connector.common.con_com.crawl.CrawlQueue;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.xml.DocumentXML;
import com.nridge.core.base.std.NSException;
import com.nridge.ds.content.ds_content.ContentExtractor;
import edu.uci.ics.crawler4j.parser.BinaryParseData;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.nridge.connector.common.con_com.Connector;
import com.nridge.connector.common.con_com.crawl.CrawlFollow;
import com.nridge.connector.common.con_com.crawl.CrawlIgnore;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.std.WebUtl;
import com.nridge.ds.content.ds_content.Content;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.commons.lang.time.StopWatch;
import org.jsoup.Jsoup;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.BlockingQueue;

/**
 * The SiteCrawler uses the Visitor design pattern to traverse an
 * HTML page hierarchy.  Please note that not all of the overridden
 * methods are used - they are kept here in case there is need in the
 * future for overriding them.
 * <p>
 * Multiple parsers are used in this class to accomplish different
 * goals.  The <i>crawl4j</i> package uses fetching logic based on
 * Apache Commons HTTPClient, Tika and an embedded database to do
 * its job.  The <i>jsoup</i> package parses HTML/CSS tags and
 * makes it easy to extract the structured nodes via a DOM interface.
 * The <i>HtmlUnit</i> package is a web page parser that supports
 * HTML/CSS/JavaScript and is based on a Firefox rendering engine.
 * </p>
 *
 * @see <a href="https://code.google.com/p/crawler4j/">Crawler4j</a>
 * @see <a href="http://htmlunit.sourceforge.net/">HtmlUnit</a>
 * @see <a href="http://jsoup.org/">jsoup: Java HTML Parser</a>
 * @see <a href="http://en.wikipedia.org/wiki/List_of_HTTP_header_fields">HTTP Request/Response Fields</a>
 * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/BlockingQueue.html">JavaDoc BlockingQueue</a>
 */
@SuppressWarnings("unchecked")
public class SiteCrawler extends WebCrawler
{
    /**
     * You should implement this function to specify whether
     * the given url should be crawled or not (based on your
     * crawling logic).
     *
     * @param aURL Candidate URL
     */
    @Override
    public boolean shouldVisit(WebURL aURL)
    {
        boolean shouldVisit = false;
        CrawlController crawlController = getMyController();
        AppMgr appMgr = (AppMgr) crawlController.getCustomData();
        Logger appLogger = appMgr.getLogger(this, "shouldVisit");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        String urlString = aURL.getURL().toLowerCase();
        CrawlFollow crawlFollow = (CrawlFollow) appMgr.getProperty(Constants.PROPERTY_CRAWL_FOLLOW);
        if (crawlFollow != null)
        {
            if (crawlFollow.isMatched(urlString))
                shouldVisit = true;
        }
        CrawlIgnore crawlIgnore = (CrawlIgnore) appMgr.getProperty(Constants.PROPERTY_CRAWL_IGNORE);
        if ((crawlIgnore != null) && (shouldVisit))
        {
            if (crawlIgnore.isMatched(urlString))
                shouldVisit = false;
        }

        appLogger.debug(String.format("Should Visit (%s): %s", shouldVisit, urlString));

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);

        return shouldVisit;
    }

    private void assignHTMLTags(AppMgr anAppMgr, Page aPage, Document aDocument)
    {
        Logger appLogger = anAppMgr.getLogger(this, "assignHTMLTags");

        appLogger.trace(anAppMgr.LOGMSG_TRACE_ENTER);

        if (aPage.getParseData() instanceof HtmlParseData)
        {
            DataBag wsBag = aDocument.getBag();

            HtmlParseData htmlParseData = (HtmlParseData) aPage.getParseData();
            String pageHTML = htmlParseData.getHtml();
            if (StringUtils.isNotEmpty(pageHTML))
            {
                org.jsoup.nodes.Document jSoupDocument = Jsoup.parse(pageHTML);
                String htmlTitle = jSoupDocument.title();
                if (StringUtils.isNotEmpty(htmlTitle))
                    wsBag.setValueByName("nsd_title", htmlTitle);
                wsBag.setValueByName("nsd_mime_type", Content.CONTENT_TYPE_HTML);
            }
        }

        appLogger.trace(anAppMgr.LOGMSG_TRACE_DEPART);
    }

    private Document createDocument(AppMgr anAppMgr, CrawlQueue aCrawlQueue, Page aPage)
    {
        String idValuePrefix;
        Logger appLogger = anAppMgr.getLogger(this, "createDocument");

        appLogger.trace(anAppMgr.LOGMSG_TRACE_ENTER);

        String propertyValue = anAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.id_value_prefix");
        if (StringUtils.isNotEmpty(propertyValue))
            idValuePrefix = propertyValue;
        else
            idValuePrefix = StringUtils.EMPTY;

        DataBag schemaBag = (DataBag) anAppMgr.getProperty(Connector.PROPERTY_SCHEMA_NAME);

        WebURL webURL = aPage.getWebURL();
        String urlString = webURL.getURL();
        String parentURLString = webURL.getParentUrl();
        byte[] contentData = aPage.getContentData();
        Header[] httpHeaders = aPage.getFetchResponseHeaders();

        String docId = Content.hashId(urlString);
        Document wsDocument = new Document(Constants.WS_DOCUMENT_TYPE, schemaBag);
        DataBag webPageBag = wsDocument.getBag();
        webPageBag.resetValuesWithDefaults();
        webPageBag.setValueByName("nsd_id", idValuePrefix + docId);
        String fileName = WebUtl.urlExtractFileName(urlString);
        webPageBag.setValueByName("nsd_name", fileName);
        webPageBag.setValueByName("nsd_url", urlString);
        webPageBag.setValueByName("nsd_url_view", urlString);
        webPageBag.setValueByName("nsd_url_parent", parentURLString);
        webPageBag.setValueByName("nsd_file_name", fileName);
        webPageBag.setValueByName("nsd_crawl_type", aCrawlQueue.getCrawlType());
        if ((httpHeaders != null) && (httpHeaders.length > 0))
        {
            for (Header httpHeader : httpHeaders)
            {
                if (httpHeader.getName().equals("Last-Modified"))
                {
                    Date lmDate = Field.createDate(httpHeader.getValue(), Field.FORMAT_RFC1123_DATE_TIME);
                    webPageBag.setValueByName("nsd_doc_modified_ts", lmDate);
                }
                else if (httpHeader.getName().equals("Content-Length"))
                    webPageBag.setValueByName("nsd_file_size", httpHeader.getValue());
            }
        }
        DataField dataField = webPageBag.getFieldByName("nsd_file_size");
        if ((dataField != null) && (! dataField.isAssigned()) && (contentData != null))
            webPageBag.setValueByName("nsd_file_size", contentData.length);

        webPageBag.setValueByName("nsd_doc_hash", wsDocument.generateUniqueHash(false));

        appLogger.trace(anAppMgr.LOGMSG_TRACE_DEPART);

        return wsDocument;
    }

/* The HtmlUnit framework cannot handle all types of files.  Specifically, it
will fail to extract content from the NS site if the files have PHP extensions.
The workaround logic will check to see if any content was extracted - if not,
then the crawl4j Tika-based content will be used. */

    private void fetchPageUsingBrowser(AppMgr anAppMgr, Page aPage, Document aDocument)
    {
        WebClient webClient;
        Logger appLogger = anAppMgr.getLogger(this, "fetchPageUsingBrowser");

        appLogger.trace(anAppMgr.LOGMSG_TRACE_ENTER);

        String urlString = aPage.getWebURL().getURL();
        String proxyHostName = anAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.proxy_host_name");
        if (StringUtils.isNotEmpty(proxyHostName))
        {
            String proxyPortNumber = anAppMgr.getString(Constants.CFG_PROPERTY_PREFIX + ".extract.proxy_port_number", "8080");
            webClient = new WebClient(BrowserVersion.FIREFOX_24, proxyHostName, Integer.parseInt(proxyPortNumber));
        }
        else
            webClient = new WebClient(BrowserVersion.FIREFOX_24);
        try
        {
            HtmlPage htmlPage = webClient.getPage(urlString);
            DataBag wsBag = aDocument.getBag();

            DataField dataField = wsBag.getFirstFieldByFeatureName(Field.FEATURE_IS_CONTENT);
            if (dataField != null)
            {
                String pageText = htmlPage.getTextContent();
                if ((StringUtils.isEmpty(pageText)) && (aPage.getParseData() instanceof HtmlParseData))
                {
                    HtmlParseData htmlParseData = (HtmlParseData) aPage.getParseData();
                    pageText = htmlParseData.getText();
                }
                if (StringUtils.isNotEmpty(pageText))
                    dataField.setValue(pageText);
            }
            String pageTitle = htmlPage.getTitleText();
            if ((StringUtils.isEmpty(pageTitle))  && (aPage.getParseData() instanceof HtmlParseData))
            {
                HtmlParseData htmlParseData = (HtmlParseData) aPage.getParseData();
                pageTitle = htmlParseData.getTitle();
            }
            if (StringUtils.isNotEmpty(pageTitle))
                wsBag.setValueByName("nsd_title", pageTitle);

            ContentExtractor contentExtractor = new ContentExtractor(anAppMgr);
            contentExtractor.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
            try
            {
                URL docURL = new URL(urlString);
                String mimeType = contentExtractor.detectType(docURL);
                if (StringUtils.isNotEmpty(mimeType))
                    wsBag.setValueByName("nsd_mime_type", mimeType);
            }
            catch (Exception e)
            {
                String msgStr = String.format("%s: %s", urlString, e.getMessage());
                appLogger.warn(msgStr);
            }
        }
        catch (IOException e)
        {
            String msgStr = String.format("%s: %s", urlString, e.getMessage());
            appLogger.warn(msgStr, e);
        }

        appLogger.trace(anAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void fetchDocument(AppMgr anAppMgr, Page aPage, Document aDocument)
    {
        Logger appLogger = anAppMgr.getLogger(this, "fetchDocument");

        appLogger.trace(anAppMgr.LOGMSG_TRACE_ENTER);

        String urlString = aPage.getWebURL().getURL();

        DataBag wsBag = aDocument.getBag();
        DataField dataField = wsBag.getFirstFieldByFeatureName(Field.FEATURE_IS_CONTENT);
        if (dataField != null)
        {
            ContentExtractor contentExtractor = new ContentExtractor(anAppMgr, wsBag);
            contentExtractor.setCfgPropertyPrefix(Constants.CFG_PROPERTY_PREFIX + ".extract");
            try
            {
                URL docURL = new URL(urlString);
                String mimeType = contentExtractor.detectType(docURL);
                if (StringUtils.isNotEmpty(mimeType))
                    wsBag.setValueByName("nsd_mime_type", mimeType);
                String docContent = contentExtractor.process(docURL);
                if (StringUtils.isNotEmpty(docContent))
                    dataField.setValue(docContent);
            }
            catch (Exception e)
            {
                String msgStr = String.format("%s: %s", urlString, e.getMessage());
                appLogger.warn(msgStr);
            }
        }

        appLogger.trace(anAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * This method is called when a page is fetched and ready
     * to be processed by your program.
     *
     * @param aPage Web page to visit.
     */
    @Override
    public void visit(Page aPage)
    {
        CrawlController crawlController = getMyController();
        AppMgr appMgr = (AppMgr) crawlController.getCustomData();

        Logger appLogger = appMgr.getLogger(this, "visit");

        appLogger.trace(appMgr.LOGMSG_TRACE_ENTER);

        String urlString = aPage.getWebURL().getURL();
        CrawlQueue crawlQueue = (CrawlQueue) appMgr.getProperty(Connector.PROPERTY_CRAWL_QUEUE);
        BlockingQueue extractQueue = (BlockingQueue) appMgr.getProperty(Connector.QUEUE_EXTRACT_NAME);
        boolean isCrawlJavaScript = appMgr.getBoolean(Constants.CFG_PROPERTY_PREFIX + ".extract.crawl_javascript", false);

        if ((crawlQueue == null) || (extractQueue == null))
            appLogger.error("Internal Error: Crawl/Extract queue is null.");
        else
        {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            Document wsDocument = createDocument(appMgr, crawlQueue, aPage);
            DataBag wsBag = wsDocument.getBag();
            if (aPage.getParseData() instanceof BinaryParseData)
                fetchDocument(appMgr, aPage, wsDocument);
            else if (aPage.getParseData() instanceof HtmlParseData)
            {
                HtmlParseData htmlParseData = (HtmlParseData) aPage.getParseData();

                if (isCrawlJavaScript)
                    fetchPageUsingBrowser(appMgr, aPage, wsDocument);
                else
                {
                    assignHTMLTags(appMgr, aPage, wsDocument);
                    DataField dataField = wsBag.getFirstFieldByFeatureName(Field.FEATURE_IS_CONTENT);
                    if (dataField != null)
                    {
                        String pageText = htmlParseData.getText();
                        if (StringUtils.isNotEmpty(pageText))
                            dataField.setValue(pageText);
                    }
                }
            }
            else
            {
                wsDocument = null;
                String msgStr = String.format("Unknown Parse Date Type '%s': %s", aPage.getParseData(), urlString);
                appLogger.error(msgStr);
            }

            if (wsDocument != null)
            {
                String docId = wsBag.getValueAsString("nsd_id");
                String queueBagPathFileName = crawlQueue.docPathFileName(Connector.QUEUE_EXTRACT_NAME, docId);
                DocumentXML documentXML = new DocumentXML(wsDocument);
                try
                {
                    documentXML.save(queueBagPathFileName);
                }
                catch (IOException e)
                {
                    wsDocument = null;
                    String msgStr = String.format("%s: %s", queueBagPathFileName, e.getMessage());
                    appLogger.error(msgStr);
                }

                stopWatch.stop();

                if (wsDocument != null)
                {
                    String queueItem = Connector.queueItemIdPhaseTime(docId, Connector.PHASE_EXTRACT,
                                                                      stopWatch.getTime());
                    try
                    {
                        // If queue is full, this thread may block.
                        extractQueue.put(queueItem);
                    }
                    catch (InterruptedException e)
                    {
                        // Restore the interrupted status so parent can handle (if it wants to).
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        appLogger.trace(appMgr.LOGMSG_TRACE_DEPART);
    }
}
