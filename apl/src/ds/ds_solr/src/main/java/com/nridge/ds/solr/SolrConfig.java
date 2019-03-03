/*
 * NorthRidge Software, LLC - Copyright (c) 2019.
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
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.NSException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SolrConfig
{
	private final String CONTENT_TYPE_JSON = "application/json";

	private AppMgr mAppMgr;
	private SolrDS mSolrDS;
	private boolean mIsSolrDSOwnedByClass;

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the Solr Config accordingly.
	 *
	 * @param anAppMgr Application manager.
	 */
	public SolrConfig(AppMgr anAppMgr)
	{
		mAppMgr = anAppMgr;
	}

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the Solr Config class accordingly.
	 *
	 * @param anAppMgr Application manager.
	 * @param aSolrDS Solr data source instance.
	 */
	public SolrConfig(AppMgr anAppMgr, SolrDS aSolrDS)
	{
		mAppMgr = anAppMgr;
		mSolrDS = aSolrDS;
	}

	private void initialize()
	{
		Logger appLogger = mAppMgr.getLogger(this, "initialize");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (mSolrDS == null)
		{
			mSolrDS = new SolrDS(mAppMgr);
			mIsSolrDSOwnedByClass = true;
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Downloads the Solr Config file identified via the URL parameter
	 * and creates a Document from its contents.  The Solr Dashboard
	 * exposes the URL to a schema file.
	 *
	 * @param aURL Uniform Resource Location of schema file.
	 * @param aDocumentName Name of the document to be created.
	 *
	 * @return Document representing the Solr configuration.
	 *
	 * @throws NSException Thrown when I/O errors are detected.
	 */
	public Document downloadAndParse(String aURL, String aDocumentName)
		throws NSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "downloadAndParse");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		Document solrConfigDocument = null;
		if (StringUtils.isNotEmpty(aURL))
		{
			InputStream inputStream = null;
			CloseableHttpResponse httpResponse = null;
			HttpGet httpGet = new HttpGet(aURL);
			CloseableHttpClient httpClient = HttpClients.createDefault();

			try
			{
				httpResponse = httpClient.execute(httpGet);
				HttpEntity httpEntity = httpResponse.getEntity();
				if (httpEntity != null)
				{
					inputStream = httpEntity.getContent();
					SolrConfigJSON solrConfigJSON = new SolrConfigJSON(mAppMgr);
					solrConfigDocument = solrConfigJSON.load(inputStream);
				}
			}
			catch (IOException e)
			{
				String msgStr = String.format("%s (%s): %s", aURL, aDocumentName, e.getMessage());
				appLogger.error(msgStr, e);
				throw new NSException(msgStr);
			}
			finally
			{
				if (inputStream != null)
					IO.closeQuietly(inputStream);
				if (httpResponse != null)
					IO.closeQuietly(httpResponse);
			}
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return solrConfigDocument;
	}

	private void update(StringBuilder aStringBuilder, Document aDocument)
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "update");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		initialize();

		SolrConfigJSON solrConfigJSON = new SolrConfigJSON(mAppMgr);

		String docType = aDocument.getType();
		if (StringUtils.equals(docType, Solr.RESPONSE_CONFIG_RH_SN))
			solrConfigJSON.convert(aStringBuilder, aDocument);
		else if (StringUtils.startsWith(docType, Solr.RESPONSE_CONFIG_SEARCH_COMPONENT))
			solrConfigJSON.convert(aStringBuilder, aDocument);
		else
			throw new DSException(String.format("Unknown document type '%s' - cannot update Solr configuration.", docType));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Updates the Solr configuration for the search cluster with a single batch
	 * operation.  This method should be used to update the request handler associated
	 * with a schema.  You should ensure that this method is invoked prior to schema fields
	 * referencing a new field type.
	 *
	 * Field type in a <i>Document</i> are modeled in a hierarchy consisting of a
	 * common set of fields (name, class, etc.) and relationships representing
	 * Solr component configurations.  Contained within each component relationship
	 * document are configuration parameters (which are tables with columns that
	 * will depend on the class selected).
	 *
	 * @see <a href="http://lucene.apache.org/solr/guide/7_6/config-api.html">Solr Config API</a>
	 *
	 * @param aDocument Document of type RESPONSE_CONFIG_REQUEST_HANDLER or
	 *                  RESPONSE_CONFIG_SEARCH_COMPONENT.
	 *
	 * @throws DSException Data source exception.
	 */
	public void update(Document aDocument)
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "update");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(String.format("{%n"));
		update(stringBuilder, aDocument);
		stringBuilder.append(String.format("%n}%n"));
		String jsonPayload = stringBuilder.toString();

// Construct our query URI string

		String baseSolrURL = mSolrDS.getBaseURL(true);
		String solrURI = baseSolrURL + "/config";

// Next, we will execute the HTTP POST request with the Solr schema API service.

		CloseableHttpResponse httpResponse = null;
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost(solrURI);
		httpPost.addHeader("Content-type", CONTENT_TYPE_JSON);

		try
		{
			HttpEntity httpEntity = new ByteArrayEntity(jsonPayload.getBytes(StandardCharsets.UTF_8));
			httpPost.setEntity(httpEntity);
			httpResponse = httpClient.execute(httpPost);
			StatusLine statusLine = httpResponse.getStatusLine();
			int statusCode = statusLine.getStatusCode();
			String msgStr = String.format("%s [%d]: %s", solrURI, statusCode, statusLine);
			appLogger.debug(msgStr);
			if (statusCode == HttpStatus.SC_OK)
			{
				httpEntity = httpResponse.getEntity();
				EntityUtils.consume(httpEntity);
			}
			else
			{
				msgStr = String.format("%s [%d]: %s", solrURI, statusCode, statusLine);
				appLogger.error(msgStr);
				appLogger.debug(jsonPayload);
				throw new DSException(msgStr);
			}
		}
		catch (IOException e)
		{
			String msgStr = String.format("%s (%s): %s", solrURI, aDocument.getName(), e.getMessage());
			appLogger.error(msgStr, e);
			throw new DSException(msgStr);
		}
		finally
		{
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Releases any allocated resources for the Solr config session.
	 *
	 * <p>
	 * <b>Note:</b> This method should be invoked after all Solr config
	 * operations are completed.
	 * </p>
	 */
	public void shutdown()
	{
		Logger appLogger = mAppMgr.getLogger(this, "shutdown");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if ((mIsSolrDSOwnedByClass) && (mSolrDS != null))
		{
			mSolrDS.shutdown();
			mSolrDS = null;
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

}
