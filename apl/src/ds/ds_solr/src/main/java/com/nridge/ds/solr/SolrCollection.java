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

import com.google.gson.stream.JsonReader;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;

/**
 * The SolrCollection class exposes methods that can manage the lifecycle of a Solr collection.
 *
 * @see <a href="http://lucene.apache.org/solr/guide/7_6/collections-api.html">Solr Collection API</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class SolrCollection
{
	private AppMgr mAppMgr;
	private SolrDS mSolrDS;
	private boolean mIsSolrDSOwnedByClass;

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the Solr Collection class accordingly.
	 *
	 * @param anAppMgr Application manager.
	 */
	public SolrCollection(AppMgr anAppMgr)
	{
		mAppMgr = anAppMgr;
	}

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the Solr Collection class accordingly.
	 *
	 * @param anAppMgr Application manager.
	 * @param aSolrDS Solr data source instance.
	 */
	public SolrCollection(AppMgr anAppMgr, SolrDS aSolrDS)
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

	private ArrayList<String> load(InputStream anIS)
		throws IOException
	{
		String jsonName, jsonValue;

		ArrayList<String> configSetList = new ArrayList<>();
		String configSetString = IOUtils.toString(anIS, StrUtl.CHARSET_UTF_8);
		StringReader stringReader = new StringReader(configSetString);
		JsonReader jsonReader = new JsonReader(stringReader);
		jsonReader.beginObject();
		while (jsonReader.hasNext())
		{
			jsonName = jsonReader.nextName();
			if (StringUtils.equals(jsonName, "collections"))
			{
				jsonReader.beginArray();
				while (jsonReader.hasNext())
				{
					jsonValue = jsonReader.nextString();
					configSetList.add(jsonValue);
				}
				jsonReader.endArray();
			}
		}
		jsonReader.endObject();

		return configSetList;
	}

	/**
	 * Generates a list of Solr collections currently being managed in the search cluster.
	 *
	 * @return List of Solr collection names.
	 *
	 * @throws DSException Data source exception.
	 */
	public ArrayList<String> list()
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "list");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		initialize();

		ArrayList<String> configSetList = new ArrayList<>();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		String solrURI = baseSolrURL + "/admin/collections?action=LIST&omitHeader=true";

		InputStream inputStream = null;
		CloseableHttpResponse httpResponse = null;
		HttpGet httpGet = new HttpGet(solrURI);
		CloseableHttpClient httpClient = HttpClients.createDefault();

		try
		{
			httpResponse = httpClient.execute(httpGet);
			HttpEntity httpEntity = httpResponse.getEntity();
			if (httpEntity != null)
			{
				inputStream = httpEntity.getContent();
				configSetList = load(inputStream);
			}
		}
		catch (IOException e)
		{
			String msgStr = String.format("%s: %s", solrURI, e.getMessage());
			appLogger.error(msgStr, e);
			throw new DSException(msgStr);
		}
		finally
		{
			if (inputStream != null)
				IO.closeQuietly(inputStream);
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return configSetList;
	}

	private void parseReply(String aMessage)
		throws DSException, IOException
	{
		String jsonName;

		Logger appLogger = mAppMgr.getLogger(this, "parseReply");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		int msgCode = 0;
		String msgString = StringUtils.EMPTY;
		StringReader stringReader = new StringReader(aMessage);
		JsonReader jsonReader = new JsonReader(stringReader);
		jsonReader.beginObject();
		while (jsonReader.hasNext())
		{
			jsonName = jsonReader.nextName();
			if (StringUtils.equals(jsonName, "exception"))
			{
				jsonReader.beginObject();
				while (jsonReader.hasNext())
				{
					jsonName = jsonReader.nextName();
					if (StringUtils.equals(jsonName, "msg"))
						msgString = jsonReader.nextString();
					else if (StringUtils.equals(jsonName, "rspCode"))
						msgCode = jsonReader.nextInt();
					else
						jsonReader.skipValue();
				}
				jsonReader.endObject();
			}
			else
				jsonReader.skipValue();
		}
		jsonReader.endObject();

		if (StringUtils.isNotEmpty(msgString))
			throw new DSException(String.format("Solr Exception [%d]: %s", msgCode, msgString));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	private void parseReply(InputStream anIS)
		throws DSException, IOException
	{
		Logger appLogger = mAppMgr.getLogger(this, "parseReply");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		String replyMessage = IOUtils.toString(anIS, StrUtl.CHARSET_UTF_8);
		parseReply(replyMessage);

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Identifies if the collection name exists in the search cluster.
	 *
	 * @param aCollectionName Collection name.
	 *
	 * @return <i>true</i> if it exists, <i>false</i> otherwise
	 *
	 * @throws DSException Data source exception
	 */
	public boolean exists(String aCollectionName)
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "exists");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aCollectionName))
			throw new DSException("Collection name must be specified.");

		ArrayList<String> solrCollectionList = list();
		for (String collectionName : solrCollectionList)
		{
			if (StringUtils.equals(collectionName, aCollectionName))
				return true;
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return false;
	}

	/**
	 * Create a Solr collection.
	 *
	 * @param aConfigSetName ConfigSet name.
	 * @param aCollectionName Collection name.
	 * @param aNumberOfShards Number of shards (spreads documents across the shards)
	 * @param aReplicationFactor Replication factor (2 or more helps high availability)
	 *
	 * @throws DSException Data source exception
	 */
	public void create(String aConfigSetName, String aCollectionName, int aNumberOfShards, int aReplicationFactor)
		throws DSException
	{
		String solrURI;
		Logger appLogger = mAppMgr.getLogger(this, "create");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aConfigSetName))
			throw new DSException("Configuration set name must be specified.");
		else if (StringUtils.isEmpty(aCollectionName))
			throw new DSException("Collection name must be specified.");
		aNumberOfShards = Math.max(aNumberOfShards, 1);
		aReplicationFactor = Math.max(aReplicationFactor, 1);

		initialize();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		solrURI = String.format("%s/admin/collections?action=CREATE&name=%s&collection.configName=%s&numShards=%d&replicationFactor=%d&wt=json",
								baseSolrURL, aCollectionName, aConfigSetName, aNumberOfShards, aReplicationFactor);

		InputStream inputStream = null;
		CloseableHttpResponse httpResponse = null;
		HttpGet httpGet = new HttpGet(solrURI);
		CloseableHttpClient httpClient = HttpClients.createDefault();

		try
		{
			httpResponse = httpClient.execute(httpGet);
			HttpEntity httpEntity = httpResponse.getEntity();
			if (httpEntity != null)
			{
				inputStream = httpEntity.getContent();
				parseReply(inputStream);
			}
		}
		catch (IOException e)
		{
			String msgStr = String.format("%s: %s", solrURI, e.getMessage());
			appLogger.error(msgStr, e);
			throw new DSException(msgStr);
		}
		finally
		{
			if (inputStream != null)
				IO.closeQuietly(inputStream);
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Reloads the Solr collection.  This is typically used when a ConfigSet was externally
	 * updated and the cluster nodes need to update their configuration snapshots.
	 *
	 * @param aCollectionName Collection name.
	 *
	 * @throws DSException Data source exception.
	 */
	public void reload(String aCollectionName)
		throws DSException
	{
		String solrURI;
		Logger appLogger = mAppMgr.getLogger(this, "reload");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aCollectionName))
			throw new DSException("Collection name must be specified.");

		initialize();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		solrURI = String.format("%s/admin/collections?action=RELOAD&name=%s&wt=json", baseSolrURL, aCollectionName);

		InputStream inputStream = null;
		CloseableHttpResponse httpResponse = null;
		HttpGet httpGet = new HttpGet(solrURI);
		CloseableHttpClient httpClient = HttpClients.createDefault();

		try
		{
			httpResponse = httpClient.execute(httpGet);
			HttpEntity httpEntity = httpResponse.getEntity();
			if (httpEntity != null)
			{
				inputStream = httpEntity.getContent();
				parseReply(inputStream);
			}
		}
		catch (IOException e)
		{
			String msgStr = String.format("%s: %s", solrURI, e.getMessage());
			appLogger.error(msgStr, e);
			throw new DSException(msgStr);
		}
		finally
		{
			if (inputStream != null)
				IO.closeQuietly(inputStream);
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Deletes a Solr collection from the search cluster.
	 *
	 * @param aCollectionName Collection name.
	 *
	 * @throws DSException Data source exception.
	 */
	public void delete(String aCollectionName)
		throws DSException
	{
		String solrURI;
		Logger appLogger = mAppMgr.getLogger(this, "delete");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aCollectionName))
			throw new DSException("Collection name must be specified.");

		initialize();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		solrURI = String.format("%s/admin/collections?action=DELETE&name=%s&wt=json", baseSolrURL, aCollectionName);

		InputStream inputStream = null;
		CloseableHttpResponse httpResponse = null;
		HttpGet httpGet = new HttpGet(solrURI);
		CloseableHttpClient httpClient = HttpClients.createDefault();

		try
		{
			httpResponse = httpClient.execute(httpGet);
			HttpEntity httpEntity = httpResponse.getEntity();
			if (httpEntity != null)
			{
				inputStream = httpEntity.getContent();
				parseReply(inputStream);
			}
		}
		catch (IOException e)
		{
			String msgStr = String.format("%s: %s", solrURI, e.getMessage());
			appLogger.error(msgStr, e);
			throw new DSException(msgStr);
		}
		finally
		{
			if (inputStream != null)
				IO.closeQuietly(inputStream);
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Releases any allocated resources for the Solr collection session.
	 *
	 * <p>
	 * <b>Note:</b> This method should be invoked after all Solr collection
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
