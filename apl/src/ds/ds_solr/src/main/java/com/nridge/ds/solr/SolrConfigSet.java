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
import com.nridge.core.base.std.FilUtl;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * The SolrCollection class exposes methods that can manage the lifecycle of a Solr config set.
 * Unlike collections, the lifecycle is limited to create (with an upload), copy or delete.
 * The update is isolated to individual configuration files within a config set.
 *
 * @see <a href="http://lucene.apache.org/solr/guide/7_6/configsets-api.html">Solr Configsets API</a>
 * @see <a href="https://lucene.apache.org/solr/guide/6_6/using-zookeeper-to-manage-configuration-files.html">Zookeeper, Solr and Configsets</a>
 * @see <a href="https://lucene.apache.org/solr/7_6_0//solr-solrj/org/apache/solr/client/solrj/impl/CloudSolrClient.html#getClusterStateProvider--">SolrClient ClusterStateProvider</a>
 * @see <a href="https://lucene.apache.org/solr/6_6_0//solr-solrj/org/apache/solr/client/solrj/impl/ZkClientClusterStateProvider.html">Solr ZkClientClusterStateProvider</a>
 *
 * @author Al Cole
 * @since 1.0
 */
public class SolrConfigSet
{
	private AppMgr mAppMgr;
	private SolrDS mSolrDS;
	private boolean mIsSolrDSOwnedByClass;

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the Solr Configsets class accordingly.
	 *
	 * @param anAppMgr Application manager.
	 */
	public SolrConfigSet(AppMgr anAppMgr)
	{
		mAppMgr = anAppMgr;
	}

	/**
	 * Constructor accepts an application manager parameter and initializes
	 * the Solr Configsets class accordingly.
	 *
	 * @param anAppMgr Application manager.
	 * @param aSolrDS Solr data source instance.
	 */
	public SolrConfigSet(AppMgr anAppMgr, SolrDS aSolrDS)
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
			if (StringUtils.equals(jsonName, "configSets"))
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
	 * Generates a list of Solr config sets currently being managed by Zookeeper.
	 *
	 * @return List of Solr config sets.
	 *
	 * @throws DSException  Data source exception.
	 */
	public ArrayList<String> list()
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "list");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		initialize();

		ArrayList<String> configSetList = new ArrayList<>();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		String solrURI = baseSolrURL + "/admin/configs?action=LIST&omitHeader=true";

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

	/**
	 * Identifies if the config set name exists in the search cluster.
	 *
	 * @param aConfigSetName Configuration set name.
	 *
	 * @return <i>true</i> if it exists, <i>false</i> otherwise
	 *
	 * @throws DSException Data source exception
	 */
	public boolean exists(String aConfigSetName)
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "exists");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		if (StringUtils.isEmpty(aConfigSetName))
			throw new DSException("ConfigSet name must be specified.");

		ArrayList<String> solrConfigSetList = list();
		for (String cfgSetName : solrConfigSetList)
		{
			if (StringUtils.equals(cfgSetName, aConfigSetName))
				return true;
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

		return false;
	}

	/**
	 * Uploads the Solr config set files into Zookeeper.
	 *
	 * @see <a href="https://lucene.apache.org/solr/7_6_0//solr-solrj/org/apache/solr/client/solrj/impl/ZkClientClusterStateProvider.html">Solr ZkClientClusterStateProvider</a>
	 *
	 * @param aConfigSetName Config set name.
	 * @param aPathName Name of a path to upload the configuration files from.
	 *
	 * @throws DSException Solr Data Source exception.
	 * @throws IOException Zookeeper I/O exception.
	 */
	public void uploadFolderFiles(String aConfigSetName, String aPathName)
	throws DSException, IOException
	{
		Logger appLogger = mAppMgr.getLogger(this, "uploadFolderFiles");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		File pathFile = new File(aPathName);
		if (pathFile.exists())
		{
			initialize();

			Path folderPath = Paths.get(aPathName);
			ZkClientClusterStateProvider zkClientClusterStateProvider = mSolrDS.getZkClusterStateProvider();
			if (zkClientClusterStateProvider == null)
				throw new DSException("Unable to obtain the Zookeeper client cluster state instance.");
			zkClientClusterStateProvider.uploadConfig(folderPath, aConfigSetName);
		}
		else
			throw new DSException(String.format("%s: Does not exist!", aPathName));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Uploads the Solr config set ZIP file into the search cluster.
	 *
	 * @see <a href="http://lucene.apache.org/solr/guide/7_6/configsets-api.html">Solr ConfigSets API</a>
	 * @see <a href="https://www.baeldung.com/httpclient-post-http-request">Upload Binary File with HttpClient 4</a>
	 *
	 * @param aPathFileName Path file name of ZIP containing the configuration set.
	 * @param aConfigSetName Config set name.
	 *
	 * @throws DSException Solr Data Source exception.
	 */
	public void uploadZipFile(String aPathFileName, String aConfigSetName)
	throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "uploadZipFile");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		initialize();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		String solrURI = String.format("%s/admin/configs?action=UPLOAD&name=%s", baseSolrURL, aConfigSetName);

		File pathFile = new File(aPathFileName);
		CloseableHttpResponse httpResponse = null;
		HttpPost httpPost = new HttpPost(solrURI);
		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
		multipartEntityBuilder.addBinaryBody("file", pathFile, ContentType.APPLICATION_OCTET_STREAM, "file.zip");
		HttpEntity httpEntity = multipartEntityBuilder.build();
		httpPost.setEntity(httpEntity);
		CloseableHttpClient httpClient = HttpClients.createDefault();
		try
		{
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
				throw new DSException(msgStr);
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
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Downloads the Solr config set files from Zookeeper.
	 *
	 * @see <a href="https://lucene.apache.org/solr/6_6_0/solr-solrj/org/apache/solr/client/solrj/impl/ZkClientClusterStateProvider.html">Solr ZkClientClusterStateProvider</a>
	 *
	 * @param aConfigSetName Config set name.
	 * @param aPathName Name of a path to store the configuration files.
	 *
	 * @throws DSException Solr Data Source exception.
	 * @throws IOException Zookeeper I/O exception.
	 */
	public void download(String aConfigSetName, String aPathName)
		throws DSException, IOException
	{
		Logger appLogger = mAppMgr.getLogger(this, "download");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		File pathFile = new File(aPathName);
		if (pathFile.exists())
		{
			initialize();

			Path folderPath = Paths.get(aPathName);
			ZkClientClusterStateProvider zkClientClusterStateProvider = mSolrDS.getZkClusterStateProvider();
			if (zkClientClusterStateProvider == null)
				throw new DSException("Unable to obtain the Zookeeper client cluster state instance.");
			zkClientClusterStateProvider.downloadConfig(aConfigSetName, folderPath);
		}
		else
			throw new DSException(String.format("%s: Does not exist!", aPathName));

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Copies an existing Solr config set to create a new one.
	 *
	 * @param aExistingConfigSetName Existing config set name.
	 * @param aNewConfigSetName New config set name.
	 *
	 * @throws DSException Data source exception.
	 */
	public void copy(String aExistingConfigSetName, String aNewConfigSetName)
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "copy");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		initialize();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		String solrURI = String.format("%s/admin/configs?action=CREATE&name=%s&baseConfigSet=%s&omitHeader=true",
									   baseSolrURL, aNewConfigSetName, aExistingConfigSetName);

		CloseableHttpResponse httpResponse = null;
		HttpGet httpGet = new HttpGet(solrURI);
		CloseableHttpClient httpClient = HttpClients.createDefault();
		try
		{
			httpResponse = httpClient.execute(httpGet);
			StatusLine statusLine = httpResponse.getStatusLine();
			int statusCode = statusLine.getStatusCode();
			String msgStr = String.format("%s [%d]: %s", solrURI, statusCode, statusLine);
			appLogger.debug(msgStr);
			if (statusCode == HttpStatus.SC_OK)
			{
				HttpEntity httpEntity = httpResponse.getEntity();
				EntityUtils.consume(httpEntity);
			}
			else
			{
				msgStr = String.format("%s [%d]: %s", solrURI, statusCode, statusLine);
				appLogger.error(msgStr);
				throw new DSException(msgStr);
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
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}




	/**
	 * Deletes a Solr config set from Zookeeper.
	 *
	 * @param aConfigSetName Config set name.
	 *
	 * @throws DSException Data source exception.
	 */
	public void delete(String aConfigSetName)
		throws DSException
	{
		Logger appLogger = mAppMgr.getLogger(this, "delete");

		appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

		initialize();

		String baseSolrURL = mSolrDS.getBaseURL(false);
		String solrURI = String.format("%s/admin/configs?action=DELETE&name=%s&omitHeader=true", baseSolrURL, aConfigSetName);

		CloseableHttpResponse httpResponse = null;
		HttpGet httpGet = new HttpGet(solrURI);
		CloseableHttpClient httpClient = HttpClients.createDefault();
		try
		{
			httpResponse = httpClient.execute(httpGet);
			StatusLine statusLine = httpResponse.getStatusLine();
			int statusCode = statusLine.getStatusCode();
			String msgStr = String.format("%s [%d]: %s", solrURI, statusCode, statusLine);
			appLogger.debug(msgStr);
			if (statusCode == HttpStatus.SC_OK)
			{
				HttpEntity httpEntity = httpResponse.getEntity();
				EntityUtils.consume(httpEntity);
			}
			else
			{
				msgStr = String.format("%s [%d]: %s", solrURI, statusCode, statusLine);
				appLogger.error(msgStr);
				throw new DSException(msgStr);
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
			if (httpResponse != null)
				IO.closeQuietly(httpResponse);
		}

		appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
	}

	/**
	 * Releases any allocated resources for the Solr config set session.
	 *
	 * <p>
	 * <b>Note:</b> This method should be invoked after all Solr config set
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
