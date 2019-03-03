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
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.IOUtils;
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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * The SolrSchema class provides a collection of methods that can create, update and delete
 * Solr schema objects.
 *
 * @see <a href="http://lucene.apache.org/solr/guide/7_6/schema-api.html">Solr Schema API</a>
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings("FieldCanBeLocal")
public class SolrSchema
{
    private final String CONTENT_TYPE_JSON = "application/json";

    private AppMgr mAppMgr;
    private SolrDS mSolrDS;
    private boolean mIsSolrDSOwnedByClass;

    /**
     * Constructor accepts an application manager parameter and initializes
     * the Solr Schema accordingly.
     *
     * @param anAppMgr Application manager.
     */
    public SolrSchema(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
    }

    /**
     * Constructor accepts an application manager parameter and initializes
     * the Solr Schema class accordingly.
     *
     * @param anAppMgr Application manager.
     * @param aSolrDS Solr data source instance.
     */
    public SolrSchema(AppMgr anAppMgr, SolrDS aSolrDS)
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

    private DataField createTextField(String aName, String aTitle, String aDescription,
                                      boolean anIsRequired, boolean anIsStored)
    {
        DataField dataField = new DataTextField(aName, aTitle);
        if (StringUtils.isNotEmpty(aDescription))
            dataField.addFeature(Field.FEATURE_DESCRIPTION, aDescription);
        if (anIsRequired)
            dataField.addFeature(Field.FEATURE_IS_REQUIRED, StrUtl.booleanToString(anIsRequired));
        dataField.addFeature(Field.FEATURE_IS_STORED, StrUtl.booleanToString(anIsStored));

        return dataField;
    }

    @SuppressWarnings("SameParameterValue")
    private DataField createBooleanField(String aName, String aTitle, String aDescription,
                                         boolean anIsRequired, Boolean aDefaultValue)
    {
        DataField dataField = new DataBooleanField(aName, aTitle);
        if (StringUtils.isNotEmpty(aDescription))
            dataField.addFeature(Field.FEATURE_DESCRIPTION, aDescription);
        if (aDefaultValue != null)
            dataField.setDefaultValue(aDefaultValue);
        if (anIsRequired)
            dataField.addFeature(Field.FEATURE_IS_REQUIRED, StrUtl.booleanToString(anIsRequired));
        dataField.enableFeature(Field.FEATURE_IS_STORED);

        return dataField;
    }

    public DataBag createFieldsBag()
    {
        DataBag schemaBag = new DataBag("Solr Schema Fields");

// http://lucene.apache.org/solr/guide/7_6/field-type-definitions-and-properties.html#field-type-definitions-and-properties

        schemaBag.add(createTextField(Solr.SCHEMA_OPERATION_FIELD_NAME, "Operation", "The field operation to perform in Solr.",
                                      false,false));

        DataField dataField = createTextField(Solr.SCHEMA_SEARCH_TYPE_FIELD_NAME, "Search Type", "Describes how the field will be used in a search (multi-value).",
                                              false,false);
        dataField.setRange("Query Field", "Facet Field", "Display Field", "Detail Field");
        dataField.setMultiValueFlag(true);
        schemaBag.add(dataField);
        schemaBag.add(createTextField("name", "Name", "The name of the field. Field names should consist of alphanumeric or underscore characters only and not start with a digit. This is not currently strictly enforced, but other field names will not have first class support from all components and back compatibility is not guaranteed. Names with both leading and trailing underscores (e.g., _version_) are reserved. Every field must have a name.",
                                      true,true));
        schemaBag.add(createTextField("type", "Type", "The name of the fieldType for this field. This will be found in the name attribute on the fieldType definition. Every field must have a type.",
                                      true, true));
        schemaBag.add(createBooleanField("required", "Is Required", "Instructs Solr to reject any attempts to add a document which does not have a value for this field. This property defaults to false.",
                                         false, false));
        schemaBag.add(createTextField("default", "Default", "A default value that will be added automatically to any document that does not have a value in this field when it is indexed. If this property is not specified, there is no default.",
                                      false, true));
        schemaBag.add(createBooleanField("indexed", "Is Indexed", "If true, the value of the field can be used in queries to retrieve matching documents.",
                                         false, true));
        schemaBag.add(createBooleanField("stored", "Is Stored", "If true, the actual value of the field can be retrieved by queries.",
                                         false, true));
        schemaBag.add(createBooleanField("docValues", "Is DocValues", "If true, the value of the field will be put in a column-oriented DocValues structure.",
                                         false, false));
        schemaBag.add(createBooleanField("useDocValuesAsStored", "Use Doc Values as Stored", "If the field has docValues enabled, setting this to true would allow the field to be returned as if it were a stored field (even if it has stored=false) when matching “*” in an fl parameter.",
                                         false, true));
        schemaBag.add(createBooleanField("sortMissingFirst ", "Sort Missing First", "Control the placement of documents when a sort field is not present.",
                                         false, false));
        schemaBag.add(createBooleanField("sortMissingLast", "Sort Missing Last", "Control the placement of documents when a sort field is not present.",
                                         false, false));
        schemaBag.add(createBooleanField("multiValued", "Is Multi-Valued", "If true, indicates that a single document might contain multiple values for this field type.",
                                         false, false));
        schemaBag.add(createBooleanField("uninvertible", "Is Uninvertible", "If true, indicates that an indexed='true' docValues='false' field can be 'un-inverted' at query time to build up large in memory data structure to serve in place of DocValues. Defaults to true for historical reasons, but users are strongly encouraged to set this to false for stability and use docValues='true' as needed.",
                                         false, true));
        schemaBag.add(createBooleanField("omitNorms", "Omit Normalization", "If true, omits the norms associated with this field (this disables length normalization for the field, and saves some memory). Defaults to true for all primitive (non-analyzed) field types, such as int, float, data, bool, and string. Only full-text fields or fields need norms.",
                                         false, null));
        schemaBag.add(createBooleanField("omitTermFreqAndPositions", "Omit Term Frequency and Positions", "If true, omits term frequency, positions, and payloads from postings for this field. This can be a performance boost for fields that don’t require that information. It also reduces the storage space required for the index. Queries that rely on position that are issued on a field with this option will silently fail to find documents. This property defaults to true for all field types that are not text fields.",
                                         false, null));
        schemaBag.add(createBooleanField("omitPositions", "Omit Positions", "Similar to omitTermFreqAndPositions but preserves term frequency information.",
                                         false, null));
        schemaBag.add(createBooleanField("termVectors", "Store Term Vectors", "These options instruct Solr to maintain full term vectors for each document, optionally including position, offset and payload information for each term occurrence in those vectors. These can be used to accelerate highlighting and other ancillary functionality, but impose a substantial cost in terms of index size. They are not necessary for typical uses of Solr.",
                                         false, false));
        schemaBag.add(createBooleanField("termPositions ", "Store Term Positions", "These options instruct Solr to maintain full term vectors for each document, optionally including position, offset and payload information for each term occurrence in those vectors. These can be used to accelerate highlighting and other ancillary functionality, but impose a substantial cost in terms of index size. They are not necessary for typical uses of Solr.",
                                         false, false));
        schemaBag.add(createBooleanField("termOffsets", "Store Term Offsets", "These options instruct Solr to maintain full term vectors for each document, optionally including position, offset and payload information for each term occurrence in those vectors. These can be used to accelerate highlighting and other ancillary functionality, but impose a substantial cost in terms of index size. They are not necessary for typical uses of Solr.",
                                         false, false));
        schemaBag.add(createBooleanField("termPayloads", "Store Term Payloads", "These options instruct Solr to maintain full term vectors for each document, optionally including position, offset and payload information for each term occurrence in those vectors. These can be used to accelerate highlighting and other ancillary functionality, but impose a substantial cost in terms of index size. They are not necessary for typical uses of Solr.",
                                         false, false));
        schemaBag.add(createBooleanField("large", "Is Large Field", "Large fields are always lazy loaded and will only take up space in the document cache if the actual value is < 512KB. This option requires stored='true' and multiValued='false'. It’s intended for fields that might have very large values so that they don’t get cached in memory.",
                                         false, false));

        return schemaBag;
    }

    public Document createFieldType(DataBag aBag)
    {
        return new Document(Solr.RESPONSE_SCHEMA_FIELD_TYPE, aBag);
    }

    public void addBagTextField(DataBag aBag, String aName, String aValue)
    {
        DataTextField dataTextField = new DataTextField(aName, Field.nameToTitle(aName), aValue);
        dataTextField.enableFeature(Field.FEATURE_IS_STORED);
        aBag.add(dataTextField);
    }

    public Document createFieldType(String aName, String aClassName)
    {
        DataBag fieldTypeBag = new DataBag(Solr.RESPONSE_SCHEMA_FIELD_TYPE);
        DataTextField operationField = new DataTextField(Solr.SCHEMA_OPERATION_FIELD_NAME, Field.nameToTitle(Solr.SCHEMA_OPERATION_FIELD_NAME));
        fieldTypeBag.add(operationField);
        addBagTextField(fieldTypeBag, Solr.SCHEMA_OPERATION_FIELD_NAME, Solr.SCHEMA_OPERATION_FT_ADD_FIELD);
        addBagTextField(fieldTypeBag, "name", aName);
        addBagTextField(fieldTypeBag, "class", aClassName);

        return createFieldType(fieldTypeBag);
    }

    /*  Document(Field Type)
        + Relationship(Analyzers)
          + Document(Analyzer) or Document(Index Analyzer) or Document(Query Analyzer)
            + Relationship(Tokenizer) or Relationship(Filters)
              + Document(bag of tokenizer or filter fields)
     */
    public Document addFieldTypeAnalyzer(Document aFieldType, String aType, String aTokenizerName)
    {
        Document analyzerDocument = new Document(aType);
        Document tokenizerDocument = new Document(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER);
        DataBag tokenizerBag = tokenizerDocument.getBag();
        tokenizerBag.setName("tokenizer");
        addBagTextField(tokenizerBag, "class", aTokenizerName);
        analyzerDocument.addRelationship(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER, tokenizerDocument);

        if (aFieldType.getRelationships().isEmpty())
            aFieldType.addRelationship(Solr.RESPONSE_SCHEMA_FT_ANALYZERS);
        Relationship analyzersRelationship = aFieldType.getFirstRelationship(Solr.RESPONSE_SCHEMA_FT_ANALYZERS);
        analyzersRelationship.add(analyzerDocument);

        return analyzerDocument;
    }

    public Document addFieldTypeAnalyzer(Document aFieldType, String aType)
    {
        Document analyzerDocument = new Document(aType);
        if (aFieldType.getRelationships().isEmpty())
            aFieldType.addRelationship(Solr.RESPONSE_SCHEMA_FT_ANALYZERS);
        Relationship analyzersRelationship = aFieldType.getFirstRelationship(Solr.RESPONSE_SCHEMA_FT_ANALYZERS);
        analyzersRelationship.add(analyzerDocument);

        return analyzerDocument;
    }

    public Document addFieldTypeAnalyzerTokenizer(Document anAnalyzer, DataBag aBag)
    {
        Relationship tokenizerRelationship = anAnalyzer.getFirstRelationship(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER);
        if (tokenizerRelationship == null)
        {
            anAnalyzer.addRelationship(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER);
            tokenizerRelationship = anAnalyzer.getFirstRelationship(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER);
        }
        Document tokenizerDocument = new Document(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER, aBag);
        tokenizerRelationship.add(tokenizerDocument);

        return tokenizerDocument;
    }

    public Document addFieldTypeAnalyzerFilter(Document anAnalyzer, DataBag aBag)
    {
        Relationship analyzersRelationship = anAnalyzer.getFirstRelationship(Solr.RESPONSE_SCHEMA_FTA_FILTERS);
        if (analyzersRelationship == null)
        {
            anAnalyzer.addRelationship(Solr.RESPONSE_SCHEMA_FTA_FILTERS);
            analyzersRelationship = anAnalyzer.getFirstRelationship(Solr.RESPONSE_SCHEMA_FTA_FILTERS);
        }
        Document filterDocument = new Document(Solr.RESPONSE_SCHEMA_FTA_FILTERS, aBag);
        analyzersRelationship.add(filterDocument);

        return filterDocument;
    }

    /**
     * Downloads the Solr schema file and parses the file into a Document.
     *
     * @param aDocumentName Path/Name where file should be stored.
     *
     * @return Document representing the Solr schema.
     *
     * @throws NSException Thrown when I/O errors are detected.
     */
    public Document downloadAndParse(String aDocumentName)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "downloadAndParse");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

// Construct our query URI string

        String baseSolrURL = mSolrDS.getBaseURL(true);
        String solrURI = baseSolrURL + "/schema";

        Document schemaDocument = null;
        if (StringUtils.isNotEmpty(solrURI))
        {
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
                    SolrSchemaJSON solrSchemaJSON = new SolrSchemaJSON(mAppMgr);
                    schemaDocument = solrSchemaJSON.load(inputStream);
                }
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s (%s): %s", solrURI, aDocumentName, e.getMessage());
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

        return schemaDocument;
    }

    /**
     * Downloads the Solr schema file and store it to the path/file name specified.
     *
     * @param aPathFileName Path/Name where file should be stored.
     *
     * @throws NSException Thrown when I/O errors are detected.
     */
    public void downloadAndSave(String aPathFileName)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "downloadAndSave");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (StringUtils.isNotEmpty(aPathFileName))
        {
            initialize();

// Construct our query URI string

            String baseSolrURL = mSolrDS.getBaseURL(true);
            String solrURI = baseSolrURL + "/schema";

            InputStream inputStream = null;
            OutputStream outputStream = null;
            CloseableHttpResponse httpResponse = null;
            File schemaFile = new File(aPathFileName);
            HttpGet httpGet = new HttpGet(solrURI);
            CloseableHttpClient httpClient = HttpClients.createDefault();

            try
            {
                httpResponse = httpClient.execute(httpGet);
                HttpEntity httpEntity = httpResponse.getEntity();
                inputStream = httpEntity.getContent();
                outputStream = new FileOutputStream(schemaFile);
                IOUtils.copy(inputStream, outputStream);
            }
            catch (IOException e)
            {
                String msgStr = String.format("%s (%s): %s", solrURI, aPathFileName, e.getMessage());
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

    /**
     * Updates the Solr schema configuration for the search cluster with a single batch
     * operation.  The following Solr schema table types are supported:
     * RESPONSE_SCHEMA_FIELD_NAMES, RESPONSE_SCHEMA_COPY_FIELDS and
     * RESPONSE_SCHEMA_DYNAMIC_FIELDS.  Please note that the operations will be
     * performed in the order of the rows provided.  If you wish to have add operations
     * performed first, then you must sort the table prior to calling this method.
     *
     * @see <a href="http://lucene.apache.org/solr/guide/7_6/schema-api.html">Solr Schema API</a>
     * @see <a href="https://codebeautify.org/jsonvalidator">JSON Validator</a>
     *
     * @param aTable A schema, copy field or dynamic fields table.
     *
     * @throws DSException Data source exception.
     */
    public void update(DataTable aTable)
        throws DSException
    {
        DataBag schemaBag;
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        int rowCount = aTable.rowCount();
        if (rowCount > 0)
        {
            initialize();

// Construct our query URI string

            String baseSolrURL = mSolrDS.getBaseURL(true);
            String solrURI = baseSolrURL + "/schema";

// First, we will generate our Solr Schema API payload.

            SolrSchemaJSON solrSchemaJSON = new SolrSchemaJSON(mAppMgr);
            StringBuilder stringBuilder = new StringBuilder(String.format("{%n"));
            for (int row = 0; row < rowCount; row++)
            {
                if (row > 0)
                    stringBuilder.append(String.format(",%n"));
                schemaBag = aTable.getRowAsBag(row);
                solrSchemaJSON.schemaFieldToJSON(stringBuilder, schemaBag);
            }
            stringBuilder.append(String.format("%n}%n"));
            String jsonPayload = stringBuilder.toString();

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
                String msgStr = String.format("%s (%s): %s", solrURI, aTable.getName(), e.getMessage());
                appLogger.error(msgStr, e);
                throw new DSException(msgStr);
            }
            finally
            {
                if (httpResponse != null)
                    IO.closeQuietly(httpResponse);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void update(StringBuilder aStringBuilder, Document aDocument)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "update");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String docType = aDocument.getType();
        if (!StringUtils.equals(docType, Solr.RESPONSE_SCHEMA_FIELD_TYPE))
            throw new DSException(String.format("Expecting '%s' and found '%s' - cannot update field type.",
                                                Solr.RESPONSE_SCHEMA_FIELD_TYPE, docType));

        initialize();

// Generate our Solr Schema API payload.

        SolrSchemaJSON solrSchemaJSON = new SolrSchemaJSON(mAppMgr);
        solrSchemaJSON.schemaFieldTypeToJSON(aStringBuilder, aDocument);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Updates the Solr schema configuration for the search cluster with a single batch
     * operation.  This method should be used to update the field types associated with
     * a schema.  You should ensure that this method is invoked prior to schema fields
     * referencing a new field type.
     *
     * Field type in a <i>Document</i> are modeled in a hierarchy consisting of a
     * common set of fields (name, class, etc.) and relationships representing
     * Analyzers, Index Analyzers and Query Analyzers.  Contained within each Analyzer
     * relationship document are tokenizers and filters (which are tables with columns
     * that will depend on the class selected).
     *
     * @see <a href="http://lucene.apache.org/solr/guide/7_6/schema-api.html">Solr Schema API</a>
     *
     * @param aDocument Document of type RESPONSE_SCHEMA_FIELD_TYPE
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
        String solrURI = baseSolrURL + "/schema";

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
     * Updates the Solr schema configuration for the search cluster with a single batch
     * operation.  This method should be used to update the field types associated with
     * a schema.  You should ensure that this method is invoked prior to schema fields
     * referencing a new field type.
     *
     * Field type in a <i>Document</i> are modeled in a hierarchy consisting of a
     * common set of fields (name, class, etc.) and relationships representing
     * Analyzers, Index Analyzers and Query Analyzers.  Contained within each Analyzer
     * relationship document are tokenizers and filters (which are tables with columns
     * that will depend on the class selected).
     *
     * @see <a href="http://lucene.apache.org/solr/guide/7_6/schema-api.html">Solr Schema API</a>
     *
     * @param aDocSolrFieldTypes List of Documents of type RESPONSE_SCHEMA_FIELD_TYPE
     *
     * @throws DSException Data source exception.
     */
    public void update(ArrayList<Document> aDocSolrFieldTypes)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "update");

        if (! aDocSolrFieldTypes.isEmpty())
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(String.format("{%n"));
            for (Document docSolrFieldType : aDocSolrFieldTypes)
                update(stringBuilder, docSolrFieldType);
            stringBuilder.append(String.format("%n}%n"));
            String jsonPayload = stringBuilder.toString();

// Construct our query URI string

            String baseSolrURL = mSolrDS.getBaseURL(true);
            String solrURI = baseSolrURL + "/schema";

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
                String msgStr = String.format("%s (Document List): %s", solrURI, e.getMessage());
                appLogger.error(msgStr, e);
                throw new DSException(msgStr);
            }
            finally
            {
                if (httpResponse != null)
                    IO.closeQuietly(httpResponse);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Releases any allocated resources for the Solr schema session.
     *
     * <p>
     * <b>Note:</b> This method should be invoked after all Solr schema
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
