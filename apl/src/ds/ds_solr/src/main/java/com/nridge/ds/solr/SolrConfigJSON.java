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
import com.google.gson.stream.JsonToken;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;

import java.io.*;
import java.util.ArrayList;


/**
 * The SolrSchemaJSON provides a collection of methods that can load
 * a JSON representation of a Solr schema.  This class supports the
 * latest Solr 7.5 API service calls.
 *
 * @see <a href="http://lucene.apache.org/solr/guide/7_6/config-api.html">Solr Config API</a>
 * @see <a href="http://lucene.apache.org/solr/guide/7_6/requesthandlers-and-searchcomponents-in-solrconfig.html#search-components">Solr Search Components</a>
 * @see <a href="https://lucene.apache.org/solr/guide/7_6/implicit-requesthandlers.html">Solr Request Handler ParamSets</a>
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings("FieldCanBeLocal")
public class SolrConfigJSON
{
    private final String OBJECT_RESPONSE_HEADER = "responseHeader";
    private final String OBJECT_RESPONSE_CONFIG = "config";
    private final String OBJECT_RESPONSE_CONFIG_QUERY = "query";
    private final String OBJECT_RESPONSE_CONFIG_QUERY_FC = "filterCache";
    private final String OBJECT_RESPONSE_CONFIG_QUERY_RC = "queryResultCache";
    private final String OBJECT_RESPONSE_CONFIG_QUERY_DC = "documentCache";
    private final String OBJECT_RESPONSE_CONFIG_QUERY_FVC = "fieldValueCache";
    private final String OBJECT_RESPONSE_CONFIG_UPDATE_HANDLER = "updateHandler";
    private final String OBJECT_RESPONSE_CONFIG_UH_IW = "indexWriter";
    private final String OBJECT_RESPONSE_CONFIG_UH_CW = "commitWithin";
    private final String OBJECT_RESPONSE_CONFIG_UH_AC = "autoCommit";
    private final String OBJECT_RESPONSE_CONFIG_UH_ASC = "autoSoftCommit";
    private final String OBJECT_RESPONSE_CONFIG_REQUEST_HANDLER = "requestHandler";
    private final String OBJECT_RESPONSE_CONFIG_SN_DEFAULTS = "defaults";
    private final String OBJECT_RESPONSE_CONFIG_SN_LC = "last-components";
    private final String OBJECT_RESPONSE_CONFIG_SN_IN = "invariants";
    private final String OBJECT_RESPONSE_CONFIG_SN_CO = "components";
    private final String OBJECT_RESPONSE_CONFIG_SN_UPDATE = "update";
    private final String OBJECT_RESPONSE_CONFIG_RESPONSE_WRITER = "queryResponseWriter";
    private final String OBJECT_RESPONSE_CONFIG_SEARCH_COMPONENT = "searchComponent";
    private final String OBJECT_RESPONSE_CONFIG_SC_SPELLCHECK = "spellcheck";
    private final String OBJECT_RESPONSE_CONFIG_SC_SC_SC = "spellchecker";
    private final String OBJECT_RESPONSE_CONFIG_SC_SUGGEST = "suggest";
    private final String OBJECT_RESPONSE_CONFIG_SC_SUGGESTER = "suggester";
    private final String OBJECT_RESPONSE_CONFIG_SC_TERM_VECTOR = "tvComponent";
    private final String OBJECT_RESPONSE_CONFIG_SC_TERMS = "terms";
    private final String OBJECT_RESPONSE_CONFIG_SC_ELEVATOR = "elevator";
    private final String OBJECT_RESPONSE_CONFIG_SC_HIGHLIGHT = "highlight";
    private final String OBJECT_RESPONSE_CONFIG_INIT_PARAMS = "initParams";
    private final String OBJECT_RESPONSE_CONFIG_LISTENER = "listener";
    private final String OBJECT_RESPONSE_CONFIG_DIRECTORY_FACTORY = "directoryFactory";
    private final String OBJECT_RESPONSE_CONFIG_CODEC_FACTORY = "codecFactory";
    private final String OBJECT_RESPONSE_CONFIG_UPDATE_HU_LOG = "updateHandlerupdateLog";
    private final String OBJECT_RESPONSE_CONFIG_REQUEST_DISPATCHER = "requestDispatcher";
    private final String OBJECT_RESPONSE_CONFIG_INDEX_CONFIG = "indexConfig";
    private final String OBJECT_RESPONSE_CONFIG_PEER_SYNC = "peerSync";

    private final AppMgr mAppMgr;

    public SolrConfigJSON(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
    }

    public SolrConfigJSON(final AppMgr anAppMgr, Document aDocument)
    {
        mAppMgr = anAppMgr;
    }

    private boolean isNextTokenAnObject(JsonReader aReader)
        throws IOException
    {
        JsonToken jsonToken = aReader.peek();

        return (jsonToken == JsonToken.BEGIN_OBJECT);
    }

    private boolean isNextTokenAnArray(JsonReader aReader)
        throws IOException
    {
        JsonToken jsonToken = aReader.peek();

        return (jsonToken == JsonToken.BEGIN_ARRAY);
    }

    private String nextValueAsString(JsonReader aReader)
        throws IOException
    {
        String valueString = StringUtils.EMPTY;

        JsonToken jsonToken = aReader.peek();
        switch (jsonToken)
        {
            case BOOLEAN:
                valueString = StrUtl.booleanToString(aReader.nextBoolean());
                break;
            case NUMBER:
            case STRING:
                valueString = Solr.cleanContent(aReader.nextString());
                break;
            default:
                aReader.skipValue();
                break;
        }

        return valueString;
    }

    private void addBagTextField(DataBag aBag, String aName, String aValue)
    {
        if ((aBag != null) && (StringUtils.isNotEmpty(aName)))
        {
            DataTextField dataTextField = new DataTextField(aName, Field.nameToTitle(aName), aValue);
            dataTextField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
            aBag.add(dataTextField);
        }
    }

    private void populateCfgUpdateHandler(JsonReader aReader, Document aCfgDocument, String aDocType)
        throws IOException
    {
        DataBag dataBag;
        String jsonName, jsonValue;
        Document iwDocument, cwDocument, acDocument, ascDocument;

        Document uhDocument = new Document(aDocType);
        DataBag uhBag = uhDocument.getBag();
        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_UH_IW))
            {
                iwDocument = new Document(Solr.RESPONSE_CONFIG_UH_INDEX_WRITER);
                iwDocument.setName(jsonName);
                dataBag = iwDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                uhDocument.addRelationship(Solr.RESPONSE_CONFIG_UH_INDEX_WRITER, iwDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_UH_CW))
            {
                cwDocument = new Document(Solr.RESPONSE_CONFIG_UH_COMMIT_WITHIN);
                cwDocument.setName(jsonName);
                dataBag = cwDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                uhDocument.addRelationship(Solr.RESPONSE_CONFIG_UH_COMMIT_WITHIN, cwDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_UH_AC))
            {
                acDocument = new Document(Solr.RESPONSE_CONFIG_UH_AUTO_COMMIT);
                acDocument.setName(jsonName);
                dataBag = acDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                uhDocument.addRelationship(Solr.RESPONSE_CONFIG_UH_AUTO_COMMIT, acDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_UH_ASC))
            {
                ascDocument = new Document(Solr.RESPONSE_CONFIG_UH_AUTO_SOFT_COMMIT);
                ascDocument.setName(jsonName);
                dataBag = ascDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                uhDocument.addRelationship(Solr.RESPONSE_CONFIG_UH_AUTO_SOFT_COMMIT, ascDocument);
            }
            else
            {
                jsonValue = nextValueAsString(aReader);
                addBagTextField(uhBag, jsonName, jsonValue);
            }
        }
        aReader.endObject();
        aCfgDocument.addRelationship(Solr.RESPONSE_CONFIG_UPDATE_HANDLER, uhDocument);
    }

    private void populateCfgQuery(JsonReader aReader, Document aCfgDocument, String aDocType)
        throws IOException
    {
        DataBag dataBag;
        String jsonName, jsonValue;
        Document fcDocument, rcDocument, dcDocument, fvcDocument;

        Document queryDocument = new Document(aDocType);
        DataBag queryBag = queryDocument.getBag();
        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_QUERY_FC))
            {
                fcDocument = new Document(Solr.RESPONSE_CONFIG_QUERY_FC);
                fcDocument.setName(jsonName);
                dataBag = fcDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                queryDocument.addRelationship(Solr.RESPONSE_CONFIG_QUERY_FC, fcDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_QUERY_RC))
            {
                rcDocument = new Document(Solr.RESPONSE_CONFIG_QUERY_RC);
                rcDocument.setName(jsonName);
                dataBag = rcDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                queryDocument.addRelationship(Solr.RESPONSE_CONFIG_QUERY_RC, rcDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_QUERY_DC))
            {
                dcDocument = new Document(Solr.RESPONSE_CONFIG_QUERY_DC);
                dcDocument.setName(jsonName);
                dataBag = dcDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                queryDocument.addRelationship(Solr.RESPONSE_CONFIG_QUERY_DC, dcDocument);
            }
            else if ((StringUtils.isEmpty(jsonName)) || // This handles a Solr 7.5 JSON output bug (empty string)
                    (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_QUERY_FVC)))
            {
                fvcDocument = new Document(Solr.RESPONSE_CONFIG_QUERY_FVC);
                fvcDocument.setName(jsonName);
                dataBag = fvcDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                queryDocument.addRelationship(Solr.RESPONSE_CONFIG_QUERY_FVC, fvcDocument);
            }
            else
            {
                jsonValue = nextValueAsString(aReader);
                addBagTextField(queryBag, jsonName, jsonValue);
            }
        }
        aReader.endObject();
        aCfgDocument.addRelationship(Solr.RESPONSE_CONFIG_QUERY, queryDocument);
    }

    private void addFieldToDocument(JsonReader aReader, Document aDocument, String aName)
        throws IOException
    {
        DataBag childBag;
        JsonToken jsonToken;
        boolean isArrayArray;
        Document childDocument;
        String jsonName, jsonValue;
        DataTextField dataTextField;

        DataBag dataBag = aDocument.getBag();
        if (isNextTokenAnArray(aReader))
        {
            aReader.beginArray();
            dataTextField = new DataTextField(aName, Field.nameToTitle(aName));
            dataTextField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
            jsonToken = aReader.peek();
            if (jsonToken == JsonToken.BEGIN_ARRAY)
            {
                isArrayArray = true;
                aReader.beginArray();
                jsonToken = aReader.peek();
            }
            else
                isArrayArray = false;
            while (jsonToken != JsonToken.END_ARRAY)
            {
                jsonValue = nextValueAsString(aReader);
                dataTextField.addValue(jsonValue);
                jsonToken = aReader.peek();
            }
            aReader.endArray();
            if (isArrayArray)
                aReader.endArray();
            dataBag.add(dataTextField);
        }
        else if (isNextTokenAnObject(aReader))
        {
            childDocument = new Document(aName);
            childDocument.setName(aName);
            childBag = childDocument.getBag();

            aReader.beginObject();
            jsonToken = aReader.peek();
            while (jsonToken != JsonToken.END_OBJECT)
            {
                jsonName = aReader.nextName();
                jsonValue = nextValueAsString(aReader);
                addBagTextField(childBag, jsonName, jsonValue);
                jsonToken = aReader.peek();
            }
            aReader.endObject();
            aDocument.addRelationship(aName, childDocument);
        }
        else
        {
            jsonValue = nextValueAsString(aReader);
            addBagTextField(dataBag, aName, jsonValue);
        }
    }

    private void addFieldToDocument(JsonReader aReader, Document aDocument)
        throws IOException
    {
        String jsonName;

        jsonName = aReader.nextName();
        addFieldToDocument(aReader, aDocument, jsonName);
    }

    private void populateCfgRequestHandler(JsonReader aReader, Document aCfgDocument, String aDocType)
        throws IOException
    {
        DataBag rhBag, snBag;
        String jsonName, jsonValue;
        Document snDocument, defDocument, coDocument, lcDocument, inDocument, upDocument;

        Document rhDocument = new Document(aDocType);
        rhBag = rhDocument.getBag();
        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.startsWith(jsonName, "/"))
            {
                snDocument = new Document(Solr.RESPONSE_CONFIG_RH_SN);
                snDocument.setName(jsonName);
                snBag = snDocument.getBag();
                DataTextField operationField = new DataTextField(Solr.CONFIG_OPERATION_FIELD_NAME, Field.nameToTitle(Solr.CONFIG_OPERATION_FIELD_NAME));
                operationField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_FALSE);
                snBag.add(operationField);
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_SN_DEFAULTS))
                    {
                        defDocument = new Document(Solr.RESPONSE_CONFIG_RH_SN_DEFAULTS);
                        defDocument.setName(jsonName);
                        aReader.beginObject();
                        while (aReader.hasNext())
                            addFieldToDocument(aReader, defDocument);
                        aReader.endObject();
                        snDocument.addRelationship(Solr.RESPONSE_CONFIG_RH_SN_DEFAULTS, defDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_SN_CO))
                    {
                        coDocument = new Document(Solr.RESPONSE_CONFIG_RH_SN_COMPONENTS);
                        coDocument.setName(jsonName);
                        addFieldToDocument(aReader, coDocument, jsonName);
                        snDocument.addRelationship(Solr.RESPONSE_CONFIG_RH_SN_COMPONENTS, coDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_SN_LC))
                    {
                        lcDocument = new Document(Solr.RESPONSE_CONFIG_RH_SN_LAST_COMPONENTS);
                        lcDocument.setName(jsonName);
                        addFieldToDocument(aReader, lcDocument, jsonName);
                        snDocument.addRelationship(Solr.RESPONSE_CONFIG_RH_SN_LAST_COMPONENTS, lcDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_SN_IN))
                    {
                        inDocument = new Document(Solr.RESPONSE_CONFIG_RH_SN_INVARIANTS);
                        inDocument.setName(jsonName);
                        aReader.beginObject();
                        while (aReader.hasNext())
                            addFieldToDocument(aReader, inDocument);
                        aReader.endObject();
                        snDocument.addRelationship(Solr.RESPONSE_CONFIG_RH_SN_INVARIANTS, inDocument);
                    }
                    else
                    {
                        jsonValue = nextValueAsString(aReader);
                        addBagTextField(snBag, jsonName, jsonValue);
                        snBag.add(new DataTextField(jsonName, Field.nameToTitle(jsonName), jsonValue));
                    }
                }
                aReader.endObject();
                rhDocument.addRelationship(Solr.RESPONSE_CONFIG_RH_SN, snDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_SN_UPDATE))
            {
                upDocument = new Document(Solr.RESPONSE_CONFIG_RH_SN_UPDATE);
                upDocument.setName(jsonName);
                aReader.beginObject();
                while (aReader.hasNext())
                    addFieldToDocument(aReader, upDocument);
                aReader.endObject();
                rhDocument.addRelationship(Solr.RESPONSE_CONFIG_RH_SN_UPDATE, upDocument);
            }
            else
            {
                jsonValue = nextValueAsString(aReader);
                addBagTextField(rhBag, jsonName, jsonValue);
            }
        }
        aReader.endObject();
        aCfgDocument.addRelationship(Solr.RESPONSE_CONFIG_REQUEST_HANDLER, rhDocument);
    }

    private void populateQueryResponseWriterHandler(JsonReader aReader, Document aCfgDocument, String aDocType)
        throws IOException
    {
        DataBag dataBag;
        Document rwDocument;
        String jsonName, jsonValue;

        Document qrwDocument = new Document(aDocType);
        DataBag qewBag = qrwDocument.getBag();
        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (isNextTokenAnObject(aReader))
            {
                rwDocument = new Document(Solr.RESPONSE_CONFIG_QRW_RESPONSE_WRITER);
                rwDocument.setName(jsonName);
                dataBag = rwDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(dataBag, jsonName, jsonValue);
                }
                aReader.endObject();
                qrwDocument.addRelationship(Solr.RESPONSE_CONFIG_QRW_RESPONSE_WRITER, rwDocument);
            }
            else
            {
                jsonValue = nextValueAsString(aReader);
                addBagTextField(qewBag, jsonName, jsonValue);
            }
        }
        aReader.endObject();
        aCfgDocument.addRelationship(Solr.RESPONSE_CONFIG_QUERY_RESPONSE_WRITER, qrwDocument);
    }

    private void addObjectToDocument(JsonReader aReader, Document aParentDocument, String aType, String aName)
        throws IOException
    {
        String jsonValue;
        JsonToken jsonToken;
        boolean isArrayArray;
        Document childDocument;

        DataBag dataBag = aParentDocument.getBag();
        if (isNextTokenAnArray(aReader))
        {
            aReader.beginArray();
            jsonToken = aReader.peek();
            if (jsonToken == JsonToken.BEGIN_ARRAY)
            {
                isArrayArray = true;
                aReader.beginArray();
                jsonToken = aReader.peek();
            }
            else
                isArrayArray = false;
            while (jsonToken != JsonToken.END_ARRAY)
            {
                childDocument = new Document(aType);
                childDocument.setName(aName);
                aReader.beginObject();
                while (aReader.hasNext())
                    addFieldToDocument(aReader, childDocument);
                aReader.endObject();
                aParentDocument.addRelationship(aType, childDocument);
                jsonToken = aReader.peek();
            }
            aReader.endArray();
            if (isArrayArray)
                aReader.endArray();
        }
        else if (isNextTokenAnObject(aReader))
        {
            childDocument = new Document(aType);
            childDocument.setName(aName);
            aReader.beginObject();
            while (aReader.hasNext())
                addFieldToDocument(aReader, childDocument);
            aReader.endObject();
            aParentDocument.addRelationship(aType, childDocument);
        }
        else
        {
            jsonValue = nextValueAsString(aReader);
            addBagTextField(dataBag, aName, jsonValue);
        }
    }

    private void assignSearchComponentType(Document aDocument)
    {
        DataBag dataBag = aDocument.getBag();
        String className = dataBag.getValueAsString("class");

        if (StringUtils.contains(className, "SpellCheck"))
            aDocument.setType(Solr.RESPONSE_CONFIG_SEARCH_COMPONENT + " - " + Solr.RESPONSE_CONFIG_SC_SPELLCHECK);
        else if (StringUtils.contains(className, "Suggest"))
            aDocument.setType(Solr.RESPONSE_CONFIG_SEARCH_COMPONENT + " - " + Solr.RESPONSE_CONFIG_SC_SUGGEST);
        else if (StringUtils.contains(className, "TermVector"))
            aDocument.setType(Solr.RESPONSE_CONFIG_SEARCH_COMPONENT + " - " + Solr.RESPONSE_CONFIG_SC_TERM_VECTOR);
        else if (StringUtils.contains(className, "TermsComponent"))
            aDocument.setType(Solr.RESPONSE_CONFIG_SEARCH_COMPONENT + " - " + Solr.RESPONSE_CONFIG_SC_TERMS);
        else if (StringUtils.contains(className, "QueryElevation"))
            aDocument.setType(Solr.RESPONSE_CONFIG_SEARCH_COMPONENT + " - " + Solr.RESPONSE_CONFIG_SC_ELEVATOR);
        else if (StringUtils.contains(className, "HighlightComponent"))
            aDocument.setType(Solr.RESPONSE_CONFIG_SEARCH_COMPONENT + " - " + Solr.RESPONSE_CONFIG_SC_HIGHLIGHT);
    }

    private void populateSearchComponent(JsonReader aReader, Document aCfgDocument, String aDocType)
        throws IOException
    {
        DataBag scBag;
        Document scDocument;
        JsonToken jsonToken;
        boolean isArrayArray;
        Document childDocument;
        String jsonName, jsonValue;

        Document searchComponentDocument = new Document(aDocType);
        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            scDocument = new Document(Solr.RESPONSE_CONFIG_SEARCH_COMPONENT);
            scDocument.setName(jsonName);
            scBag = scDocument.getBag();
            DataTextField operationField = new DataTextField(Solr.CONFIG_OPERATION_FIELD_NAME, Field.nameToTitle(Solr.CONFIG_OPERATION_FIELD_NAME));
            operationField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_FALSE);
            scBag.add(operationField);

            aReader.beginObject();
            while (aReader.hasNext())
            {
                jsonName = aReader.nextName();
                if (isNextTokenAnArray(aReader))
                {
                    aReader.beginArray();
                    jsonToken = aReader.peek();
                    if (jsonToken == JsonToken.BEGIN_ARRAY)
                    {
                        isArrayArray = true;
                        aReader.beginArray();
                        jsonToken = aReader.peek();
                    }
                    else
                        isArrayArray = false;
                    while (jsonToken != JsonToken.END_ARRAY)
                    {
                        childDocument = new Document(Field.nameToTitle(jsonName));
                        childDocument.setName(jsonName);
                        aReader.beginObject();
                        while (aReader.hasNext())
                            addFieldToDocument(aReader, childDocument);
                        aReader.endObject();
                        scDocument.addRelationship(Field.nameToTitle(jsonName), childDocument);
                        jsonToken = aReader.peek();
                    }
                    aReader.endArray();
                    if (isArrayArray)
                        aReader.endArray();
                }
                else if (isNextTokenAnObject(aReader))
                {
                    childDocument = new Document(Field.nameToTitle(jsonName));
                    childDocument.setName(jsonName);
                    aReader.beginObject();
                    while (aReader.hasNext())
                        addFieldToDocument(aReader, childDocument);
                    aReader.endObject();
                    scDocument.addRelationship(Field.nameToTitle(jsonName), childDocument);
                }
                else
                {
                    jsonValue = nextValueAsString(aReader);
                    addBagTextField(scBag, jsonName, jsonValue);
                }
            }
            aReader.endObject();
            assignSearchComponentType(scDocument);
            searchComponentDocument.addRelationship(scDocument.getType(), scDocument);
        }

        aCfgDocument.addRelationship(aDocType, searchComponentDocument);
    }

    /**
     * Parses an JSON string representing the Solr schema and loads it into a document
     * and returns an instance to it.
     *
     * @param aConfigString JSON string representation of the Solr schema.
     *
     * @return Document instance containing the parsed data.
     *
     * @throws IOException I/O related exception.
     */
    public Document loadDocument(String aConfigString)
        throws IOException
    {
        String jsonName, jsonValue;
        Document componentDocument;

        Document configDocument = new Document(Solr.DOCUMENT_SCHEMA_TYPE);
        DataBag configBag = configDocument.getBag();

        StringReader stringReader = new StringReader(aConfigString);
        JsonReader jsonReader = new JsonReader(stringReader);

        jsonReader.beginObject();
        while (jsonReader.hasNext())
        {
            jsonName = jsonReader.nextName();
            if (StringUtils.equals(jsonName, OBJECT_RESPONSE_HEADER))
            {
                Document headerDocument = new Document(Solr.RESPONSE_SCHEMA_HEADER);
                DataBag headerBag = headerDocument.getBag();
                jsonReader.beginObject();
                while (jsonReader.hasNext())
                {
                    jsonName = jsonReader.nextName();
                    jsonValue = nextValueAsString(jsonReader);
                    headerBag.add(new DataTextField(jsonName, Field.nameToTitle(jsonName), jsonValue));
                }
                jsonReader.endObject();
                configDocument.addRelationship(Solr.RESPONSE_SCHEMA_HEADER, headerDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG))
            {
                jsonReader.beginObject();
                while (jsonReader.hasNext())
                {
                    jsonName = jsonReader.nextName();
                    if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_UPDATE_HANDLER))
                        populateCfgUpdateHandler(jsonReader, configDocument, Solr.RESPONSE_CONFIG_UPDATE_HANDLER);
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_QUERY))
                        populateCfgQuery(jsonReader, configDocument, Solr.RESPONSE_CONFIG_QUERY);
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_REQUEST_HANDLER))
                        populateCfgRequestHandler(jsonReader, configDocument, Solr.RESPONSE_CONFIG_REQUEST_HANDLER);
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_RESPONSE_WRITER))
                        populateQueryResponseWriterHandler(jsonReader, configDocument, Solr.RESPONSE_CONFIG_QUERY_RESPONSE_WRITER);
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_SEARCH_COMPONENT))
                        populateSearchComponent(jsonReader, configDocument, Solr.RESPONSE_CONFIG_SEARCH_COMPONENT);
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_INIT_PARAMS))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_INIT_PARAMS);
                        addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_INIT_PARAMETERS, jsonName);
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_INIT_PARAMS, componentDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_LISTENER))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_LISTENER);
                        jsonReader.beginArray();
                        while (jsonReader.hasNext())
                            addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_LISTENER_EVENTS, jsonName);
                        jsonReader.endArray();
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_LISTENER, componentDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_DIRECTORY_FACTORY))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_DIRECTORY_FACTORY);
                        addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_DIRECTORY_ENTRIES, jsonName);
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_DIRECTORY_FACTORY, componentDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_CODEC_FACTORY))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_CODEC_FACTORY);
                        addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_CODEC_ENTRIES, jsonName);
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_CODEC_FACTORY, componentDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_UPDATE_HU_LOG))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_UPDATE_HANDLER_ULOG);
                        addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_UHUL_ENTRIES, jsonName);
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_UPDATE_HANDLER_ULOG, componentDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_REQUEST_DISPATCHER))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_REQUEST_DISPATCHER);
                        addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_RD_ENTRIES, jsonName);
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_REQUEST_DISPATCHER, componentDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_INDEX_CONFIG))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_REQUEST_DISPATCHER);
                        addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_INDEX_CONFIG, jsonName);
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_IC_ENTRIES, componentDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_RESPONSE_CONFIG_PEER_SYNC))
                    {
                        componentDocument = new Document(Solr.RESPONSE_CONFIG_PEER_SYNC);
                        addObjectToDocument(jsonReader, componentDocument, Solr.RESPONSE_CONFIG_PS_ENTRIES, jsonName);
                        configDocument.addRelationship(Solr.RESPONSE_CONFIG_PEER_SYNC, componentDocument);
                    }
                    else
                    {
                        jsonValue = nextValueAsString(jsonReader);
                        addBagTextField(configBag, jsonName, jsonValue);
                    }
                }
                jsonReader.endObject();
            }
            else
                jsonReader.skipValue();
        }
        jsonReader.endObject();
        jsonReader.close();

        return configDocument;
    }

    /**
     * Parses an input stream and loads it into an internally managed
     * document instance.
     *
     * @param anIS Input stream instance.
     *
     * @return Document capturing the Solr config.
     *
     * @throws IOException I/O related exception.
     */
    public Document load(InputStream anIS)
        throws IOException
    {
        String schemaString = IOUtils.toString(anIS, StrUtl.CHARSET_UTF_8);
        return loadDocument(schemaString);
    }

    /**
     * Parses a JSON formatted path/file name and loads it into an internally managed
     * document instance.
     *
     * @param aPathFileName Input path file name.
     *
     * @return Document capturing the Solr config.
     *
     * @throws IOException I/O related exception.
     */
    public Document load(String aPathFileName)
        throws IOException
    {
        File jsonFile = new File(aPathFileName);
        if (! jsonFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        Document solrConfigDocument = null;
        try (FileInputStream fileInputStream = new FileInputStream(jsonFile))
        {
            solrConfigDocument = load(fileInputStream);
        }

        return solrConfigDocument;
    }

    private void appendFieldValueJSON(StringBuilder aStringBuilder, DataField aField)
    {
        if (aField.isValueNotEmpty())
        {
            String fieldName = aField.getName();
            aStringBuilder.append(String.format("  \"%s\":", fieldName));
            if (aField.isMultiValue())
            {
                int appendCount = 0;
                aStringBuilder.append(String.format(" [%n"));
                for (String fieldValue : aField.getValues())
                {
                    if (appendCount > 0)
                        aStringBuilder.append(String.format(",%n"));
                    if ((Field.isNumber(aField.getType())) ||
                        ((StringUtils.contains(fieldName, ".facet.") && (NumberUtils.isNumber(fieldValue)))))
                        aStringBuilder.append(String.format("%s", fieldValue));
                    else if (Field.isBoolean(aField.getType()))
                        aStringBuilder.append(String.format("%s", StrUtl.stringToBoolean(fieldValue)));
                    else
                        aStringBuilder.append(String.format("\"%s\"", fieldValue));
                    appendCount++;
                }
                aStringBuilder.append(" ]");
            }
            else
            {
                if ((Field.isNumber(aField.getType())) || (NumberUtils.isNumber(aField.getValue())))
                    aStringBuilder.append(String.format("%s", aField.getValue()));
                else if (Field.isBoolean(aField.getType()))
                    aStringBuilder.append(String.format("%s", aField.isValueTrue()));
                else
                    aStringBuilder.append(String.format("\"%s\"", aField.getValue()));
            }
        }
    }

    // http://localhost:8983/solr/techproducts/config
    private void appendServiceJSON(StringBuilder aStringBuilder, Document aServiceDocument)
    {
        DataField dataField;
        int appendCount, fieldCount;

        DataBag serviceBag = aServiceDocument.getBag();
        String serviceType = aServiceDocument.getType();
        switch (serviceType)
        {
            case Solr.RESPONSE_CONFIG_RH_SN_UPDATE:
            case Solr.RESPONSE_CONFIG_RH_SN_DEFAULTS:
            case Solr.RESPONSE_CONFIG_RH_SN_INVARIANTS:
                aStringBuilder.append(String.format("  \"%s\": {", serviceType.toLowerCase()));
                appendCount = 0;
                fieldCount = serviceBag.count();
                for (int i = 0; i < fieldCount; i++)
                {
                    dataField = serviceBag.getByOffset(i);
                    if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
                    {
                        if (appendCount > 0)
                            aStringBuilder.append(String.format(",%n"));
                        appendFieldValueJSON(aStringBuilder, dataField);
                        appendCount++;
                    }
                }
                aStringBuilder.append(String.format(" }%n"));
                break;
            case Solr.RESPONSE_CONFIG_RH_SN_COMPONENTS:
            case Solr.RESPONSE_CONFIG_RH_SN_LAST_COMPONENTS:
                fieldCount = serviceBag.count();
                if (fieldCount > 0)
                {
                    dataField = serviceBag.getByOffset(0);
                    appendFieldValueJSON(aStringBuilder, dataField);
                }
                break;
            default:
                break;
        }
    }

    // http://localhost:8983/solr/techproducts/config
    private void appendComponentJSON(StringBuilder aStringBuilder, Document aComponentDocument)
    {
        DataField dataField;
        int appendCount, fieldCount;

        DataBag componentBag = aComponentDocument.getBag();
        aStringBuilder.append(" {");
        appendCount = 0;
        fieldCount = componentBag.count();
        for (int i = 0; i < fieldCount; i++)
        {
            dataField = componentBag.getByOffset(i);
            if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
            {
                if (appendCount > 0)
                    aStringBuilder.append(String.format(",%n"));
                appendFieldValueJSON(aStringBuilder, dataField);
                appendCount++;
            }
        }
        aStringBuilder.append(String.format(" }%n"));
    }

    private boolean isOpearationValid(String anOperation)
    {
        switch (anOperation)
        {
            case Solr.CONFIG_OPERATION_ADD_REQ_HANDLER:
            case Solr.CONFIG_OPERATION_UPD_REQ_HANDLER:
            case Solr.CONFIG_OPERATION_DEL_REQ_HANDLER:
            case Solr.CONFIG_OPERATION_ADD_SC_HANDLER:
            case Solr.CONFIG_OPERATION_UPD_SC_HANDLER:
            case Solr.CONFIG_OPERATION_DEL_SC_HANDLER:
                return true;
            default:
                return false;
        }
    }

    public void convert(StringBuilder aStringBuilder, Document aDocument)
        throws DSException
    {
        DataField dataField;
        int fieldCount, appendCount;
        Logger appLogger = mAppMgr.getLogger(this, "convert");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag solrConfigurationBag = aDocument.getBag();
        String configOperation = solrConfigurationBag.getValueAsString(Solr.CONFIG_OPERATION_FIELD_NAME);
        if (StringUtils.isEmpty(configOperation))
            throw new DSException("Undefined config field operation - cannot convert to JSON.");
        else if (! isOpearationValid(configOperation))
            throw new DSException(String.format("Config operation '%s' is invalid - cannot convert to JSON.", configOperation));
        else if (! solrConfigurationBag.isValid())
            throw new DSException("Data bag is not valid - cannot convert to JSON.");

        switch (configOperation)
        {
            case Solr.CONFIG_OPERATION_ADD_REQ_HANDLER:
            case Solr.CONFIG_OPERATION_UPD_REQ_HANDLER:
                aStringBuilder.append(String.format(" \"%s\": {%n", configOperation));
                appendCount = 0;
                fieldCount = solrConfigurationBag.count();
                for (int i = 0; i < fieldCount; i++)
                {
                    dataField = solrConfigurationBag.getByOffset(i);
                    if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
                    {
                        if (appendCount > 0)
                            aStringBuilder.append(String.format(",%n"));
                        appendFieldValueJSON(aStringBuilder, dataField);
                        appendCount++;
                    }
                }
                ArrayList<Document> serviceDocuments = aDocument.getRelatedDocuments();
                if ((serviceDocuments != null) && (serviceDocuments.size() > 0))
                {
                    for (Document serviceDocument : serviceDocuments)
                    {
                        if (appendCount > 0)
                            aStringBuilder.append(String.format(",%n"));
                        appendServiceJSON(aStringBuilder, serviceDocument);
                        appendCount++;
                    }
                }
                aStringBuilder.append("}");
                break;
            case Solr.CONFIG_OPERATION_DEL_REQ_HANDLER:
                dataField = solrConfigurationBag.getFieldByName("name");
                if (dataField != null)
                    aStringBuilder.append(String.format(" \"%s\": \"%s\"", configOperation, dataField.getValue()));
                break;
            case Solr.CONFIG_OPERATION_ADD_SC_HANDLER:
            case Solr.CONFIG_OPERATION_UPD_SC_HANDLER:
                aStringBuilder.append(String.format(" \"%s\": {%n", configOperation));
                appendCount = 0;
                fieldCount = solrConfigurationBag.count();
                for (int i = 0; i < fieldCount; i++)
                {
                    dataField = solrConfigurationBag.getByOffset(i);
                    if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
                    {
                        if (appendCount > 0)
                            aStringBuilder.append(String.format(",%n"));
                        appendFieldValueJSON(aStringBuilder, dataField);
                        appendCount++;
                    }
                }
                ArrayList<Document> componentDocuments = aDocument.getRelatedDocuments();
                if ((componentDocuments != null) && (componentDocuments.size() > 0))
                {
                    if (appendCount > 0)
                    {
                        aStringBuilder.append(String.format(",%n"));
                        appendCount = 0;
                    }
                    boolean isArray = componentDocuments.size() > 1;
                    for (Document componentDocument : componentDocuments)
                    {
                        if (appendCount == 0)
                        {
                            if (isArray)
                                aStringBuilder.append(String.format(" \"%s\": [%n", componentDocument.getBag().getName()));
                            else
                                aStringBuilder.append(String.format(" \"%s\": ", componentDocument.getBag().getName()));
                        }
                        else
                            aStringBuilder.append(String.format(",%n"));
                        appendComponentJSON(aStringBuilder, componentDocument);
                        appendCount++;
                    }
                    if (isArray)
                        aStringBuilder.append("]");
                }
                aStringBuilder.append("}");
                break;
            case Solr.CONFIG_OPERATION_DEL_SC_HANDLER:
                dataField = solrConfigurationBag.getFieldByName("name");
                if (dataField != null)
                    aStringBuilder.append(String.format(" \"%s\": \"%s\"", configOperation, dataField.getValue()));
                break;
            default:
                break;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
