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
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import java.io.*;
import java.util.ArrayList;


/**
 * The SolrSchemaJSON class provides a collection of methods that can load
 * a JSON representation of a Solr schema.  This class supports the latest
 * Solr 7.6 API service calls.
 *
 * @see <a href="http://lucene.apache.org/solr/guide/7_6/schema-api.html">Solr Schema API</a>
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings("FieldCanBeLocal")
public class SolrSchemaJSON
{
    private final String OBJECT_SCHEMA = "schema";
    private final String OBJECT_SCHEMA_FIELDS = "fields";
    private final String OBJECT_SCHEMA_FIELD_TYPES = "fieldTypes";
    private final String OBJECT_SCHEMA_COPY_FIELDS = "copyFields";
    private final String OBJECT_RESPONSE_HEADER = "responseHeader";
    private final String OBJECT_SCHEMA_DYNAMIC_FIELDS = "dynamicFields";
    private final String OBJECT_SCHEMA_FT_ANALYZER = "analyzer";
    private final String OBJECT_SCHEMA_FT_INDEX_ANALYZER = "indexAnalyzer";
    private final String OBJECT_SCHEMA_FT_QUERY_ANALYZER = "queryAnalyzer";
    private final String OBJECT_SCHEMA_FT_ANALYZER_FILTERS = "filters";
    private final String OBJECT_SCHEMA_FT_ANALYZER_TOKENIZER = "tokenizer";

    private Document mDocument;
    private final AppMgr mAppMgr;

    public SolrSchemaJSON(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mDocument = new Document(Solr.DOCUMENT_SCHEMA_TYPE);
    }

    public SolrSchemaJSON(final AppMgr anAppMgr, Document aDocument)
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

    private DataTable createSchemaFieldTable(String aSchemaString, String aFieldName, String aTitle)
        throws IOException
    {
        String jsonName;
        DataField dataField;

        DataTable dataTable = new DataTable(aTitle);
        DataBag dataBag = dataTable.getColumnBag();

        StringReader stringReader = new StringReader(aSchemaString);
        JsonReader jsonReader = new JsonReader(stringReader);

        jsonReader.beginObject();
        while (jsonReader.hasNext())
        {
            jsonName = jsonReader.nextName();
            if (StringUtils.equals(jsonName, OBJECT_SCHEMA))
            {
                jsonReader.beginObject();
                while (jsonReader.hasNext())
                {
                    jsonName = jsonReader.nextName();
                    if (StringUtils.equals(jsonName, aFieldName))
                    {
                        jsonReader.beginArray();
                        while (jsonReader.hasNext())
                        {
                            jsonReader.beginObject();
                            while (jsonReader.hasNext())
                            {
                                jsonName = jsonReader.nextName();
                                dataField = dataBag.getFieldByName(jsonName);
                                if (dataField == null)
                                {
                                    dataField = new DataTextField(jsonName, Field.nameToTitle(jsonName));
                                    dataField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
                                    dataBag.add(dataField);
                                }
                                jsonReader.skipValue();
                            }
                            jsonReader.endObject();
                        }
                        jsonReader.endArray();;
                    }
                    else
                        jsonReader.skipValue();
                }
                jsonReader.endObject();
            }
            else
                jsonReader.skipValue();
        }
        jsonReader.endObject();
        jsonReader.close();

        return dataTable;
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
                valueString = aReader.nextString();
                break;
            default:
                aReader.skipValue();
                break;
        }

        return valueString;
    }

    private void populateSchemaFieldTable(JsonReader aReader, DataTable aTable)
        throws IOException
    {
        String jsonName, jsonValue;

        aReader.beginArray();
        while (aReader.hasNext())
        {
            aTable.newRow();
            aReader.beginObject();
            while (aReader.hasNext())
            {
                jsonName = aReader.nextName();
                jsonValue = nextValueAsString(aReader);
                aTable.setValueByName(jsonName, jsonValue);
            }
            aReader.endObject();
            aTable.addRow();
        }
        aReader.endArray();
    }

    private void populateSchemaFieldTypeAnalyzer(JsonReader aReader, Document aFTDocument, String aDocType)
        throws IOException
    {
        String jsonName, jsonValue;
        DataTextField dataTextField;
        DataBag tokenizerBag, filterBag;
        Document tokenizerDocument, filterDocument;

        Document analyzerDocument = new Document(aDocType);
        DataBag analyzerBag = analyzerDocument.getBag();
        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, OBJECT_SCHEMA_FT_ANALYZER_TOKENIZER))
            {
                tokenizerDocument = new Document(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER);
                tokenizerDocument.setName(jsonName);
                tokenizerBag = tokenizerDocument.getBag();
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = nextValueAsString(aReader);
                    dataTextField = new DataTextField(jsonName, Field.nameToTitle(jsonName), jsonValue);
                    dataTextField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
                    tokenizerBag.add(dataTextField);
                }
                aReader.endObject();
                analyzerDocument.addRelationship(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER, tokenizerDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_SCHEMA_FT_ANALYZER_FILTERS))
            {
                aReader.beginArray();
                while (aReader.hasNext())
                {
                    filterDocument = new Document(Solr.RESPONSE_SCHEMA_FTA_FILTERS);
                    filterDocument.setName(jsonName);
                    filterBag = filterDocument.getBag();
                    aReader.beginObject();
                    while (aReader.hasNext())
                    {
                        jsonName = aReader.nextName();
                        jsonValue = nextValueAsString(aReader);
                        dataTextField = new DataTextField(jsonName, Field.nameToTitle(jsonName), jsonValue);
                        dataTextField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
                        filterBag.add(dataTextField);
                    }
                    aReader.endObject();
                    analyzerDocument.addRelationship(Solr.RESPONSE_SCHEMA_FTA_FILTERS, filterDocument);
                }
                aReader.endArray();
            }
            else
            {
                jsonValue = nextValueAsString(aReader);
                if (StringUtils.equals(jsonName, "class"))
                    analyzerDocument.setName(jsonName);
                dataTextField = new DataTextField(jsonName, Field.nameToTitle(jsonName), jsonValue);
                dataTextField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
                analyzerBag.add(dataTextField);
            }
        }
        aReader.endObject();

        aFTDocument.addRelationship(Solr.RESPONSE_SCHEMA_FT_ANALYZERS, analyzerDocument);
    }

    private void populateSchemaFieldTypes(JsonReader aReader, Document aSchemaDocument)
        throws IOException
    {
        DataBag fieldTypeBag;
        String jsonName, jsonValue;
        Document fieldTypeDocument;
        DataTextField dataTextField;

        aReader.beginArray();
        while (aReader.hasNext())
        {
            fieldTypeDocument = new Document(Solr.RESPONSE_SCHEMA_FIELD_TYPE);
            fieldTypeBag = fieldTypeDocument.getBag();
            DataTextField operationField = new DataTextField(Solr.SCHEMA_OPERATION_FIELD_NAME, Field.nameToTitle(Solr.SCHEMA_OPERATION_FIELD_NAME));
            operationField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_FALSE);
            fieldTypeBag.add(operationField);
            aReader.beginObject();
            while (aReader.hasNext())
            {
                jsonName = aReader.nextName();
                if (StringUtils.equals(jsonName, OBJECT_SCHEMA_FT_ANALYZER))
                    populateSchemaFieldTypeAnalyzer(aReader, fieldTypeDocument, Solr.RESPONSE_SCHEMA_FT_ANALYZER);
                else if (StringUtils.equals(jsonName, OBJECT_SCHEMA_FT_INDEX_ANALYZER))
                    populateSchemaFieldTypeAnalyzer(aReader, fieldTypeDocument, Solr.RESPONSE_SCHEMA_FT_INDEX_ANALYZER);
                else if (StringUtils.equals(jsonName, OBJECT_SCHEMA_FT_QUERY_ANALYZER))
                    populateSchemaFieldTypeAnalyzer(aReader, fieldTypeDocument, Solr.RESPONSE_SCHEMA_FT_QUERY_ANALYZER);
                else
                {
                    jsonValue = nextValueAsString(aReader);
                    if (StringUtils.equals(jsonName, "name"))
                        fieldTypeDocument.setName(jsonValue);
                    dataTextField = new DataTextField(jsonName, Field.nameToTitle(jsonName), jsonValue);
                    dataTextField.addFeature(Field.FEATURE_IS_STORED, StrUtl.STRING_TRUE);
                    fieldTypeBag.add(dataTextField);
                }
            }
            aReader.endObject();
            aSchemaDocument.addRelationship(Solr.RESPONSE_SCHEMA_FIELD_TYPE, fieldTypeDocument);
        }
        aReader.endArray();
    }

    /**
     * Parses an JSON string representing the Solr schema and loads it into a document
     * and returns an instance to it.
     *
     * @param aSchemaString JSON string representation of the Solr schema.
     *
     * @return Document instance containing the parsed data.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public Document loadDocument(String aSchemaString)
        throws IOException
    {
        String jsonName, jsonValue;

        Document schemaDocument = new Document(Solr.DOCUMENT_SCHEMA_TYPE);
        DataBag schemaBag = schemaDocument.getBag();

// Parse a subset of the JSON payload to identify the table columns before fully processing it.

        SolrSchema solrSchema = new SolrSchema(mAppMgr);
        DataTable fieldsTable = new DataTable(solrSchema.createFieldsBag());
        DataTable dynamicFieldsTable = createSchemaFieldTable(aSchemaString, OBJECT_SCHEMA_DYNAMIC_FIELDS, "Solr Schema Dynamic Fields");
        DataTable copyFieldsTable = createSchemaFieldTable(aSchemaString, OBJECT_SCHEMA_COPY_FIELDS, "Solr Schema Copy Fields");

        StringReader stringReader = new StringReader(aSchemaString);
        JsonReader jsonReader = new JsonReader(stringReader);

        jsonReader.beginObject();
        while (jsonReader.hasNext())
        {
            jsonName = jsonReader.nextName();
            if (StringUtils.equals(jsonName, OBJECT_RESPONSE_HEADER))
            {
                Document headerDocument = new Document(Solr.RESPONSE_SCHEMA_HEADER);
                headerDocument.setName(jsonName);
                DataBag headerBag = headerDocument.getBag();
                jsonReader.beginObject();
                while (jsonReader.hasNext())
                {
                    jsonName = jsonReader.nextName();
                    jsonValue = nextValueAsString(jsonReader);
                    headerBag.add(new DataTextField(jsonName, Field.nameToTitle(jsonName), jsonValue));
                }
                jsonReader.endObject();
                schemaDocument.addRelationship(Solr.RESPONSE_SCHEMA_HEADER, headerDocument);
            }
            else if (StringUtils.equals(jsonName, OBJECT_SCHEMA))
            {
                jsonReader.beginObject();
                while (jsonReader.hasNext())
                {
                    jsonName = jsonReader.nextName();
                    if (StringUtils.equals(jsonName, "name"))
                    {
                        jsonValue = nextValueAsString(jsonReader);
                        schemaBag.add(jsonName, Field.nameToTitle(jsonName), jsonValue);
                    }
                    else if (StringUtils.equals(jsonName, "version"))
                    {
                        jsonValue = nextValueAsString(jsonReader);
                        schemaBag.add(jsonName, Field.nameToTitle(jsonName), jsonValue);
                    }
                    else if (StringUtils.equals(jsonName, "uniqueKey"))
                    {
                        jsonValue = nextValueAsString(jsonReader);
                        schemaBag.add(jsonName, Field.nameToTitle(jsonName), jsonValue);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_SCHEMA_FIELD_TYPES))
                    {
                        populateSchemaFieldTypes(jsonReader, schemaDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_SCHEMA_FIELDS))
                    {
                        populateSchemaFieldTable(jsonReader, fieldsTable);
                        Document fieldsDocument = new Document(Solr.RESPONSE_SCHEMA_FIELD_NAMES, fieldsTable);
                        schemaDocument.addRelationship(Solr.RESPONSE_SCHEMA_FIELD_NAMES, fieldsDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_SCHEMA_DYNAMIC_FIELDS))
                    {
                        populateSchemaFieldTable(jsonReader, dynamicFieldsTable);
                        Document dynamicFieldsDocument = new Document(Solr.RESPONSE_SCHEMA_DYNAMIC_FIELDS, dynamicFieldsTable);
                        schemaDocument.addRelationship(Solr.RESPONSE_SCHEMA_DYNAMIC_FIELDS, dynamicFieldsDocument);
                    }
                    else if (StringUtils.equals(jsonName, OBJECT_SCHEMA_COPY_FIELDS))
                    {
                        populateSchemaFieldTable(jsonReader, copyFieldsTable);
                        Document copyFieldsDocument = new Document(Solr.RESPONSE_SCHEMA_COPY_FIELDS, copyFieldsTable);
                        schemaDocument.addRelationship(Solr.RESPONSE_SCHEMA_COPY_FIELDS, copyFieldsDocument);
                    }
                    else
                        jsonReader.skipValue();
                }
                jsonReader.endObject();
            }
            else
                jsonReader.skipValue();
        }
        jsonReader.endObject();
        jsonReader.close();

        return schemaDocument;
    }

    /**
     * Parses an input stream and loads it into an internally managed
     * document instance.
     *
     * @param anIS Input stream instance.
     *
     * @return Document representation of the Solr schema.
     *
     * @throws java.io.IOException I/O related exception.
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
     * @return Document representation of the Solr schema.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public Document load(String aPathFileName)
        throws IOException
    {
        Document schemaDocument;

        File jsonFile = new File(aPathFileName);
        if (! jsonFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        try (FileInputStream fileInputStream = new FileInputStream(jsonFile))
        {
            schemaDocument = load(fileInputStream);
        }

        return schemaDocument;
    }

    private boolean isOpearationValid(String anOperation)
    {
        switch (anOperation)
        {
            case Solr.SCHEMA_OPERATION_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_REP_FIELD:
            case Solr.SCHEMA_OPERATION_DEL_FIELD:
            case Solr.SCHEMA_OPERATION_DYN_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_DYN_REP_FIELD:
            case Solr.SCHEMA_OPERATION_DYN_DEL_FIELD:
            case Solr.SCHEMA_OPERATION_COPY_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_COPY_DEL_FIELD:
            case Solr.SCHEMA_OPERATION_FT_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_FT_REP_FIELD:
            case Solr.SCHEMA_OPERATION_FT_DEL_FIELD:
                return true;
            default:
                return false;
        }
    }

    private void appendFieldValueJSON(StringBuilder aStringBuilder, DataField aField)
    {
        if (aField.isValueNotEmpty())
        {
            aStringBuilder.append(String.format("  \"%s\":", aField.getName()));
            if (aField.isMultiValue())
            {
                for (String fieldValue : aField.getValues())
                {
                    if (Field.isNumber(aField.getType()))
                        aStringBuilder.append(String.format("%s", fieldValue));
                    else if (Field.isBoolean(aField.getType()))
                        aStringBuilder.append(String.format("%s", StrUtl.stringToBoolean(fieldValue)));
                    else
                        aStringBuilder.append(String.format("\"%s\"", fieldValue));
                }
            }
            else
            {
                if (Field.isNumber(aField.getType()))
                    aStringBuilder.append(String.format("%s", aField.getValue()));
                else if (Field.isBoolean(aField.getType()))
                    aStringBuilder.append(String.format("%s", aField.isValueTrue()));
                else
                    aStringBuilder.append(String.format("\"%s\"", aField.getValue()));
            }
        }
    }

    /**
     * Generates a JSON payload based off the fields in the data bag.
     *
     * @param aStringBuilder String builder instance.
     * @param aBag Data bag instance.
     *
     * @throws DSException Data source exception.
     */
    public void schemaFieldToJSON(StringBuilder aStringBuilder, DataBag aBag)
        throws DSException
    {
        DataField dataField;
        int fieldCount, appendCount;
        Logger appLogger = mAppMgr.getLogger(this, "schemaFieldToJSON");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String schemaOperation = aBag.getValueAsString(Solr.SCHEMA_OPERATION_FIELD_NAME);
        if (StringUtils.isEmpty(schemaOperation))
            throw new DSException("Undefined schema field operation - cannot convert to JSON.");
        else if (! isOpearationValid(schemaOperation))
            throw new DSException(String.format("Schema operation '%s' is invalid - cannot convert to JSON.", schemaOperation));
        else if (! aBag.isValid())
            throw new DSException("Data bag is not valid - cannot convert to JSON.");

        aStringBuilder.append(String.format(" \"%s\": {%n", schemaOperation));
        switch (schemaOperation)
        {
            case Solr.SCHEMA_OPERATION_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_DYN_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_COPY_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_REP_FIELD:
            case Solr.SCHEMA_OPERATION_DYN_REP_FIELD:
                appendCount = 0;
                fieldCount = aBag.count();
                for (int i = 0; i < fieldCount; i++)
                {
                    dataField = aBag.getByOffset(i);
                    if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
                    {
                        if (appendCount > 0)
                            aStringBuilder.append(String.format(",%n"));
                        appendFieldValueJSON(aStringBuilder, dataField);
                        appendCount++;
                    }
                }
                break;
            case Solr.SCHEMA_OPERATION_DEL_FIELD:
            case Solr.SCHEMA_OPERATION_DYN_DEL_FIELD:
                dataField = aBag.getFieldByName("name");
                if (dataField != null)
                    appendFieldValueJSON(aStringBuilder, dataField);
                break;
            case Solr.SCHEMA_OPERATION_COPY_DEL_FIELD:
                dataField = aBag.getFieldByName("source");
                if (dataField != null)
                {
                    appendFieldValueJSON(aStringBuilder, dataField);
                    aStringBuilder.append(",");
                    dataField = aBag.getFieldByName("dest");
                    if (dataField != null)
                        appendFieldValueJSON(aStringBuilder, dataField);
                }
                break;
            default:
                break;
        }
        aStringBuilder.append("}");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /* A Analyzer (including index and query) can have exactly one tokenizer relationship and
    one-or-more filter relationships (due to varying parameters). */

    private void fieldTypeAnalyzerToJSON(StringBuilder aStringBuilder, Document aDocument, String anAnalyzerName)
    {
        DataField dataField;
        int fieldCount, appendCount;
        Logger appLogger = mAppMgr.getLogger(this, "fieldTypeAnalyzerToJSON");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        aStringBuilder.append(String.format(",%n \"%s\": {%n", anAnalyzerName));
        Document tokenizerDocument = aDocument.getFirstRelatedDocument(Solr.RESPONSE_SCHEMA_FTA_TOKENIZER);
        if (tokenizerDocument != null)
        {
            aStringBuilder.append(String.format("   \"tokenizer\": {%n"));
            DataBag tokenizerBag = tokenizerDocument.getBag();
            appendCount = 0;
            fieldCount = tokenizerBag.count();
            for (int i = 0; i < fieldCount; i++)
            {
                dataField = tokenizerBag.getByOffset(i);
                if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
                {
                    if (appendCount > 0)
                        aStringBuilder.append(String.format(",%n"));
                    appendFieldValueJSON(aStringBuilder, dataField);
                    appendCount++;
                }
            }
            aStringBuilder.append(" }");
        }
        ArrayList<Document> filterDocuments = aDocument.getRelatedDocuments(Solr.RESPONSE_SCHEMA_FTA_FILTERS);
        if ((filterDocuments != null) && (filterDocuments.size() > 0))
        {
            if (tokenizerDocument != null)
                aStringBuilder.append(String.format(",%n"));
            aStringBuilder.append("   \"filters\": [");

            DataBag filterBag;
            int filterCount = 0;
            for (Document filterDocument : filterDocuments)
            {
                if (filterCount > 0)
                    aStringBuilder.append(",");
                filterBag = filterDocument.getBag();
                appendCount = 0;
                fieldCount = filterBag.count();
                if (fieldCount > 0)
                {
                    aStringBuilder.append("{");
                    for (int i = 0; i < fieldCount; i++)
                    {
                        dataField = filterBag.getByOffset(i);
                        if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
                        {
                            if (appendCount > 0)
                                aStringBuilder.append(String.format(",%n"));
                            appendFieldValueJSON(aStringBuilder, dataField);
                            appendCount++;
                        }
                    }
                    aStringBuilder.append("}");
                }
                filterCount++;
            }
            aStringBuilder.append("]");
        }
        aStringBuilder.append(String.format(" }%n"));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Generates a JSON payload based off the document field type.
     *
     * @param aStringBuilder String builder instance.
     * @param aDocument Document describing the field type.
     *
     * @throws DSException Data source exception.
     */
    public void schemaFieldTypeToJSON(StringBuilder aStringBuilder, Document aDocument)
        throws DSException
    {
        DataField dataField;
        int fieldCount, appendCount;
        Logger appLogger = mAppMgr.getLogger(this, "schemaFieldTypeToJSON");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag fieldTypeBag = aDocument.getBag();
        String schemaOperation = fieldTypeBag.getValueAsString(Solr.SCHEMA_OPERATION_FIELD_NAME);
        if (StringUtils.isEmpty(schemaOperation))
            throw new DSException("Undefined schema field operation - cannot convert to JSON.");
        else if (! isOpearationValid(schemaOperation))
            throw new DSException(String.format("Schema operation '%s' is invalid - cannot convert to JSON.", schemaOperation));
        else if (! fieldTypeBag.isValid())
            throw new DSException("Data bag is not valid - cannot convert to JSON.");

        aStringBuilder.append(String.format(" \"%s\": {%n", schemaOperation));
        switch (schemaOperation)
        {
            case Solr.SCHEMA_OPERATION_FT_ADD_FIELD:
            case Solr.SCHEMA_OPERATION_FT_REP_FIELD:
                appendCount = 0;
                fieldCount = fieldTypeBag.count();
                for (int i = 0; i < fieldCount; i++)
                {
                    dataField = fieldTypeBag.getByOffset(i);
                    if ((dataField.isValueNotEmpty()) && (dataField.isFeatureTrue(Field.FEATURE_IS_STORED)))
                    {
                        if (appendCount > 0)
                            aStringBuilder.append(String.format(",%n"));
                        appendFieldValueJSON(aStringBuilder, dataField);
                        appendCount++;
                    }
                }
                ArrayList<Document> analyzerDocuments = aDocument.getRelatedDocuments(Solr.RESPONSE_SCHEMA_FT_ANALYZERS);
                for (Document analyzerDocument: analyzerDocuments)
                {
                    switch (analyzerDocument.getType())
                    {
                        case Solr.RESPONSE_SCHEMA_FT_ANALYZER:
                            fieldTypeAnalyzerToJSON(aStringBuilder, analyzerDocument, "analyzer");
                            break;
                        case Solr.RESPONSE_SCHEMA_FT_INDEX_ANALYZER:
                            fieldTypeAnalyzerToJSON(aStringBuilder, analyzerDocument, "indexAnalyzer");
                            break;
                        case Solr.RESPONSE_SCHEMA_FT_QUERY_ANALYZER:
                            fieldTypeAnalyzerToJSON(aStringBuilder, analyzerDocument, "queryAnalyzer");
                            break;
                        default:
                            break;
                    }
                }
                break;
            case Solr.SCHEMA_OPERATION_FT_DEL_FIELD:
                dataField = fieldTypeBag.getFieldByName("name");
                if (dataField != null)
                    appendFieldValueJSON(aStringBuilder, dataField);
                break;
            default:
                break;
        }
        aStringBuilder.append("}");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
