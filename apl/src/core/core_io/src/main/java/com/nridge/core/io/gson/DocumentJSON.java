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

package com.nridge.core.io.gson;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * The DocumentJSON provides a collection of methods that can generate/load
 * a JSON representation of a <i>Document</i> object.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DocumentJSON
{
    private Document mDocument;

    /**
     * Default constructor.
     */
    public DocumentJSON()
    {
        mDocument = new Document(IO.XML_DOCUMENT_NODE_NAME);
    }

    /**
     * Constructor accepts a document as a parameter.
     *
     * @param aDocument Document instance.
     */
    public DocumentJSON(Document aDocument)
    {
        mDocument = aDocument;
    }

    /**
     * Assigns a {@link Document}.  This should be done via
     * a constructor or this method prior to a save method
     * being invoked.
     *
     * @param aDocument Document instance.
     */
    public void setDocument(Document aDocument)
    {
        mDocument = aDocument;
    }

    /**
     * Returns a reference to the {@link Document} that this
     * class is managing.
     *
     * @return Document instance.
     */
    public Document getDocument()
    {
        return mDocument;
    }

    /**
     * Saves the Document to the writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     * @param aDocument Document instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter, Document aDocument)
        throws IOException
    {
        aWriter.beginObject();

        IOJSON.writeNameValue(aWriter, IO.JSON_NAME_MEMBER_NAME, aDocument.getName());
        IOJSON.writeNameValue(aWriter, IO.JSON_VERSION_MEMBER_NAME, aDocument.getSchemaVersion());
        IOJSON.writeNameValue(aWriter, IO.JSON_TYPE_MEMBER_NAME, aDocument.getType());
        IOJSON.writeNameValue(aWriter, IO.JSON_TITLE_MEMBER_NAME, aDocument.getTitle());
        IOJSON.writeNameValue(aWriter, IO.JSON_FEATURES_ARRAY_NAME, aDocument.getFeatures());

        DataTableJSON dataTableJSON = new DataTableJSON(aDocument.getTable());
        dataTableJSON.save(aWriter, true);

        ArrayList<Relationship> relationshipList = aDocument.getRelationships();
        if (relationshipList.size() > 0)
        {
            aWriter.name(IO.JSON_RELATED_ARRAY_NAME).beginArray();
            RelationshipJSON relationshipJSON = new RelationshipJSON();
            for (Relationship relationship : relationshipList)
                relationshipJSON.save(aWriter, relationship);
            aWriter.endArray();
        }

        HashMap<String, String> docACL = aDocument.getACL();
        if (docACL.size() > 0)
        {
            aWriter.name(IO.JSON_ACL_ARRAY_NAME).beginArray();
            for (Map.Entry<String,String> aclEntry : docACL.entrySet())
            {
                aWriter.beginObject();
                aWriter.name(IO.JSON_NAME_MEMBER_NAME).value(aclEntry.getKey());
                aWriter.name(IO.JSON_VALUE_MEMBER_NAME).value(aclEntry.getValue());
                aWriter.endObject();
            }
            aWriter.endArray();
        }

        aWriter.endObject();
    }

    /**
     * Saves the previous assigned document (e.g. via constructor or set method)
     * to the JSON writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter)
        throws IOException
    {
        save(aWriter, mDocument);
    }

    /**
     * Saves the previous assigned document (e.g. via constructor or set method)
     * to the writer stream specified as a parameter.
     *
     * @param anOS Output stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(OutputStream anOS)
        throws IOException
    {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(anOS, StrUtl.CHARSET_UTF_8);
        try (JsonWriter jsonWriter = new JsonWriter(outputStreamWriter))
        {
            save(jsonWriter);
        }
    }

    /**
     * Saves the previous assigned document (e.g. via constructor or set method)
     * to the writer stream specified as a parameter.
     *
     * @param aPathFileName Output path file name.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(String aPathFileName)
        throws IOException
    {
        try (FileOutputStream fileOutputStream = new FileOutputStream(aPathFileName))
        {
            save(fileOutputStream);
        }
    }

    /**
     * Saves the previous assigned document list (e.g. via constructor or set method)
     * to a string and returns it.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public String saveAsString()
        throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        try (PrintWriter printWriter = new PrintWriter(stringWriter))
        {
            try (JsonWriter jsonWriter = new JsonWriter(printWriter))
            {
                save(jsonWriter);
            }
        }

        return stringWriter.toString();
    }

    private void loadACL(JsonReader aReader, Document aDocument)
        throws IOException
    {
        String jsonName, aceName, aceValue;

        HashMap<String, String> docACL = aDocument.getACL();
        aReader.beginArray();
        while (aReader.hasNext())
        {
            aceName = StringUtils.EMPTY;

            aReader.beginObject();
            while (aReader.hasNext())
            {
                jsonName = aReader.nextName();
                if (StringUtils.equals(jsonName, IO.JSON_NAME_MEMBER_NAME))
                    aceName = aReader.nextString();
                else if (StringUtils.equals(jsonName, IO.JSON_VALUE_MEMBER_NAME))
                {
                    aceValue = aReader.nextString();
                    if (StringUtils.isNotEmpty(aceName))
                        docACL.put(aceName, aceValue);
                }
                else
                    aReader.skipValue();
            }
            aReader.endObject();
        }
        aReader.endArray();
    }

    private void loadRelated(JsonReader aReader, Document aDocument)
        throws IOException
    {
        Relationship relationship;

        RelationshipJSON relationshipJSON = new RelationshipJSON();
        ArrayList<Relationship> relationshipList = aDocument.getRelationships();
        aReader.beginArray();
        while (aReader.hasNext())
        {
            relationship = relationshipJSON.loadRelationship(aReader);
            relationshipList.add(relationship);
        }
        aReader.endArray();
    }

    /**
     * Parses an JSON stream and loads it into a document and returns
     * an instance to it.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public Document loadDocument(JsonReader aReader)
        throws IOException
    {
        String jsonName, jsonValue;

        Document document = new Document(IO.XML_DOCUMENT_NODE_NAME);

        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_NAME_MEMBER_NAME))
                document.setName(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_TYPE_MEMBER_NAME))
                document.setType(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_VERSION_MEMBER_NAME))
                document.setSchemaVersion(Integer.parseInt(aReader.nextString()));
            else if (StringUtils.equals(jsonName, IO.JSON_TITLE_MEMBER_NAME))
                document.setTitle(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_FEATURES_ARRAY_NAME))
            {
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = aReader.nextString();
                    document.addFeature(jsonName, jsonValue);
                }
                aReader.endObject();
            }
            else if (StringUtils.equals(jsonName, IO.JSON_TABLE_OBJECT_NAME))
            {
                DataTableJSON dataTableJSON = new DataTableJSON();
                dataTableJSON.load(aReader);
                document.setTable(dataTableJSON.getTable());
            }
            else if (StringUtils.equals(jsonName, IO.JSON_RELATED_ARRAY_NAME))
                loadRelated(aReader, document);
            else if (StringUtils.equals(jsonName, IO.JSON_ACL_ARRAY_NAME))
                loadACL(aReader, document);
            else
                aReader.skipValue();
        }
        aReader.endObject();

        return document;
    }

    /**
     * Parses an JSON stream and loads it into an internally managed
     * document instance.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(JsonReader aReader)
        throws IOException
    {
        mDocument = loadDocument(aReader);
    }

    /**
     * Parses an input stream and loads it into an internally managed
     * document instance.
     *
     * @param anIS Input stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(InputStream anIS)
        throws IOException
    {
        InputStreamReader inputStreamReader = new InputStreamReader(anIS);
        JsonReader jsonReader = new JsonReader(inputStreamReader);
        load(jsonReader);
    }

    /**
     * Parses a JSON formatted path/file name and loads it into an internally managed
     * document instance.
     *
     * @param aPathFileName Input path file name.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(String aPathFileName)
        throws IOException
    {
        File jsonFile = new File(aPathFileName);
        if (! jsonFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        try (FileInputStream fileInputStream = new FileInputStream(jsonFile))
        {
            load(fileInputStream);
        }
    }
}
