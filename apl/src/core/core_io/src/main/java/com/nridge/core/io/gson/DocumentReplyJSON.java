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
import com.nridge.core.base.doc.Doc;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataIntegerField;
import com.nridge.core.base.io.DocReplyInterface;
import com.nridge.core.base.io.IO;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;

/**
 * The DocumentReplyJSON class provides JSON helper methods.
 */
public class DocumentReplyJSON implements DocReplyInterface
{
    private DataField mField;
    private ArrayList<Document> mDocumentList;

    /**
     * Default constructor.
     */
    public DocumentReplyJSON()
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(Doc.STATUS_CODE_SUCCESS);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Default constructor that accepts a status code.
     *
     * @param aCode Status code.
     */
    public DocumentReplyJSON(int aCode)
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(aCode);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Default constructor that accepts a status code.
     *
     * @param aCode Status code.
     * @param aSessionId Session identifier.
     */
    public DocumentReplyJSON(int aCode, String aSessionId)
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(aCode);
        mField.addFeature(Doc.FEATURE_REPLY_SESSION_ID, aSessionId);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Constructor accepts a status code and a list of document instances
     * as a parameter.
     *
     * @param aCode Status code.
     * @param aDocumentList A list of document instances.
     */
    public DocumentReplyJSON(int aCode, ArrayList<Document> aDocumentList)
    {
        mField = new DataIntegerField("status_code", "Status Code");
        mField.setValue(aCode);
        mDocumentList = aDocumentList;
    }

    /**
     * Assigns a reply field containing status and other desired features.
     *
     * @param aField Operation field.
     */
    public void setField(DataField aField)
    {
        mField = aField;
    }

    /**
     * Returns a reply field.
     *
     * @return Data field instance.
     */
    public DataField getField()
    {
        return mField;
    }

    /**
     * Assigns standard message features to the reply.
     *
     * @param aMessage Reply message.
     * @param aDetail Reply message detail.
     */
    public void setFeatures(String aMessage, String aDetail)
    {
        if (StringUtils.isNotEmpty(aMessage))
            mField.addFeature(Doc.FEATURE_REPLY_MESSAGE, aMessage);
        if (StringUtils.isNotEmpty(aDetail))
            mField.addFeature(Doc.FEATURE_REPLY_DETAIL, aDetail);
    }

    /**
     * Assigns standard account features to the reply.
     *
     * @param aSessionId Session identifier.
     */
    public void setFeatures(String aSessionId)
    {
        mField.addFeature(Doc.FEATURE_OP_SESSION, aSessionId);
    }

    /**
     * Assigns the document list parameter to the internally managed list instance.
     *
     * @param aDocumentList A list of document instances.
     */
    public void setDocumentList(ArrayList<Document> aDocumentList)
    {
        mDocumentList = aDocumentList;
    }

    /**
     * Returns a reference to the internally managed list of document instances.
     *
     * @return List of document instances.
     */
    public ArrayList<Document> getDocumentList()
    {
        return mDocumentList;
    }

    /**
     * Saves the operation payload to the writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter)
        throws IOException
    {
        aWriter.beginObject();

        DataFieldJSON dataFieldJSON = new DataFieldJSON();
        dataFieldJSON.save(aWriter, mField, true);

        int docCount = mDocumentList.size();
        if (docCount > 0)
        {
            DocumentJSON documentJSON;

            aWriter.name(IO.JSON_DOCUMENTS_ARRAY_NAME).beginArray();
            for (Document document : mDocumentList)
            {
                documentJSON = new DocumentJSON(document);
                documentJSON.save(aWriter);
            }

            aWriter.endArray();
        }

        aWriter.endObject();
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
        String jsonName;

        mDocumentList.clear();
        mField.clearFeatures();

        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_FIELD_OBJECT_NAME))
            {
                DataFieldJSON dataFieldJSON = new DataFieldJSON();
                mField = dataFieldJSON.load(aReader);
            }
            else if (StringUtils.equals(jsonName, IO.JSON_DOCUMENTS_ARRAY_NAME))
            {
                DocumentJSON documentJSON;

                aReader.beginArray();
                while (aReader.hasNext())
                {
                    documentJSON = new DocumentJSON();
                    documentJSON.load(aReader);
                    mDocumentList.add(documentJSON.getDocument());
                }
                aReader.endArray();
            }
            else
                aReader.skipValue();
        }

        aReader.endObject();
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
