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
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.io.IO;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;

/**
 * The RelationshipJSON provides a collection of methods that can generate/load
 * a JSON representation of a <i>Relationship</i> object.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class RelationshipJSON
{
    private Relationship mRelationship;

    /**
     * Default constructor.
     */
    public RelationshipJSON()
    {
        mRelationship = new Relationship();
    }

    /**
     * Constructor accepts a relationship as a parameter.
     *
     * @param aRelationship Relationship instance.
     */
    public RelationshipJSON(Relationship aRelationship)
    {
        mRelationship = aRelationship;
    }

    /**
     * Assigns a {@link Relationship}.  This should be done via
     * a constructor or this method prior to a save method
     * being invoked.
     *
     * @param aRelationship Relationship instance.
     */
    public void setRelationship(Relationship aRelationship)
    {
        mRelationship = aRelationship;
    }

    /**
     * Returns a reference to the {@link Relationship} that this
     * class is managing.
     *
     * @return Document instance.
     */
    public Relationship getRelationship()
    {
        return mRelationship;
    }

    /**
     * Saves the Relationship to the writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     * @param aRelationship Relationship instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter, Relationship aRelationship)
        throws IOException
    {
        aWriter.beginObject();
        IOJSON.writeNameValue(aWriter, IO.JSON_TYPE_MEMBER_NAME, aRelationship.getType());
        IOJSON.writeNameValue(aWriter, IO.JSON_FEATURES_ARRAY_NAME, aRelationship.getFeatures());
        DataBag dataBag = aRelationship.getBag();
        if ((dataBag != null) && (dataBag.assignedCount() > 0))
        {
            DataBagJSON dataBagJSON = new DataBagJSON(dataBag);
            dataBagJSON.save(aWriter, true);
        }
        ArrayList<Document> relatedDocuments = aRelationship.getDocuments();
        if (relatedDocuments.size() > 0)
        {
            aWriter.name(IO.JSON_DOCUMENTS_ARRAY_NAME).beginArray();
            DocumentJSON documentJSON = new DocumentJSON();
            for (Document document : relatedDocuments)
                documentJSON.save(aWriter, document);
            aWriter.endArray();
        }
        aWriter.endObject();
    }

    /**
     * Saves the previous assigned relationship (e.g. via constructor or set method)
     * to the JSON writer stream specified as a parameter.
     *
     * @param aWriter Json writer stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(JsonWriter aWriter)
        throws IOException
    {
        save(aWriter, mRelationship);
    }

    /**
     * Saves the previous assigned relationship (e.g. via constructor or set method)
     * to the writer stream specified as a parameter.
     *
     * @param anOS Output stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void save(OutputStream anOS)
        throws IOException
    {
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(anOS);
        JsonWriter jsonWriter = new JsonWriter(outputStreamWriter);
        save(jsonWriter);
    }

    /**
     * Saves the previous assigned relationship (e.g. via constructor or set method)
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

    private void loadDocuments(JsonReader aReader, Relationship aRelationship)
        throws IOException
    {
        Document relationshipDocument;

        DocumentJSON documentJSON = new DocumentJSON();
        ArrayList<Document> documentList = aRelationship.getDocuments();
        aReader.beginArray();
        while (aReader.hasNext())
        {
            relationshipDocument = documentJSON.loadDocument(aReader);
            documentList.add(relationshipDocument);
        }
        aReader.endArray();
    }

    /**
     * Parses an JSON stream and loads it into a relationship and returns
     * an instance to it.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public Relationship loadRelationship(JsonReader aReader)
        throws IOException
    {
        String jsonName, jsonValue;

        Relationship relationship = new Relationship();

        aReader.beginObject();
        while (aReader.hasNext())
        {
            jsonName = aReader.nextName();
            if (StringUtils.equals(jsonName, IO.JSON_TYPE_MEMBER_NAME))
                relationship.setType(aReader.nextString());
            else if (StringUtils.equals(jsonName, IO.JSON_FEATURES_ARRAY_NAME))
            {
                aReader.beginObject();
                while (aReader.hasNext())
                {
                    jsonName = aReader.nextName();
                    jsonValue = aReader.nextString();
                    relationship.addFeature(jsonName, jsonValue);
                }
                aReader.endObject();
            }
            else if (StringUtils.equals(jsonName, IO.JSON_DOCUMENTS_ARRAY_NAME))
                loadDocuments(aReader, relationship);
            else
                aReader.skipValue();
        }
        aReader.endObject();

        return relationship;
    }

    /**
     * Parses an JSON stream and loads it into an internally
     * managed relationship.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(JsonReader aReader)
        throws IOException
    {
        mRelationship = loadRelationship(aReader);
    }

    /**
     * Parses an input stream and loads it into an internally
     * managed relationship.
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
     * Parses a JSON formatted path/file name and loads it into an internally
     * managed relationship.
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
