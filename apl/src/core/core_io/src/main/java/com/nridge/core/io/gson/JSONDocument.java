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

package com.nridge.core.io.gson;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.DatUtl;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

/**
 * The JSONDocument provides a collection of methods that can load
 * a JSON payload into a <i>Document</i> object.  This class does
 * not offer save methods since that should be handled by the
 * DocumentJSON class.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class JSONDocument
{
    public JSONDocument()
    {
    }

    private boolean isNextTokenAnObject(JsonReader aReader)
        throws IOException
    {
        JsonToken jsonToken = aReader.peek();

        return (jsonToken == JsonToken.BEGIN_OBJECT);
    }

    private void loadDocumentArray(JsonReader aReader, Document aDocument)
        throws IOException
    {
        JsonToken jsonToken;
        Document childDocument;

        do
        {
            childDocument = new Document(aDocument.getType());
            childDocument.setName(aDocument.getName());
            aDocument.addRelationship(aDocument.getTitle(), childDocument);
            loadDocument(aReader, childDocument);
            jsonToken = aReader.peek();
        } while (jsonToken == JsonToken.BEGIN_OBJECT);
    }

    /**
     * Parses an JSON stream and loads it into an internally managed
     * document instance.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws IOException I/O related exception.
     */
    private void loadDocument(JsonReader aReader, Document aDocument)
        throws IOException
    {
        DataBag dataBag;
        JsonToken jsonToken;
        DataField dataField;
        String jsonName, jsonValue, jsonTitle;

        aReader.beginObject();

        jsonToken = aReader.peek();
        while (jsonToken == JsonToken.NAME)
        {
            jsonName = aReader.nextName();
            jsonTitle = Field.nameToTitle(jsonName);

            jsonToken = aReader.peek();
            switch (jsonToken)
            {
                case BOOLEAN:
                    dataBag = aDocument.getBag();
                    dataField = new DataField(jsonName, jsonTitle, aReader.nextBoolean());
                    dataBag.add(dataField);
                    break;
                case NUMBER:
                    dataBag = aDocument.getBag();
                    jsonValue = aReader.nextString();
                    if (StringUtils.contains(jsonValue, StrUtl.CHAR_DOT))
                        dataField = new DataField(jsonName, jsonTitle, Double.valueOf(jsonValue));
                    else
                        dataField = new DataField(jsonName, jsonTitle, Long.valueOf(jsonValue));
                    dataBag.add(dataField);
                    break;
                case STRING:
                    dataBag = aDocument.getBag();
                    jsonValue = aReader.nextString();
                    Date dateValue = DatUtl.detectCreateDate(jsonValue);
                    if (dateValue != null)
                        dataField = new DataField(jsonName, jsonTitle, dateValue);
                    else
                        dataField = new DataField(Field.Type.Text, jsonName, jsonTitle, jsonValue);
                    dataBag.add(dataField);
                    break;
                case NULL:
                    dataBag = aDocument.getBag();
                    aReader.nextNull();
                    dataField = new DataField(Field.Type.Text, jsonName, jsonTitle);
                    dataBag.add(dataField);
                    break;
                case BEGIN_ARRAY:
                    aReader.beginArray();
                    if (isNextTokenAnObject(aReader))
                    {
                        Document childDocument = new Document(jsonTitle);
                        childDocument.setName(jsonName);
                        aDocument.addRelationship(jsonTitle, childDocument);
                        loadDocumentArray(aReader, childDocument);
                    }
                    else
                    {
                        dataBag = aDocument.getBag();
                        dataField = new DataField(Field.Type.Text, jsonName, jsonTitle);
                        jsonToken = aReader.peek();
                        while (jsonToken != JsonToken.END_ARRAY)
                        {
                            jsonValue = aReader.nextString();
                            dataField.addValue(jsonValue);
                            jsonToken = aReader.peek();
                        }
                        dataBag.add(dataField);
                    }
                    aReader.endArray();
                    break;
                case BEGIN_OBJECT:
                    Document childDocument = new Document(jsonTitle);
                    childDocument.setName(jsonName);
                    aDocument.addRelationship(jsonTitle, childDocument);
                    loadDocument(aReader, childDocument);
                    break;
                default:
                    aReader.skipValue();
                    break;
            }
            jsonToken = aReader.peek();
        }

        aReader.endObject();
    }

    /**
     * Parses an JSON stream and loads it into a document and returns
     * an instance to it.
     *
     * @param aReader Json reader stream instance.
     * @param aType Type of document to create.
     * @param aName Name of document to assign.
     *
     * @return Document instance containing parsed data.
     *
     * @throws IOException I/O related error condition.
     */
    public Document load(JsonReader aReader, String aType, String aName)
        throws IOException
    {
        Document rootDocument = new Document(aType);
        rootDocument.setName(aName);
        loadDocument(aReader, rootDocument);

        return rootDocument;
    }

    /**
     * Parses an input stream and loads it into a document instance.
     *
     * @param anIS Input stream instance.
     * @param aType Document type.
     * @param aName Document name.
     *
     * @throws IOException I/O related exception.
     */
    public Document load(InputStream anIS, String aType, String aName)
        throws IOException
    {
        InputStreamReader inputStreamReader = new InputStreamReader(anIS);
        JsonReader jsonReader = new JsonReader(inputStreamReader);

        return load(jsonReader, aType, aName);
    }
}
