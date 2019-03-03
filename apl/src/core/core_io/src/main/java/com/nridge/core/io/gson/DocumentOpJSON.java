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
import com.google.gson.stream.JsonWriter;
import com.nridge.core.base.doc.Doc;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.io.DocOpInterface;
import com.nridge.core.base.io.IO;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;

/**
 * The DocumentJSON provides a collection of methods that can generate/load
 * a JSON representation of a <i>Document</i> operation object.
 * <p>
 * This class utilizes the
 * <a href="https://code.google.com/p/google-gson/">Gson</a>
 * framework to manage these transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class DocumentOpJSON implements DocOpInterface
{
    private DataField mField;
    private DSCriteria mCriteria;
    private ArrayList<Document> mDocumentList;

    /**
     * Default constructor.
     */
    public DocumentOpJSON()
    {
        mField = new DataTextField("undefined");
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Default constructor.
     *
     * @param aName Operation name.
     */
    public DocumentOpJSON(String aName)
    {
        mField = new DataTextField(aName);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Constructor accepts a list of document instances as a parameter.
     *
     * @param aName Operation name.
     * @param aDocumentList A list of document instances.
     */
    public DocumentOpJSON(String aName, ArrayList<Document> aDocumentList)
    {
        mField = new DataTextField(aName);
        mDocumentList = aDocumentList;
    }

    /**
     * Constructor accepts a data source criteria as a parameter.
     *
     * @param aName Operation name.
     * @param aCriteria Data source criteria instance.
     */
    public DocumentOpJSON(String aName, DSCriteria aCriteria)
    {
        mCriteria = aCriteria;
        mField = new DataTextField(aName);
        mDocumentList = new ArrayList<Document>();
    }

    /**
     * Assigns an operation field containing a name and other desired features.
     *
     * @param aField Operation field.
     */
    public void setField(DataField aField)
    {
        mField = aField;
    }

    /**
     * Returns an operation field.
     *
     * @return Data field instance.
     */
    public DataField getField()
    {
        return mField;
    }

    /**
     * Assigns standard account features to the operation. Please
     * note that the parent application is responsible for encrypting
     * the account password prior to assigning it in this method.
     *
     * @param anAccountName Account name.
     * @param anAccountPassword Account password.
     */
    public void setAccountNamePassword(String anAccountName, String anAccountPassword)
    {
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_NAME, anAccountName);
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_PASSWORD, anAccountPassword);
    }

    /**
     * Assigns standard account features to the operation. Please
     * note that the parent application is responsible for encrypting
     * the account password prior to assigning it in this method.
     *
     * @param anAccountName Account name.
     * @param anAccountPasshash Account password.
     */
    public void setAccountNamePasshash(String anAccountName, String anAccountPasshash)
    {
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_NAME, anAccountName);
        mField.addFeature(Doc.FEATURE_OP_ACCOUNT_PASSHASH, anAccountPasshash);
    }

    /**
     * Assigns standard account features to the operation.
     *
     * @param aSessionId Session identifier.
     */
    public void setSessionId(String aSessionId)
    {
        mField.addFeature(Doc.FEATURE_OP_SESSION, aSessionId);
    }

    /**
     * Adds the document to the internally managed document list.
     *
     * @param aDocument Document instance.
     */
    public void addDocument(Document aDocument)
    {
        if (aDocument != null)
            mDocumentList.add(aDocument);
    }

    /**
     * Assigns the document list parameter to the internally managed list instance.
     *
     * @param aDocumentList A list of document instances.
     */
    public void setDocumentList(ArrayList<Document> aDocumentList)
    {
        if (aDocumentList != null)
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
     * Assigns the data source criteria instance.  A criteria instance
     * is only assigned for <code>Doc.OPERATION_FETCH</code> names.
     *
     * @param aCriteria Data source criteria instance.
     */
    public void setCriteria(DSCriteria aCriteria)
    {
        mCriteria = aCriteria;
    }

    /**
     * Returns a reference to an internally managed criteria instance.
     *
     * @return Data source criteria instance if the operation was
     * <code>Doc.OPERATION_FETCH</code> or <i>null</i> otherwise.
     */
    public DSCriteria getCriteria()
    {
        return mCriteria;
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

        if (mCriteria != null)
        {
            DSCriteriaJSON dsCriteriaJSON = new DSCriteriaJSON(mCriteria);
            dsCriteriaJSON.save(aWriter, true);
        }
        else
        {
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
     * document operation instance.
     *
     * @param aReader Json reader stream instance.
     *
     * @throws java.io.IOException I/O related exception.
     */
    public void load(JsonReader aReader)
        throws IOException
    {
        String jsonName;

        mCriteria = null;
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
            else if (StringUtils.equals(jsonName, IO.JSON_CRITERIA_OBJECT_NAME))
            {
                DSCriteriaJSON dsCriteriaJSON = new DSCriteriaJSON();
                dsCriteriaJSON.load(aReader);
                mCriteria = dsCriteriaJSON.getCriteria();
            }
            else
                aReader.skipValue();
        }

        aReader.endObject();
    }

    /**
     * Parses an input stream and loads it into an internally managed
     * document operation instance.
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
     * document operation instance.
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
