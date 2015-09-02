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

package com.nridge.ds.solr;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.io.xml.IOXML;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import com.nridge.core.base.std.XMLUtl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.w3c.dom.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.*;

/**
 * The SolrSchemaXML provides a collection of methods that can load
 * an XML representation of a Solr schema.  In general, the developer
 * should use the data source I/O methods instead of this helper
 * implementation.
 *
 * @author Al Cole
 * @since 1.0
 */
public class SolrSchemaXML
{
    private final String SOLR_TEXT_FIELD_TYPE_DEFAULT = "text_en";

    private Document mDocument;
    private final AppMgr mAppMgr;

    public SolrSchemaXML(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mDocument = new Document("Solr Schema");
    }

    public SolrSchemaXML(final AppMgr anAppMgr, Document aDocument)
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

    private void saveComment(PrintWriter aPW, int anIndentAmount)
        throws IOException
    {
        anIndentAmount++;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<!-- Valid attributes for fields:%n");
        anIndentAmount++;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("name: mandatory - the name for the field%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("type: mandatory - the name of a field type from the <types> fieldType section%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("indexed: true if this field should be indexed (searchable or sortable)%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("stored: true if this field should be retrievable%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("multiValued: true if this field may contain multiple values per document%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("omitNorms: (expert) set to true to omit the norms associated with%n");
        anIndentAmount += 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("this field (this disables length normalization and index-time%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("boosting for the field, and saves some memory).  Only full-text%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("fields or fields that need an index-time boost need norms.%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("Norms are omitted for primitive (non-analyzed) types by default.%n");
        anIndentAmount -= 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("termVectors: [false] set to true to store the term vector for a%n");
        anIndentAmount += 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("given field.%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("When using MoreLikeThis, fields used for similarity should be%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("stored for best performance.%n");
        anIndentAmount -= 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("termPositions: Store position information with the term vector.%n");
        anIndentAmount += 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("This will increase storage costs.%n");
        anIndentAmount -= 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("termOffsets: Store offset information with the term vector. This %n");
        anIndentAmount += 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("will increase storage costs.%n");
        anIndentAmount -= 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("required: The field is required.  It will throw an error if the%n");
        anIndentAmount += 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("value does not exist%n");
        anIndentAmount -= 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("default: a value that should be used if no value is specified%n");
        anIndentAmount += 2;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("when adding a document.%n");
        anIndentAmount -= 3;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("-->%n");

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<!--%n");
        anIndentAmount++;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("field names should consist of alphanumeric or underscore characters only and%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("not start with a digit.  This is not currently strictly enforced,%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("but other field names will not have first class support from all components%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("and back compatibility is not guaranteed.  Names with both leading and%n");
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("trailing underscores (e.g. _version_) are reserved.%n");
        anIndentAmount--;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("-->%n");
    }

    private String mapFieldType(DataField aField)
    {
        String fieldType;

        switch (aField.getType())
        {
            case Text:
                if ((StringUtils.endsWith(aField.getName(), "_name")) ||
                    (StringUtils.endsWith(aField.getName(), "_title")) ||
                    (StringUtils.endsWith(aField.getName(), "_description")) ||
                    (StringUtils.endsWith(aField.getName(), "_content")))
                    fieldType = SOLR_TEXT_FIELD_TYPE_DEFAULT;
                else
                    fieldType = "string";
                break;
            case Integer:
                fieldType = "int";
                break;
            case Long:
                fieldType = "long";
                break;
            case Float:
                fieldType = "float";
                break;
            case Double:
                fieldType = "double";
                break;
            case Boolean:
                fieldType = "boolean";
                break;
            case Date:
            case Time:
            case DateTime:
                fieldType = "date";
                break;
            default:
                fieldType = "string";
                break;
        }

        return fieldType;
    }

// http://wiki.apache.org/solr/SchemaXml

    public void save(PrintWriter aPW, String aTagName, int anIndentAmount)
        throws IOException
    {
        DataField dataField;

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<%s>%n", aTagName);
        saveComment(aPW, anIndentAmount);
        anIndentAmount++;

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<field name=\"text\" type=\"%s\" indexed=\"true\" stored=\"true\" multiValued=\"true\"/>%n",
                    SOLR_TEXT_FIELD_TYPE_DEFAULT);

        DataBag dataBag = mDocument.getBag();
        int fieldCount = dataBag.count();
        for (int i = 0; i < fieldCount; i++)
        {
            dataField = dataBag.getByOffset(i);

            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<field name=\"%s\"", dataField.getName());
            if (dataField.isFeatureAssigned(Solr.FEATURE_SOLR_TYPE))
                aPW.printf(" type=\"%s\"", dataField.getFeature(Solr.FEATURE_SOLR_TYPE));
            else
                aPW.printf(" type=\"%s\"", mapFieldType(dataField));
            if (dataField.isFeatureAssigned(Solr.FEATURE_IS_INDEXED))
                aPW.printf(" indexed=\"%s\"", dataField.getFeature(Solr.FEATURE_IS_INDEXED));
            else
                aPW.printf(" indexed=\"true\"");
            if (dataField.isFeatureAssigned(Solr.FEATURE_IS_STORED))
                aPW.printf(" stored=\"%s\"", dataField.getFeature(Solr.FEATURE_IS_STORED));
            else
                aPW.printf(" stored=\"true\"");
            if (dataField.isFeatureTrue(Field.FEATURE_IS_REQUIRED))
                aPW.printf(" required=\"true\"");
            if (dataField.isMultiValue())
                aPW.printf(" multiValued=\"true\"");
            if (dataField.isFeatureTrue(Solr.FEATURE_IS_OMIT_NORMS))
                aPW.printf(" omitNorms=\"%s\"", dataField.getFeature(Solr.FEATURE_IS_OMIT_NORMS));
            if ((dataField.isFeatureTrue(Solr.FEATURE_IS_DEFAULT)) &&
                (StringUtils.isNotEmpty(dataField.getDefaultValue())))
                aPW.printf(" default=\"%s\"", dataField.getDefaultValue());
            aPW.printf("/>%n");
        }

        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("<field name=\"_version_\" type=\"long\" indexed=\"true\" stored=\"true\"/>%n");

        anIndentAmount--;
        IOXML.indentLine(aPW, anIndentAmount);
        aPW.printf("</%s>%n", aTagName);

        dataField = dataBag.getPrimaryKeyField();
        if (dataField != null)
        {
            IOXML.indentLine(aPW, anIndentAmount);
            aPW.printf("<uniqueKey>%s</uniqueKey>%n", dataField.getName());
        }

        String fieldName;
        for (int i = 0; i < fieldCount; i++)
        {
            dataField = dataBag.getByOffset(i);

            fieldName = dataField.getName();
            if ((StringUtils.endsWith(fieldName, "_name")) ||
                (StringUtils.endsWith(fieldName, "_title")) ||
                (StringUtils.endsWith(fieldName, "_description")) ||
                (StringUtils.endsWith(fieldName, "_content")))
            {
                IOXML.indentLine(aPW, anIndentAmount);
                aPW.printf("<copyField source=\"%s\" dest=\"text\"/>%n", fieldName);
            }
        }
    }

    public void save(PrintWriter aPW, int anIndentAmount)
        throws IOException
    {
        save(aPW, "fields", anIndentAmount);
    }

    public void save(PrintWriter aPW)
        throws IOException
    {
        save(aPW, 2);
    }

    public void save(String aPathFileName)
        throws IOException
    {
        PrintWriter printWriter = new PrintWriter(aPathFileName, "UTF-8");
        save(printWriter);
        printWriter.close();
    }

    private Field.Type mapSolrFieldType(String aSolrFieldType)
    {
        if (StringUtils.equalsIgnoreCase(aSolrFieldType, "int"))
            return Field.Type.Integer;
        else if (StringUtils.equalsIgnoreCase(aSolrFieldType, "long"))
            return Field.Type.Long;
        else if (StringUtils.equalsIgnoreCase(aSolrFieldType, "float"))
            return Field.Type.Float;
        else if (StringUtils.equalsIgnoreCase(aSolrFieldType, "double"))
            return Field.Type.Double;
        else if (StringUtils.equalsIgnoreCase(aSolrFieldType, "boolean"))
            return Field.Type.Boolean;
        else if ((StringUtils.equalsIgnoreCase(aSolrFieldType, "date")) ||
                 (StringUtils.equalsIgnoreCase(aSolrFieldType, "time")))
            return Field.Type.DateTime;
        else
            return Field.Type.Text;
    }

    private void assignSolrFieldFeature(DataField aField, String aName, String aValue)
    {
        if (StringUtils.equalsIgnoreCase(aName, "indexed"))
        {
            if (StrUtl.stringToBoolean(aValue))
                aField.enableFeature("isIndexed");
        }
        else if (StringUtils.equalsIgnoreCase(aName, "stored"))
        {
            if (StrUtl.stringToBoolean(aValue))
                aField.enableFeature("isStored");
        }
        else if (StringUtils.equalsIgnoreCase(aName, "multiValued"))
        {
            if (StrUtl.stringToBoolean(aValue))
                aField.setMultiValueFlag(true);
        }
        else
            aField.addFeature(aName, aValue);
    }

    private DataField loadField(Element anElement)
    {
        Attr nodeAttr;
        DataField dataField;
        Field.Type fieldType;
        String nodeName, nodeValue;
        Logger appLogger = mAppMgr.getLogger(this, "loadField");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String attrValue = anElement.getAttribute("name");
        if (StringUtils.isNotEmpty(attrValue))
        {
            String fieldName = attrValue;
            attrValue = anElement.getAttribute("type");
            if (StringUtils.isNotEmpty(attrValue))
                fieldType = mapSolrFieldType(attrValue);
            else
                fieldType = Field.Type.Text;
            dataField = new DataField(fieldType, fieldName);
            dataField.setTitle(Field.nameToTitle(fieldName));

            NamedNodeMap namedNodeMap = anElement.getAttributes();
            int attrCount = namedNodeMap.getLength();
            for (int attrOffset = 0; attrOffset < attrCount; attrOffset++)
            {
                nodeAttr = (Attr) namedNodeMap.item(attrOffset);
                nodeName = nodeAttr.getNodeName();
                nodeValue = nodeAttr.getNodeValue();

                if (StringUtils.isNotEmpty(nodeValue))
                {
                    if ((! StringUtils.equalsIgnoreCase(nodeName, "name")) &&
                        (! StringUtils.equalsIgnoreCase(nodeName, "type")))
                        assignSolrFieldFeature(dataField, nodeName, nodeValue);
                }
            }
        }
        else
            dataField = null;

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dataField;
    }

    private void loadFields(Document aDocument, Element anElement)
    {
        Node nodeItem;
        String nodeName;
        DataField dataField;
        Element nodeElement;
        Logger appLogger = mAppMgr.getLogger(this, "loadFields");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag dataBag = aDocument.getBag();
        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (StringUtils.equalsIgnoreCase(nodeName, "field"))
            {
                nodeElement = (Element) nodeItem;
                dataField = loadField(nodeElement);
                if (dataField != null)
                    dataBag.add(dataField);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private Document loadSchema(Element anElement)
        throws IOException
    {
        Node nodeItem;
        Document document;
        Element nodeElement;
        DataField dataField;
        String nodeName, nodeValue;
        Logger appLogger = mAppMgr.getLogger(this, "loadSchema");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String schemaName = anElement.getAttribute("name");
        document = new Document("Solr Schema");
        if (StringUtils.isNotEmpty(schemaName))
            document.setName(schemaName);

        DataBag dataBag = document.getBag();
        NodeList nodeList = anElement.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            nodeItem = nodeList.item(i);

            if (nodeItem.getNodeType() != Node.ELEMENT_NODE)
                continue;

            nodeName = nodeItem.getNodeName();
            if (StringUtils.equalsIgnoreCase(nodeName, "fields"))
            {
                nodeElement = (Element) nodeItem;
                loadFields(document, nodeElement);
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, "field"))
            {
                nodeElement = (Element) nodeItem;
                dataField = loadField(nodeElement);
                if (dataField != null)
                    dataBag.add(dataField);
            }
            else if (StringUtils.equalsIgnoreCase(nodeName, "uniqueKey"))
            {
                nodeValue = XMLUtl.getNodeStrValue(nodeItem);
                dataField = document.getBag().getFieldByName(nodeValue);
                if (dataField != null)
                    dataField.enableFeature(Field.FEATURE_IS_PRIMARY_KEY);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return document;
    }

    /**
     * Parses an XML DOM element and loads it into a document.
     *
     * @param anElement DOM element.
     * @throws java.io.IOException I/O related exception.
     */
    public void load(Element anElement)
        throws IOException
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mDocument = loadSchema(anElement);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Parses an XML DOM element and loads it into a document.
     *
     * @param anIS Input stream.
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     */
    public void load(InputStream anIS)
        throws ParserConfigurationException, IOException, SAXException
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
        InputSource inputSource = new InputSource(anIS);
        org.w3c.dom.Document xmlDocument = docBuilder.parse(inputSource);
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Parses an XML file identified by the path/file name parameter
     * and loads it into a document.
     *
     * @param aPathFileName Absolute file name.
     * @throws java.io.IOException                            I/O related exception.
     * @throws javax.xml.parsers.ParserConfigurationException XML parser related exception.
     * @throws org.xml.sax.SAXException                       XML parser related exception.
     */
    public void load(String aPathFileName)
        throws IOException, ParserConfigurationException, SAXException
    {
        Logger appLogger = mAppMgr.getLogger(this, "load");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        File xmlFile = new File(aPathFileName);
        if (! xmlFile.exists())
            throw new IOException(aPathFileName + ": Does not exist.");

        DocumentBuilderFactory docBldFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docBldFactory.newDocumentBuilder();
        org.w3c.dom.Document xmlDocument = docBuilder.parse(new File(aPathFileName));
        xmlDocument.getDocumentElement().normalize();

        load(xmlDocument.getDocumentElement());

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }


    /**
     * Downloads the Solr schema file identified via the URL parameter
     * and store it to the path/file name specified.  The Solr Dashboard
     * exposes the URL to a schema file.
     *
     * @param aURL Uniform Resource Location of schema file.
     * @param aPathFileName Path/Name where file should be stored.
     *
     * @throws NSException Thrown when I/O errors are detected.
     */
    public void downloadAndSave(String aURL, String aPathFileName)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "downloadAndSave");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((StringUtils.isNotEmpty(aURL))  && (StringUtils.isNotEmpty(aPathFileName)))
        {
            InputStream inputStream = null;
            OutputStream outputStream = null;
            CloseableHttpResponse httpResponse = null;
            File schemaFile = new File(aPathFileName);
            HttpGet httpGet = new HttpGet(aURL);
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
                String msgStr = String.format("%s (%s): %s", aURL, aPathFileName, e.getMessage());
                appLogger.error(msgStr, e);
                throw new NSException(msgStr);
            }
            finally
            {
                if (inputStream != null)
                    IOUtils.closeQuietly(inputStream);
                if (outputStream != null)
                    IOUtils.closeQuietly(outputStream);
                if (httpResponse != null)
                    IOUtils.closeQuietly(httpResponse);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
