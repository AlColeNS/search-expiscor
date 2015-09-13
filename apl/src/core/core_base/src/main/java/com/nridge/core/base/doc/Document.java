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

package com.nridge.core.base.doc;

import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataTable;

import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.DigitalHash;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A Document is designed to model a document that might
 * be stored in a Search Index or an Enterprise Content
 * Management System (ECM).
 * <p>
 * The class is a composite of the following:
 * </p>
 * <ul>
 *     <li>Fields {@link DataBag}</li>
 *     <li>Rows {@link DataTable}</li>
 *     <li>Typed Relationships</li>
 *     <li>ACLs</li>
 * </ul>
 *
 * @since 1.0
 * @author Al Cole
 */
public class Document
{
    private String mType;
    private DataTable mTable;
    private int mSchemaVersion;
    private HashMap<String,String> mACL;
    private HashMap<String, String> mFeatures;
    private ArrayList<Relationship> mRelationships;
    private transient HashMap<String, Object> mProperties;

    /**
     * Constructor accepts a type parameter and initializes the Document
     * accordingly.
     *
     * @param aType Application defined type name.
     */
    public Document(String aType)
    {
        instantiate(aType, null, null);
    }

    /**
     * Constructor accepts a type and data bag instance parameter and initializes
     * the Document accordingly.
     *
     * @param aType Application defined type name.
     * @param aBag Data bag instance.
     */
    public Document(String aType, DataBag aBag)
    {
        instantiate(aType, aBag, null);
    }

    /**
     * Constructor accepts a type and data table  parameter
     * and initializes the Document accordingly.
     *
     * @param aType Application defined type name.
     * @param aTable Data table instance.
     */
    public Document(String aType, DataTable aTable)
    {
        instantiate(aType, null, aTable);
    }

    /**
     * Constructor clones an existing Document.
     *
     * @param aDocument Source document instance to clone.
     */
    public Document(Document aDocument)
    {
        if (aDocument != null)
        {
            setTable(new DataTable(aDocument.getTable()));

            setType(aDocument.getType());
            setName(aDocument.getName());
            setTitle(aDocument.getTitle());
            setSchemaVersion(aDocument.getSchemaVersion());
            mRelationships = new ArrayList<Relationship>();
            if (aDocument.getRelationships().size() > 0)
            {
                for (Relationship relationship : aDocument.getRelationships())
                    this.addRelationship(new Relationship(relationship));
            }
            mACL = new HashMap<String, String>(aDocument.getACL());
            mFeatures = new HashMap<String, String>(aDocument.getFeatures());
        }
    }

    /**
     * Creates a new instance of the document based on the
     * one currently populated.  The new instance will contain
     * only those fields that have been populated from previous
     * assignments.  This can be helpful when you start with a
     * schema table containing many possible fields that never
     * end up getting populated in a load operation.
     *
     * @return Document instance.
     */
    public Document collapseUnusedFields()
    {
        Document newDoc;

        newDoc = new Document(getType());
        String docName = getName();
        if (StringUtils.isNotEmpty(docName))
            newDoc.setName(docName);
        newDoc.setFeatures(getFeatures());
        newDoc.setTable(getTable().collapseUnusedColumns());
        for (Relationship curRelationship : mRelationships)
            newDoc.addRelationship(curRelationship.collapseUnusedFields());

        return newDoc;
    }

    private void instantiate(String aType, DataBag aBag, DataTable aTable)
    {
        mType = aType;
        mSchemaVersion = Doc.SCHEMA_VERSION_DEFAULT;
        if (aTable != null)
            mTable = new DataTable(aTable);
        else if (aBag != null)
            mTable = new DataTable(aBag);
        else
            mTable = new DataTable(aType);

        if (StringUtils.isEmpty(getTitle()))
            setTitle(aType);

        mACL = new HashMap<String, String>();           // Example: Name, Permission (R W U D)
        mFeatures = new HashMap<String, String>();
        mRelationships = new ArrayList<Relationship>();
    }

    /**
     * Returns a string representation of a Document.
     *
     * @return String summary representation of this Document.
     */
    @Override
    public String toString()
    {
        String msgStr = mTable.getColumnBag().toString();
        int count = mTable.rowCount();
        if (count > 0)
            msgStr += String.format(" - [%d rows]", count);
        count = mRelationships.size();
        if (count > 0)
            msgStr += String.format(" - [%d rels]", count);
        count = mACL.size();
        if (count > 0)
            msgStr += String.format(" - [%d acls]", count);

        return msgStr;
    }

    /**
     * Returns the application defined document type.
     *
     * @return Application defined type name.
     */
    public String getType()
    {
        return mType;
    }

    /**
     * Assigns the application defined document type.
     *
     * @param aType Application defined type name.
     */
    public void setType(String aType)
    {
        mType = aType;
    }

    /**
     * Returns the application defined schema version number.
     *
     * @return Schema version number.
     */
    public int getSchemaVersion()
    {
        return mSchemaVersion;
    }

    /**
     * Assigns an application defined schema version number.
     *
     * @param aSchemaVersion Schema version number.
     */
    public void setSchemaVersion(int aSchemaVersion)
    {
        mSchemaVersion = aSchemaVersion;
    }

    /**
     * Returns the name of the document.
     *
     * @return Name of the document.
     */
    public String getName()
    {
        return mTable.getColumnBag().getName();
    }

    /**
     * Assigns the document name.
     *
     * @param aName Document name.
     */
    public void setName(String aName)
    {
        mTable.getColumnBag().setName(aName);
    }

    /**
     * Returns the document title.
     *
     * @return Document title.
     */
    public String getTitle()
    {
        return mTable.getColumnBag().getTitle();
    }

    /**
     * Assigns the document title.
     *
     * @param aTitle Document title.
     */
    public void setTitle(String aTitle)
    {
        mTable.getColumnBag().setTitle(aTitle);
    }

    /**
     * Returns the {@link DataBag} representation of the fields
     * defined for the document.
     *
     * @return Data bag representation of the document fields.
     */
    public DataBag getBag()
    {
        return mTable.getColumnBag();
    }

    /**
     * Assigns a {@link DataBag} of fields to the document.
     *
     * @param aBag Document fields.
     */
    public void setBag(final DataBag aBag)
    {
        if (aBag != null)
            mTable = new DataTable(aBag);
    }

    /**
     * Returns the {@link DataTable} representation of the rows
     * defined for the document.
     *
     * @return Data bag representation of the document fields.
     */
    public DataTable getTable()
    {
        return mTable;
    }

    /**
     * Assigns a {@link DataTable} of rows to the document.
     *
     * @param aTable Table instance.
     */
    public void setTable(final DataTable aTable)
    {
        if (aTable != null)
            mTable = new DataTable(aTable);
    }

    /**
     * Adds the document instance to the related documents.
     *
     * @param aRelationship Relationship instance.
     */
    public void addRelationship(Relationship aRelationship)
    {
        if (aRelationship != null)
            mRelationships.add(aRelationship);
    }

    /**
     * Adds a relationship document of a type specified to
     * the document.
     * <p>
     * <b>Note:</b> This method will examine the list of existing
     * relationships to determine if one already exists of the
     * type specified.  If it does, then this document will be
     * added to existing list of the matching relationship.
     * </p>
     *
     * @param aType Relationship type.
     * @param aDocument Destination document instance.
     */
    public void addRelationship(String aType, Document aDocument)
    {
        boolean isAdded = false;
        for (Relationship docRelationship : mRelationships)
        {
            if (docRelationship.getType().equals(aType))
            {
                docRelationship.add(aDocument);
                isAdded = true;
                break;
            }

        }
        if (! isAdded)
            mRelationships.add(new Relationship(aType, aDocument));
    }

    /**
     * Adds multiple relationship documents of a type specified to
     * the document.
     * <p>
     * <b>Note:</b> This method will examine the list of existing
     * relationships to determine if one already exists of the
     * type specified.  If it does, then this document will be
     * added to existing list of the matching relationship.
     * </p>
     *
     * @param aType Relationship type.
     * @param aDocuments Destination document instances.
     */
    public void addRelationship(String aType, Document ... aDocuments)
    {
        boolean isAdded = false;
        for (Relationship docRelationship : mRelationships)
        {
            if (docRelationship.getType().equals(aType))
            {
                docRelationship.add(aDocuments);
                isAdded = true;
                break;
            }

        }
        if (! isAdded)
            mRelationships.add(new Relationship(aType, aDocuments));
    }

    /**
     * Adds a relationship document of a type specified to
     * the document.  In addition, the relationship will be
     * assigned the bag of fields specified.
     * <p>
     * <b>Note:</b> All relationships added this way will
     * be added to the list as unique instances.  No attempt
     * will be made to match the relationship to an existing
     * type.
     * </p>
     *
     * @param aType Relationship type.
     * @param aBag Data fields.
     * @param aDocument Destination document.
     */
    public void addRelationship(String aType, DataBag aBag, Document aDocument)
    {
        mRelationships.add(new Relationship(aType, aBag, aDocument));
    }

    /**
     * Adds multiple relationship documents of a type specified to
     * the document.  In addition, the relationship will be
     * assigned the bag of fields specified.
     * <p>
     * <b>Note:</b> All relationships added this way will
     * be added to the list as unique instances.  No attempt
     * will be made to match the relationship to an existing
     * type.
     * </p>
     *
     * @param aType Relationship type.
     * @param aBag Data fields.
     * @param aDocuments Destination documents.
     */
    public void addRelationship(String aType, DataBag aBag, Document ... aDocuments)
    {
        mRelationships.add(new Relationship(aType, aBag, aDocuments));
    }

    /**
     * Returns the count of related documents.
     *
     * @return Count of documents.
     */
    public int relationshipCount()
    {
        return mRelationships.size();
    }

    /**
     * Assigns a list of relationships to the document.
     *
     * @param aRelationships Relationship list.
     */
    public void setRelationships(ArrayList<Relationship> aRelationships)
    {
        if (aRelationships != null)
            mRelationships = aRelationships;
    }

    /**
     * Returns a collection of relationships associated with this
     * document.
     *
     * @return Collection of all relationships.
     */
    public ArrayList<Relationship> getRelationships()
    {
        return mRelationships;
    }

    /**
     * Returns a collection of relationships associated with this
     * document that matches the type.
     *
     * @param aType Type of relationship.
     *
     * @return List of relationships matching the type parameter.
     */
    public ArrayList<Relationship> getRelationships(String aType)
    {
        ArrayList<Relationship> relationshipList = new ArrayList<Relationship>();
        for (Relationship relationship : mRelationships)
        {
            if (relationship.getType().equals(aType))
                relationshipList.add(relationship);
        }

        return relationshipList;
    }

    /**
     * Convenience method that will return the first relationship
     * that matches the relationship type.
     *
     * @param aType Type of relationship.
     *
     * @return Relationship instance or <i>null</i> if none match.
     */
    public Relationship getFirstRelationship(String aType)
    {
        ArrayList<Relationship> relationshipList = getRelationships(aType);
        if (relationshipList.size() > 0)
            return relationshipList.get(0);
        else
            return null;
    }

    /**
     * Returns a collection of relationship properties (data bag)
     * matching the type parameter.
     *
     * @param aType Type of relationship.
     *
     * @return List of relationship properties (data bags).
     */
    public ArrayList<DataBag> getRelatedProperties(String aType)
    {
        ArrayList<DataBag> dataBagList = new ArrayList<>();
        ArrayList<Relationship> relationshipList = getRelationships(aType);
        for (Relationship relationship : relationshipList)
            dataBagList.add(relationship.getBag());

        return dataBagList;
    }

    /**
     * Convenience method that will return the properties (data bag)
     * from the first matching relationship type.
     *
     * @param aType Type of relationship.
     *
     * @return Relationship properties (data bag) or <i>null</i> if none match.
     */
    public DataBag getFirstRelatedProperties(String aType)
    {
        ArrayList<DataBag> dataBagList = getRelatedProperties(aType);
        if (dataBagList.size() > 0)
            return dataBagList.get(0);
        else
            return null;
    }

    /**
     * Returns a collection of related documents that match the
     * relationship type parameter.  If the type is matched with
     * more than one relationship, then the list will be a union
     * of those related documents.
     *
     * @param aType Type of relationship.
     *
     * @return List of related documents.
     */
    public ArrayList<Document> getRelatedDocuments(String aType)
    {
        ArrayList<Document> documentList = new ArrayList<>();
        ArrayList<Relationship> relationshipList = getRelationships(aType);
        for (Relationship relationship : relationshipList)
        {
            for (Document document : relationship.getDocuments())
                documentList.add(document);
        }

        return documentList;
    }

    /**
     * Convenience method that will return the first document within
     * the relationship that matches the type parameter.
     *
     * @param aType Type of relationship.
     *
     * @return Document instance or or <i>null</i> if none match.
     */
    public Document getFirstRelatedDocument(String aType)
    {
        ArrayList<Document> documentList = getRelatedDocuments(aType);
        if (documentList.size() > 0)
            return documentList.get(0);
        else
            return null;
    }

    /**
     * Returns a map of an ACL for the document.
     *
     * @return Map of document ACL.
     */
    public HashMap<String, String> getACL()
    {
        return mACL;
    }

    /**
     * Assigns a map of an ACL to the document.
     *
     * @param anACL Document ACL.
     */
    public void setACL(HashMap<String, String> anACL)
    {
        mACL = anACL;
    }

    /**
     * Returns a {@link DataTable} representation of the document ACL.
     *
     * @return User ACL table.
     */
    public DataTable getACLTable()
    {
        DataTable persistTable = new DataTable(mType + "_acls");
        persistTable.add(new DataTextField(Doc.FIELD_ACL_NAME, Doc.FIELD_ACL_TITLE));
        persistTable.add(new DataTextField(Doc.FIELD_PERMISSION_NAME, Doc.FIELD_PERMISSION_TITLE));

        Map<String, String> aclMap = mACL;
        for (Map.Entry<String, String> aclEntry : aclMap.entrySet())
        {
            persistTable.newRow();
            persistTable.setValueByName(Doc.FIELD_ACL_NAME, aclEntry.getKey());
            persistTable.setValueByName(Doc.FIELD_PERMISSION_NAME, aclEntry.getValue());
            persistTable.addRow();
        }

        return persistTable;
    }

    /**
     * Process the document information (properties, relationships)
     * through the digital hash algorithm.
     *
     * @param aHash Digital hash instance.
     * @param anIsFeatureIncluded Should features be included.
     *
     * @throws IOException Triggered by hash algorithm.
     */
    public void processHash(DigitalHash aHash, boolean anIsFeatureIncluded)
        throws IOException
    {
        DataBag relBag;

        aHash.processBuffer(getType());
        aHash.processBuffer(getName());
        if (anIsFeatureIncluded)
        {
            for (Map.Entry<String, String> featureEntry : getFeatures().entrySet())
            {
                aHash.processBuffer(featureEntry.getKey());
                aHash.processBuffer(featureEntry.getValue());
            }
        }
        mTable.processHash(aHash, anIsFeatureIncluded);
        for (Relationship relationship : mRelationships)
        {
            relBag = relationship.getBag();
            relBag.processHash(aHash, anIsFeatureIncluded);
            for (Document document : relationship.getDocuments())
                document.processHash(aHash, anIsFeatureIncluded);
        }
    }

    /**
     * Generates a unique hash string using the MD5 algorithm using
     * the document information.
     *
     * @param anIsFeatureIncluded Should feature name/values be included?
     *
     * @return Unique hash string.
     */
    public String generateUniqueHash(boolean anIsFeatureIncluded)
    {
        String hashId;

        DigitalHash digitalHash = new DigitalHash();
        try
        {
            processHash(digitalHash, anIsFeatureIncluded);
            hashId = digitalHash.getHashSequence();
        }
        catch (IOException e)
        {
            UUID uniqueId = UUID.randomUUID();
            hashId = uniqueId.toString();
        }

        return hashId;
    }

    /**
     * Empties the document of any assigned values (fields, acl, related).
     */
    public void reset()
    {
        mACL.clear();
        mFeatures.clear();
        mTable.emptyRows();
        mRelationships.clear();
        mTable.getColumnBag().resetValues();
    }

    /**
     * Convenience method examines all of the fields in the document to determine if
     * they are valid. A validation check ensures values are assigned when required
     * and do not exceed range limits (if assigned).
     * <p>
     * <b>Note:</b> If a field fails the validation check, then a property called
     * <i>Field.VALIDATION_PROPERTY_NAME</i> will be assigned a relevant message.
     * </p>
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isValid()
    {
        return getBag().isValid();
    }

    /**
     * Add a unique feature to this document.  A feature enhances the core
     * capability of the document.  Standard features are listed below.
     * <ul>
     *     <li>Field.FEATURE_OPERATION_NAME</li>
     * </ul>
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, String aValue)
    {
        mFeatures.put(aName, aValue);
    }

    /**
     * Add a unique feature to this document.  A feature enhances the core
     * capability of the document.
     *
     * @param aName Name of the feature.
     * @param aValue Value to associate with the feature.
     */
    public void addFeature(String aName, int aValue)
    {
        addFeature(aName, Integer.toString(aValue));
    }

    /**
     * Enabling the feature will add the name and assign it a
     * value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of the feature.
     */
    public void enableFeature(String aName)
    {
        mFeatures.put(aName, StrUtl.STRING_TRUE);
    }

    /**
     * Disabling a feature will remove its name and value
     * from the internal list.
     *
     * @param aName Name of feature.
     */
    public void disableFeature(String aName)
    {
        mFeatures.remove(aName);
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and assigned a value.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureAssigned(String aName)
    {
        return (getFeature(aName) != null);
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and assigned a value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureTrue(String aName)
    {
        return StrUtl.stringToBoolean(mFeatures.get(aName));
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and not assigned a value of <i>StrUtl.STRING_TRUE</i>.
     *
     * @param aName Name of feature.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureFalse(String aName)
    {
        return !StrUtl.stringToBoolean(mFeatures.get(aName));
    }

    /**
     * Returns <i>true</i> if the feature was previously
     * added and its value matches the one provided as a
     * parameter.
     *
     * @param aName Feature name.
     * @param aValue Feature value to match.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isFeatureEqual(String aName, String aValue)
    {
        String featureValue = getFeature(aName);
        return StringUtils.equalsIgnoreCase(featureValue, aValue);
    }

    /**
     * Count of unique features assigned to this document.
     *
     * @return Feature count.
     */
    public int featureCount()
    {
        return mFeatures.size();
    }

    /**
     * Returns the String associated with the feature name or
     * <i>null</i> if the name could not be found.
     *
     * @param aName Feature name.
     *
     * @return Feature value or <i>null</i>
     */
    public String getFeature(String aName)
    {
        return mFeatures.get(aName);
    }

    /**
     * Returns the int associated with the feature name.
     *
     * @param aName Feature name.
     *
     * @return Feature value or <i>null</i>
     */
    public int getFeatureAsInt(String aName)
    {
        return Field.createInt(getFeature(aName));
    }

    /**
     * Removes all features assigned to this object instance.
     */
    public void clearFeatures()
    {
        mFeatures.clear();
    }

    /**
     * Assigns the hash map of features to the list.
     *
     * @param aFeatures Feature list.
     */
    public void setFeatures(HashMap<String, String> aFeatures)
    {
        if (aFeatures != null)
            mFeatures = new HashMap<String, String>(aFeatures);
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param anObject Reference object with which to compare.
     * @return  {@code true} if this object is the same as the anObject
     *          argument; {@code false} otherwise.
     */
    @Override
    public boolean equals(Object anObject)
    {
        if (this == anObject)
            return true;
        if (anObject == null || getClass() != anObject.getClass())
            return false;

        Document document = (Document) anObject;

        if (mSchemaVersion != document.mSchemaVersion)
            return false;
        if (mType != null ? !mType.equals(document.mType) : document.mType != null)
            return false;
        if (mTable != null ? !mTable.equals(document.mTable) : document.mTable != null)
            return false;
        if (mACL != null ? !mACL.equals(document.mACL) : document.mACL != null)
            return false;
        if (mFeatures != null ? !mFeatures.equals(document.mFeatures) : document.mFeatures != null)
            return false;

        return !(mRelationships != null ? !mRelationships.equals(document.mRelationships) : document.mRelationships != null);
    }

    /**
     * Returns a hash code value for the object. This method is
     * supported for the benefit of hash tables such as those provided by
     * {@link java.util.HashMap}.
     *
     * @return A hash code value for this object.
     */
    @Override
    public int hashCode()
    {
        int result = mType != null ? mType.hashCode() : 0;
        result = 31 * result + (mTable != null ? mTable.hashCode() : 0);
        result = 31 * result + mSchemaVersion;
        result = 31 * result + (mACL != null ? mACL.hashCode() : 0);
        result = 31 * result + (mFeatures != null ? mFeatures.hashCode() : 0);
        result = 31 * result + (mRelationships != null ? mRelationships.hashCode() : 0);

        return result;
    }

    /**
     * Returns a read-only copy of the internal map containing
     * feature list.
     *
     * @return Internal feature map instance.
     */
    public final HashMap<String, String> getFeatures()
    {
        return mFeatures;
    }

    /**
     * Add an application defined property to the document.
     * <b>Notes:</b>
     * <ul>
     *     <li>The goal of the DataBag is to strike a balance between
     *     providing enough properties to adequately model application
     *     related data without overloading it.</li>
     *     <li>This method offers a mechanism to capture additional
     *     (application specific) properties that may be needed.</li>
     *     <li>Properties added with this method are transient and
     *     will not be stored when saved.</li>
     * </ul>
     *
     * @param aName Property name (duplicates are not supported).
     * @param anObject Instance of an object.
     */
    public void addProperty(String aName, Object anObject)
    {
        if (mProperties == null)
            mProperties = new HashMap<String, Object>();
        mProperties.put(aName, anObject);
    }

    /**
     * Returns the object associated with the property name or
     * <i>null</i> if the name could not be matched.
     *
     * @param aName Name of the property.
     * @return Instance of an object.
     */
    public Object getProperty(String aName)
    {
        if (mProperties == null)
            return null;
        else
            return mProperties.get(aName);
    }

    /**
     * Removes all application defined properties assigned to this bag.
     */
    public void clearBagProperties()
    {
        if (mProperties != null)
            mProperties.clear();
    }
}
