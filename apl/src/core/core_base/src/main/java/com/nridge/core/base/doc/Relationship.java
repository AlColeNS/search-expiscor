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
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.io.IO;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A Relationship establishes a connection among one or
 * more destination Documents.  A relationship can have
 * fields that describe the association and it is possible
 * to have a <i>null</i> or empty destination document.
 * Finally, the permissions for the relationship are handled
 * by the source document.
 * <p>
 * The class is a composite of the following:
 * </p>
 * <ul>
 *     <li>Fields {@link DataBag}</li>
 *     <li>Source Document (required)</li>
 *     <li>Destination Document (optional)</li>
 * </ul>
 *
 * @since 1.0
 * @author Al Cole
 */
public class Relationship
{
    private String mType;
    private DataBag mBag;
    private ArrayList<Document> mDocuments;
    private HashMap<String, String> mFeatures;
    private transient HashMap<String, Object> mProperties;

    /**
     * Default constructor
     */
    public Relationship()
    {
        instantiate(IO.XML_RELATIONSHIP_NODE_NAME, null);
    }

    /**
     * Constructor accepts a type parameter and initializes the Relationship
     * accordingly.
     *
     * @param aType Application defined type name.
     */
    public Relationship(String aType)
    {
        instantiate(aType, null);
    }

    /**
     * Constructor accepts a type and document parameter and initializes
     * the Relationship accordingly.
     *
     * @param aType Application defined type name.
     * @param aDocument Document instance.
     */
    public Relationship(String aType, Document aDocument)
    {
        instantiate(aType, null);
        add(aDocument);
    }

    /**
     * Constructor accepts a type and documents parameter and initializes
     * the Relationship accordingly.
     *
     * @param aType Application defined type name.
     * @param aDocuments Document instances.
     */
    public Relationship(String aType, Document ... aDocuments)
    {
        instantiate(aType, null);
        add(aDocuments);
    }

    /**
     * Constructor accepts a type and data bag instance parameter and initializes
     * the Relationship accordingly.
     *
     * @param aType Application defined type name.
     * @param aBag Data bag instance.
     */
    public Relationship(String aType, DataBag aBag)
    {
        instantiate(aType, aBag);
    }

    /**
     * Constructor accepts a type, data bag instance and document parameter
     * and initializes the Relationship accordingly.
     *
     * @param aType Application defined type name.
     * @param aBag Data bag instance.
     * @param aDocument Document instance.
     */
    public Relationship(String aType, DataBag aBag, Document aDocument)
    {
        instantiate(aType, aBag);
        add(aDocument);
    }

    /**
     * Constructor accepts a type, data bag instance and documents parameter
     * and initializes the Relationship accordingly.
     *
     * @param aType Application defined type name.
     * @param aBag Data bag instance.
     * @param aDocuments Document instances.
     */
    public Relationship(String aType, DataBag aBag, Document ... aDocuments)
    {
        instantiate(aType, aBag);
        add(aDocuments);
    }

    /**
     * Constructor clones an existing Relationship and its destination documents
     * (if assigned).
     *
     * @param aRelationship Relationship instance to clone.
     */
    public Relationship(Relationship aRelationship)
    {
        if (aRelationship != null)
        {
            mType = aRelationship.getType();
            mBag = new DataBag(aRelationship.getBag());
            mFeatures = new HashMap<String, String>(aRelationship.getFeatures());
            mDocuments = new ArrayList<Document>();
            if (aRelationship.count() > 0)
            {
                for (Document dstDocument : aRelationship.getDocuments())
                    mDocuments.add(new Document(dstDocument));
            }
        }
    }

    /**
     * Creates a new instance of the relationship based on the
     * one currently populated.  The new instance will contain
     * only those fields that have been populated from previous
     * assignments.  This can be helpful when you start with a
     * schema table containing many possible fields that never
     * end up getting populated in a load operation.
     *
     * @return Relationship instance.
     */
    public Relationship collapseUnusedFields()
    {
        Relationship newRelationship = new Relationship(getType());
        newRelationship.setBag(mBag.collapseUnusedFields());
        newRelationship.setFeatures(getFeatures());
        if (count() > 0)
        {
            for (Document curDocument : getDocuments())
                newRelationship.add(curDocument.collapseUnusedFields());
        }

        return newRelationship;
    }


    private DataBag createSchema()
    {
        mBag = new DataBag(IO.XML_RELATIONSHIP_NODE_NAME);
        DataTextField dataTextField = new DataTextField("nsd_id", "NSD Id");
        dataTextField.enableFeature(Field.FEATURE_IS_REQUIRED);
        dataTextField.enableFeature(Field.FEATURE_IS_PRIMARY_KEY);
        mBag.add(dataTextField);
        dataTextField = new DataTextField("nsd_related_src_id", "NSD Related Source Id");
        dataTextField.enableFeature(Field.FEATURE_IS_REQUIRED);
        dataTextField.enableFeature(Field.FEATURE_IS_RELATED_SRC_KEY);
        mBag.add(dataTextField);
        dataTextField = new DataTextField("nsd_related_dst_id", "NSD Related Destination Id");
        dataTextField.enableFeature(Field.FEATURE_IS_RELATED_DST_KEY);
        mBag.add(dataTextField);

        return mBag;
    }

    private void instantiate(String aType, DataBag aBag)
    {
        mType = aType;
        if (aBag == null)
            mBag = createSchema();
        else
            mBag = aBag;

        mDocuments = new ArrayList<Document>();
        mFeatures = new HashMap<String, String>();
    }

    /**
     * Returns a string representation of a Relationship.
     *
     * @return String summary representation of this Relationship.
     */
    @Override
    public String toString()
    {
        String msgStr = String.format("%s %s", mType, mBag.toString());
        int count = mDocuments.size();
        if (count > 0)
            msgStr += String.format(" - [%d docs]", count);

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
     * Returns the {@link DataBag} representation of the fields
     * defined for the document.
     *
     * @return Data bag representation of the document properties.
     */
    public final DataBag getBag()
    {
        return mBag;
    }

    /**
     * Assigns a {@link DataBag} of fields to the document.
     *
     * @param aBag Relationship fields.
     */
    public void setBag(final DataBag aBag)
    {
        if (aBag != null)
            mBag = new DataBag(aBag);
    }

    /**
     * Returns the count of related documents.
     *
     * @return Count of documents.
     */
    public int count()
    {
        return mDocuments.size();
    }

    /**
     * Returns a collection of related destination documents.
     *
     * @return Collection of related destination documents.
     */
    public ArrayList<Document> getDocuments()
    {
        return mDocuments;
    }

    /**
     * Convenience method that will return the first document
     * for the relationship.
     *
     * @return Document instance or <i>null</i> if none exists.
     */
    public Document getFirstDocument()
    {
        if (mDocuments.size() > 0)
            return mDocuments.get(0);
        else
            return null;
    }

    /**
     * Adds a relationship document to the list of destination documents.
     *
     * @param aDocument Document instance.
     */
    public void add(Document aDocument)
    {
        mDocuments.add(aDocument);
    }

    /**
     * Adds a relationship documents to the list of destination documents.
     *
     * @param aDocuments Documents instance.
     */
    public void add(Document ... aDocuments)
    {
        for (Document relatedDocument : aDocuments)
            add(relatedDocument);
    }

    /**
     * Empties the relationship of any assigned values.
     */
    public void reset()
    {
        mBag.resetValues();
        mDocuments.clear();
    }

    /**
     * Removes a relationship document from the list of destination documents.
     *
     * @param aDocument Document instance.
     */
    public void remove(Document aDocument)
    {
        mDocuments.remove(aDocument);
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
