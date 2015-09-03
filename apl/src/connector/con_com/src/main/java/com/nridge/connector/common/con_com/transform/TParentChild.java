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

package com.nridge.connector.common.con_com.transform;

import com.nridge.connector.common.con_com.Connector;
import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;

/**
 * The TParentChild document transformer collapses multi-level
 * relationships down to one level.
 * <p>Parent and child documents that could include
 * multi-level relationships collapsed to the child.
 * For example: "Document-Document File-File" would
 * become "Document-File".</p>
 * <p>Note:This logic will (by design) remove
 * any relationships from a document that does not
 * match document types in the configuration file.</p>
 *
 * @since 1.0
 * @author Al Cole
 */
public class TParentChild implements TransformInterface
{
    private final AppMgr mAppMgr;
    private PropertiesConfiguration mCfgProperties;
    private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     */
    public TParentChild(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
    }

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     * @param aCfgPropertyPrefix Property prefix.
     */
    public TParentChild(final AppMgr anAppMgr, String aCfgPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        mCfgPropertyPrefix = aCfgPropertyPrefix + ".transform";
    }

    /**
     * Convenience method that returns the value of a property using
     * the concatenation of the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     * @return Matching property value.
     */
    private String getCfgString(String aSuffix)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of a property using
     * the concatenation of the property prefix and suffix values.
     * If the property is not found, then the default value parameter
     * will be returned.
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    private String getCfgString(String aSuffix, String aDefaultValue)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    private int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        String propertyName;

        if (org.apache.commons.lang.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the a property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    private boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    /**
     * Validates that the transformation feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws com.nridge.core.base.std.NSException Indicates a configuration issue.
     */
    @Override
    public void validate()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = "parent_child_file";
        String parentChildFileName = getCfgString(propertyName);
        if (StringUtils.isEmpty(parentChildFileName))
            throw new NSException(String.format("Property '%s' is undefined.", propertyName));

        String parentChildPathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                        File.separatorChar, parentChildFileName);
        File parentChildFile = new File(parentChildPathFileName);
        if (! parentChildFile.exists())
            throw new NSException(String.format("%s: Does not exist.", parentChildPathFileName));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private DataBag collapse(DataBag aParentBag, DataBag aChildBag)
    {
        DataField collapseField;
        Logger appLogger = mAppMgr.getLogger(this, "collapse");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag collapseBag = new DataBag(aChildBag);
        for (DataField parentField : aParentBag.getFields())
        {
            collapseField = collapseBag.getFieldByName(parentField.getName());
            if (collapseField == null)
                collapseBag.add(new DataField(parentField));
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return collapseBag;
    }

    private boolean isMatch(String[] aRelPCArray, String aType, int aLevel)
    {
        int levelCount;
        String[] pcLevelArray;

        for (String pcString : aRelPCArray)
        {
            levelCount = StringUtils.countMatches(pcString, "/");
            if ((aLevel == 0) && (levelCount == 0))
            {
                if (StringUtils.equals(aType, pcString))
                    return true;
            }
            else
            {
                pcLevelArray = pcString.split("/");
                if (pcLevelArray.length > aLevel)
                {
                    if (StringUtils.equals(aType, pcLevelArray[aLevel]))
                        return true;
                }
            }
        }

        return false;
    }

    private void transform(Document aDocument)
    {
        String[] propertyValueArray;
        DataBag docBag, parentBag, collapseBag;
        String relType, relDocType, colDocType;
        DataField dfRelType, dfDocViewACL, dfRelViewACL;

        Logger appLogger = mAppMgr.getLogger(this, "transform");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aDocument != null)
        {
            ArrayList<Relationship> relationshipList = new ArrayList<Relationship>();

            docBag = aDocument.getBag();
            String docType = aDocument.getType();
            if (StringUtils.isNotEmpty(docType))
            {
                String propertyValue = mCfgProperties.getString(docType);
                if (StringUtils.isNotEmpty(propertyValue))
                {
                    int relLevel = 0;
                    propertyValueArray = mCfgProperties.getStringArray(docType);
                    ArrayList<Relationship> parentRelationshipList = aDocument.getRelationships();
                    for (Relationship parentRelationship : parentRelationshipList)
                    {
                        relType = parentRelationship.getType();
                        if (isMatch(propertyValueArray, relType, relLevel))
                        {
                            collapseBag = null;
                            colDocType = relType;
                            parentBag = parentRelationship.getBag();
                            for (Document document : parentRelationship.getDocuments())
                            {
                                relDocType = document.getType();
                                if (isMatch(propertyValueArray, relDocType, relLevel+1))
                                {
                                    colDocType = relDocType;
                                    collapseBag = collapse(parentBag, document.getBag());
                                }
                                document.setRelationships(new ArrayList<Relationship>());
                            }
                            if (collapseBag == null)
                                collapseBag = parentBag;
                            if (collapseBag.isValid())
                            {
                                dfRelType = collapseBag.getFieldByName("nsd_rel_type");
                                if (dfRelType == null)
                                {
                                    dfRelType = new DataTextField("nsd_rel_type", "NSD Relationship Type");
                                    collapseBag.add(dfRelType);
                                }
                                dfRelType.setValue(relType);
                                collapseBag.setValueByName("nsd_doc_type", colDocType);

// Assign the ACL of the parent to the collapsed child (NSD policy logic for now).

                                dfDocViewACL = docBag.getFieldByName("nsd_acl_view");
                                if (dfDocViewACL != null)
                                {
                                    dfRelViewACL = collapseBag.getFieldByName("nsd_acl_view");
                                    if (dfRelViewACL == null)
                                    {
                                        dfRelViewACL = new DataTextField("nsd_acl_view", "NSD ACL View");
                                        dfRelViewACL.setMultiValueFlag(true);
                                        collapseBag.add(dfRelViewACL);
                                        dfRelViewACL.addValues(dfDocViewACL.getValues());
                                    }
                                }
                                relationshipList.add(new Relationship(colDocType, collapseBag));
                            }
                        }
                    }
                }
            }
            aDocument.setRelationships(relationshipList);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Applies a transformation against the source document to produce
     * the returned destination document.
     *
     * @param aSrcDoc Source document instance.
     * @return New destination document instance.
     * @throws com.nridge.core.base.std.NSException Indicates a data validation error condition.
     */
    @Override
    public Document process(Document aSrcDoc)
        throws NSException
    {
        DataBag relBag;
        Document dstDoc;
        DataField dfParentId;
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aSrcDoc == null)
            throw new NSException("Source document is null.");

        if (mCfgProperties == null)
        {
            String parentChildFileName = getCfgString("parent_child_file");
            if (StringUtils.isNotEmpty(parentChildFileName))
            {
                String parentChildPathFileName = String.format("%s%c%s", mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH),
                                                                File.separatorChar, parentChildFileName);
                File parentChildFile = new File(parentChildPathFileName);
                if (parentChildFile.exists())
                {
                    try
                    {
                        mCfgProperties = new PropertiesConfiguration();
                        mCfgProperties.setFileName(parentChildPathFileName);
                        mCfgProperties.setIOFactory(new WhitespaceIOFactory());
                        mCfgProperties.load();
                    }
                    catch (ConfigurationException e)
                    {
                        mCfgProperties = null;
                        String msgStr = String.format("%s: %s", parentChildPathFileName, e.getMessage());
                        throw new NSException(msgStr);
                    }
                }
            }
        }

// Clone our source document.

        dstDoc = new Document(aSrcDoc);

// Execute the transformation.

        if (mCfgProperties != null)
            transform(dstDoc);

// Assign our parent ids now.

        DataBag dstBag = dstDoc.getBag();
        DataField dfIsParent = dstBag.getFieldByName("nsd_is_parent");
        String nsdId = dstBag.getValueAsString("nsd_id");
        if (StringUtils.isNotEmpty(nsdId))
        {
            ArrayList<Relationship> relationshipList = dstDoc.getRelationships();
            for (Relationship relationship : relationshipList)
            {
                relBag = relationship.getBag();
                dfParentId = relBag.getFieldByName("nsd_parent_id");
                if (dfParentId == null)
                {
                    dfParentId = new DataTextField("nsd_parent_id", "NSD Parent Id");
                    dfParentId.setMultiValueFlag(true);
                    relBag.add(dfParentId);
                }
                dfParentId.addValueUnique(nsdId);
                if (dfIsParent != null)
                {
                    if (dfIsParent.isValueFalse())
                        dfIsParent.setValue(true);
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dstDoc;
    }
}
