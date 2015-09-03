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
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.slf4j.Logger;

import java.util.HashMap;

/**
 * The Pipeline is responsible for executing one or more
 * document transformations on behalf of a connector.
 *
 * @since 1.0
 * @author Al Cole
 */
public class Pipeline
{
    private final AppMgr mAppMgr;
    private HashMap<String, TransformInterface> mTransformers;
    private String mCfgPropertyPrefix = Connector.CFG_PROPERTY_PREFIX;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     */
    public Pipeline(final AppMgr anAppMgr, String aCfgPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        setCfgPropertyPrefix(aCfgPropertyPrefix);
        registerTransformers();
    }

    /**
     * Returns the configuration property prefix string.
     *
     * @return Property prefix string.
     */
    public String getCfgPropertyPrefix()
    {
        return mCfgPropertyPrefix;
    }

    /**
     * Assigns a configuration property prefix string.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgPropertyPrefix = aPropertyPrefix;
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

    private void registerTransformers()
    {
        Logger appLogger = mAppMgr.getLogger(this, "registerTransformers");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        mTransformers = new HashMap<String, TransformInterface>();

        mTransformers.put(Connector.TRANSFORMER_BAG_COPY, new TBagCopy(mAppMgr));
        mTransformers.put("pc_collapse", new TCPCCollapse(mAppMgr, getCfgPropertyPrefix()));
        mTransformers.put("pc_composite", new TParentChild(mAppMgr, getCfgPropertyPrefix()));
        mTransformers.put("content_clean", new TContentClean(mAppMgr));
        mTransformers.put("doc_type", new TDocType(mAppMgr, getCfgPropertyPrefix()));
        mTransformers.put("field_mapper", new TFieldMapper(mAppMgr, getCfgPropertyPrefix()));
        mTransformers.put("field_delete", new TFieldDelete(mAppMgr, getCfgPropertyPrefix()));
        mTransformers.put("field_collapse", new TFieldCollapse(mAppMgr, getCfgPropertyPrefix()));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Validates that the transformation feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void validate()
        throws NSException
    {
        TransformInterface transformInterface;
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = mCfgPropertyPrefix + ".transform.pipe_line";
        if (mAppMgr.isPropertyMultiValue(propertyName))
        {
            String[] transformerList = mAppMgr.getStringArray(propertyName);
            for (String transformerName : transformerList)
            {
                transformInterface = mTransformers.get(transformerName);
                if (transformInterface == null)
                {
                    String msgStr = String.format("%s: Unable to match pipeline transformer to class.", transformerName);
                    throw new NSException(msgStr);
                }
                else
                    transformInterface.validate();
            }
        }
        else
        {
            String transformerName = mAppMgr.getString(propertyName);
            transformInterface = mTransformers.get(transformerName);
            if (transformInterface == null)
            {
                String msgStr = String.format("%s: Unable to match pipeline transformer to class.", transformerName);
                throw new NSException(msgStr);
            }
            else
                transformInterface.validate();
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Executes a transformation against the source document instance
     * to produce a new destination document instance.
     *
     * @param aSrcDoc Source document instance.
     *
     * @return Destination document instance.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public Document execute(Document aSrcDoc)
        throws NSException
    {
        TransformInterface transformInterface;
        Logger appLogger = mAppMgr.getLogger(this, "execute");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Document dstDoc = null;
        Document srcDoc = aSrcDoc;
        String propertyName = mCfgPropertyPrefix + ".transform.pipe_line";
        if (mAppMgr.isPropertyMultiValue(propertyName))
        {
            String[] transformerList = mAppMgr.getStringArray(propertyName);
            for (String transformerName : transformerList)
            {
                transformInterface = mTransformers.get(transformerName);
                if (transformInterface == null)
                    transformInterface = mTransformers.get(Connector.TRANSFORMER_BAG_COPY);
                dstDoc = transformInterface.process(srcDoc);
                srcDoc = dstDoc;
            }
        }
        else
        {
            String transformerName = mAppMgr.getString(propertyName);
            transformInterface = mTransformers.get(transformerName);
            if (transformInterface == null)
                transformInterface = mTransformers.get(Connector.TRANSFORMER_BAG_COPY);
            dstDoc = transformInterface.process(srcDoc);
        }

        if (dstDoc == null)
        {
            transformInterface = mTransformers.get(Connector.TRANSFORMER_BAG_COPY);
            dstDoc = transformInterface.process(srcDoc);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dstDoc;
    }
}
