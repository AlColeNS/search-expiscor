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
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

/**
 * The SolrTransform provides a collection of methods that can transform
 * a Solr {@link Document} instance.
 *
 * @author Al Cole
 * @since 1.0
 */
public class SolrTransform
{
    private final AppMgr mAppMgr;
    private String mCfgPropertyPrefix = StringUtils.EMPTY;

    public SolrTransform(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        setCfgPropertyPrefix(Solr.CFG_PROPERTY_PREFIX);
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
     * Assigns the configuration property prefix to the document data source.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     *
     * @return Matching property value.
     */
    public String getCfgString(String aSuffix)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     *
     * @return Matching property values in a string array.
     */
    public String[] getCfgStrings(String aSuffix)
    {
        String[] cfgValues;
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        if (mAppMgr.isPropertyMultiValue(propertyName))
            cfgValues = mAppMgr.getStringArray(propertyName);
        else
        {
            cfgValues = new String[1];
            String cfgValue = mAppMgr.getString(propertyName);
            if (StringUtils.isEmpty(cfgValue))
                cfgValue = StringUtils.EMPTY;
            cfgValues[0] = cfgValue;
        }

        return cfgValues;
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
    public int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the application manager configuration
     * property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    /**
     * Applies the limits to the content fields found in the response
     * table.
     *
     * @param aSolrDocument Solr document instance.
     */
    public void applyContentLimits(Document aSolrDocument)
    {
        int offset;
        String fieldName;
        String contentValue;
        boolean isHighlighted;
        Logger appLogger = mAppMgr.getLogger(this, "applyContentLimits");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String begToken = getCfgString("highlight_beg_token");
        int contentLength = getCfgInteger("content_length", Solr.CONTENT_LENGTH_DEFAULT);
        if (contentLength > 0)
        {
            if ((Solr.isHeaderPopulated(aSolrDocument)) && (StringUtils.isNotEmpty(begToken)))
            {
                DataBag headerBag = Solr.getHeader(aSolrDocument);
                isHighlighted = headerBag.isValueTrue("is_highlighted");
            }
            else
                isHighlighted = false;
            if (Solr.isResponsePopulated(aSolrDocument))
            {
                DataTable docTable = Solr.getResponse(aSolrDocument);
                DataBag docBag = docTable.getColumnBag();
                int rowCount = docTable.rowCount();
                for (DataField docField : docBag.getFields())
                {
                    if (docField.isFeatureTrue(Solr.FEATURE_IS_CONTENT))
                    {
                        fieldName = docField.getName();
                        for (int row = 0; row < rowCount; row++)
                        {
                            contentValue = docTable.getValueByName(row, fieldName);
                            if (StringUtils.isNotEmpty(contentValue))
                            {
                                int strLength = contentValue.length();
                                if (strLength > contentLength)
                                {
                                    if (isHighlighted)
                                        offset = contentValue.indexOf(begToken);
                                    else
                                        offset = -1;
                                    if (offset == -1)
                                        docTable.setValueByName(row, fieldName,
                                            contentValue.substring(0, contentLength));
                                }
                            }
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Applies HTML highlight substitution logic to document fields
     * in the response table.
     *
     * @param aSolrDocument Solr document instance.
     */
    public void applyHTMLHighlights(Document aSolrDocument)
    {
        String fieldValue, begValue, endValue;
        Logger appLogger = mAppMgr.getLogger(this, "applyHTMLHighlights");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String begToken = getCfgString("highlight_beg_token");
        String endToken = getCfgString("highlight_end_token");
        if ((StringUtils.isNotEmpty(begToken)) && (StringUtils.isNotEmpty(endToken)))
        {
            String begHTML = getCfgString("highlight_beg_html");
            String endHTML = getCfgString("highlight_end_html");
            String[] fieldNames = getCfgStrings("highlight_field_name");
            if ((StringUtils.isNotEmpty(begHTML)) && (StringUtils.isNotEmpty(endHTML)) &&
                (fieldNames.length > 0))
            {
                if (Solr.isResponsePopulated(aSolrDocument))
                {
                    if (Solr.isHeaderPopulated(aSolrDocument))
                    {
                        DataBag headerBag = Solr.getHeader(aSolrDocument);
                        if (headerBag.isValueTrue("is_highlighted"))
                        {
                            DataTable docTable = Solr.getResponse(aSolrDocument);
                            int rowCount = docTable.rowCount();
                            for (int row = 0; row < rowCount; row++)
                            {
                                for (String fieldName : fieldNames)
                                {
                                    fieldValue = docTable.getValueByName(row, fieldName);
                                    begValue = StringUtils.replace(fieldValue, begToken, begHTML);
                                    endValue = StringUtils.replace(begValue, endToken, endHTML);
                                    docTable.setValueByName(row, fieldName, endValue);
                                }
                            }
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Applies content limits and HTML highlight substitution logic to
     * document fields in the response table.
     *
     * @param aSolrDocument Solr document instance.
     */
    public void applyAll(Document aSolrDocument)
    {
        Logger appLogger = mAppMgr.getLogger(this, "applyAll");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        applyContentLimits(aSolrDocument);
        applyHTMLHighlights(aSolrDocument);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
