/*
 * NorthRidge Software, LLC - Copyright (c) 2014.
 *
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains
 * the property of NorthRidge Software, LLC and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to NorthRidge Software, LLC and its
 * suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or
 * copyright law.  Dissemination of this information or
 * reproduction of this material is strictly forbidden unless
 * prior written permission is obtained from NorthRidge
 * Software, LLC.
 */

package com.nridge.ds.solr;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Doc;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.ds.DSException;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * The SolrQueryBuilder provides a collection of methods that can generate
 * a Solr query based on a <i>DSCriteria</i>.  It is a helper class for
 * the {@link SolrDS} class.
 *
 * @author Al Cole
 * @since 1.0
 */
public class SolrQueryBuilder
{
    private final AppMgr mAppMgr;
    private String mCfgPropertyPrefix = StringUtils.EMPTY;

    /**
     * Constructor that accepts an application manager instance.
     *
     * @param anAppMgr Application manager instance.
     */
    public SolrQueryBuilder(final AppMgr anAppMgr)
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

        if (org.apache.commons.lang3.StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    private SolrQuery.ORDER valueToOrder(String aValue)
    {
        SolrQuery.ORDER sortOrder;
        try
        {
            Field.Order fieldOrder = Field.Order.valueOf(aValue);
            if (fieldOrder == Field.Order.DESCENDING)
                sortOrder = SolrQuery.ORDER.desc;
            else
                sortOrder = SolrQuery.ORDER.asc;
        }
        catch (Exception e)
        {
            sortOrder = SolrQuery.ORDER.asc;
        }

        return sortOrder;
    }

    private void createAsQueryString(StringBuilder aStringBuilder, DSCriteria aCriteria)
    {
        boolean isFirst;
        DataField dataField;
        StringBuilder qryBuilder;
        String fieldName, fieldOpValue;
        Logger appLogger = mAppMgr.getLogger(this, "createAsQueryString");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                if (aStringBuilder.length() > 0)
                    aStringBuilder.append(String.format(" %s ", ce.getBooleanOperator().name()));

                fieldName = ce.getName();
                if (StringUtils.startsWith(fieldName, Solr.FIELD_PREFIX))
                {
                    if (StringUtils.equals(fieldName, Solr.FIELD_QUERY_NAME))
                        aStringBuilder.append(ce.getValue());
                }
                else
                {
                    dataField = ce.getField();

                    if (dataField.isTypeText())
                    {
                        switch (ce.getLogicalOperator())
                        {
                            case EQUAL:
                                fieldOpValue = String.format("%s:%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case NOT_EQUAL:
                                fieldOpValue = String.format("-%s:%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case CONTAINS:
                                fieldOpValue = String.format("%s:*%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case NOT_CONTAINS:
                                fieldOpValue = String.format("-%s:*%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case STARTS_WITH:
                                fieldOpValue = String.format("%s:%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case ENDS_WITH:
                                fieldOpValue = String.format("%s:*%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                                aStringBuilder.append(qryBuilder);
                                break;
                            case NOT_IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                                aStringBuilder.append(qryBuilder);
                                break;
                        }
                    }
                    else if (dataField.isTypeNumber())
                    {
                        switch (ce.getLogicalOperator())
                        {
                            case EQUAL:
                                fieldOpValue = String.format("%s:%s", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case NOT_EQUAL:
                                fieldOpValue = String.format("-%s:%s", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN:
                                fieldOpValue = String.format("%s:{%s TO *}", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN_EQUAL:
                                fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN:
                                fieldOpValue = String.format("%s:{* TO %s}", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN_EQUAL:
                                fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    fieldOpValue = String.format("%s:{%s TO %s}", dataField.getName(), dataField.getValue(0), dataField.getValue(1));
                                    aStringBuilder.append(fieldOpValue);
                                }
                                break;
                            case NOT_BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    fieldOpValue = String.format("-%s:{%s TO %s}", dataField.getName(), dataField.getValue(0), dataField.getValue(1));
                                    aStringBuilder.append(fieldOpValue);
                                }
                                break;
                            case BETWEEN_INCLUSIVE:
                                if (dataField.getValues().size() == 2)
                                {
                                    fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(), dataField.getValue(0), dataField.getValue(1));
                                    aStringBuilder.append(fieldOpValue);
                                }
                                break;
                            case IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                                aStringBuilder.append(qryBuilder);
                                break;
                            case NOT_IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(Solr.escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                                aStringBuilder.append(qryBuilder);
                                break;
                        }
                    }
                    else if (dataField.isTypeDateOrTime())
                    {
                        String dateValue1, dateValue2;

                        switch (ce.getLogicalOperator())
                        {
                            case EQUAL:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:%s", dataField.getName(), dateValue1);
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case NOT_EQUAL:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("-%s:%s", dataField.getName(), dateValue1);
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:{%s TO *}", dataField.getName(), dateValue1);
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN_EQUAL:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dateValue1);
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:{* TO %s}", dataField.getName(), dateValue1);
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN_EQUAL:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dateValue1);
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    dateValue1 = Field.dateValueFormatted(ce.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    dateValue2 = Field.dateValueFormatted(ce.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    fieldOpValue = String.format("%s:{%s TO %s}", dataField.getName(), dateValue1, dateValue2);
                                    aStringBuilder.append(fieldOpValue);
                                }
                                break;
                            case NOT_BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    dateValue1 = Field.dateValueFormatted(ce.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    dateValue2 = Field.dateValueFormatted(ce.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    fieldOpValue = String.format("-%s:{%s TO %s}", dataField.getName(), dateValue1, dateValue2);
                                    aStringBuilder.append(fieldOpValue);
                                }
                                break;
                            case BETWEEN_INCLUSIVE:
                                if (dataField.getValues().size() == 2)
                                {
                                    dateValue1 = Field.dateValueFormatted(ce.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    dateValue2 = Field.dateValueFormatted(ce.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(), dateValue1, dateValue2);
                                    aStringBuilder.append(fieldOpValue);
                                }
                                break;
                            case IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                                for (String mValue : dataField.getValues())
                                {
                                    dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(Solr.escapeValue(dateValue1));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(Solr.escapeValue(dateValue1));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                                aStringBuilder.append(qryBuilder);
                                break;
                            case NOT_IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                                for (String mValue : dataField.getValues())
                                {
                                    dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(Solr.escapeValue(dateValue1));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(Solr.escapeValue(dateValue1));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                                aStringBuilder.append(qryBuilder);
                                break;
                        }
                    }
                    else if (dataField.isTypeBoolean())
                    {
                        switch (ce.getLogicalOperator())
                        {
                            case EQUAL:
                                fieldOpValue = String.format("%s:%s", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                            case NOT_EQUAL:
                                fieldOpValue = String.format("-%s:%s", dataField.getName(), dataField.getValue());
                                aStringBuilder.append(fieldOpValue);
                                break;
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private void appendParameter(StringBuilder aStringBuilder, String aName, String aValue)
        throws DSException
    {
        if (StringUtils.isNotEmpty(aValue))
        {
            try
            {
                String encodedValue = URLEncoder.encode(aValue, StandardCharsets.UTF_8.toString());
                int offset = aStringBuilder.toString().indexOf(StrUtl.CHAR_QUESTMARK);
                if (offset == -1)
                    aStringBuilder.append(String.format("?%s=%s", aName, encodedValue));
                else
                    aStringBuilder.append(String.format("&%s=%s", aName, encodedValue));
            }
            catch (UnsupportedEncodingException e)
            {
                Logger appLogger = mAppMgr.getLogger(this, "appendParameter");
                appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);
                String msgStr = String.format("%s: %s", e.getMessage(), aValue);
                appLogger.error(msgStr);
                throw new DSException(msgStr);
            }
        }
    }

    /**
     * Add criterion entry as a Solr query parameter.
     *
     * @param aStringBuilder String builder.
     * @param aCE Data source criterion entry.
     *
     * @throws DSException Data source exception.
     */
    public void addCriterionEntryParameter(StringBuilder aStringBuilder, DSCriterionEntry aCE)
        throws DSException
    {
        boolean isFirst;
        DataField dataField;
        StringBuilder qryBuilder;
        String fieldName, fieldOpValue;
        Logger appLogger = mAppMgr.getLogger(this, "addCriterionEntry");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        fieldName = aCE.getName();
        dataField = aCE.getField();
        if (StringUtils.startsWith(fieldName, Solr.FIELD_PREFIX))
        {
            if (StringUtils.equals(fieldName, Solr.FIELD_QUERY_NAME))
                appendParameter(aStringBuilder, "q", dataField.getValue());
            else if (StringUtils.equals(fieldName, Solr.FIELD_PARAM_NAME))
            {
                if (dataField.valueCount() == 2)
                    appendParameter(aStringBuilder, dataField.getValue(0), dataField.getValue(1));
            }
        }
        else
        {
            if (dataField.isTypeText())
            {
                switch (aCE.getLogicalOperator())
                {
                    case EQUAL:
                        fieldOpValue = String.format("%s:%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case NOT_EQUAL:
                        fieldOpValue = String.format("-%s:%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case CONTAINS:
                        fieldOpValue = String.format("%s:*%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case NOT_CONTAINS:
                        fieldOpValue = String.format("-%s:*%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case STARTS_WITH:
                        fieldOpValue = String.format("%s:%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case ENDS_WITH:
                        fieldOpValue = String.format("%s:*%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case IN:
                        isFirst = true;
                        qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                        for (String mValue : dataField.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                            else
                            {
                                qryBuilder.append(" OR ");
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                        }
                        qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                        appendParameter(aStringBuilder, "fq", qryBuilder.toString());
                        break;
                    case NOT_IN:
                        isFirst = true;
                        qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                        for (String mValue : dataField.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                            else
                            {
                                qryBuilder.append(" OR ");
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                        }
                        qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                        appendParameter(aStringBuilder, "fq", qryBuilder.toString());
                        break;
                }
            }
            else if (dataField.isTypeNumber())
            {
                switch (aCE.getLogicalOperator())
                {
                    case EQUAL:
                        fieldOpValue = String.format("%s:%s", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case NOT_EQUAL:
                        fieldOpValue = String.format("-%s:%s", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case GREATER_THAN:
                        fieldOpValue = String.format("%s:{%s TO *}", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case GREATER_THAN_EQUAL:
                        fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case LESS_THAN:
                        fieldOpValue = String.format("%s:{* TO %s}", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case LESS_THAN_EQUAL:
                        fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case BETWEEN:
                        if (dataField.getValues().size() == 2)
                        {
                            fieldOpValue = String.format("%s:{%s TO %s}", dataField.getName(), dataField.getValue(0), dataField.getValue(1));
                            appendParameter(aStringBuilder, "fq", fieldOpValue);
                        }
                        break;
                    case NOT_BETWEEN:
                        if (dataField.getValues().size() == 2)
                        {
                            fieldOpValue = String.format("-%s:{%s TO %s}", dataField.getName(), dataField.getValue(0), dataField.getValue(1));
                            appendParameter(aStringBuilder, "fq", fieldOpValue);
                        }
                        break;
                    case BETWEEN_INCLUSIVE:
                        if (dataField.getValues().size() == 2)
                        {
                            fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(), dataField.getValue(0), dataField.getValue(1));
                            appendParameter(aStringBuilder, "fq", fieldOpValue);
                        }
                        break;
                    case IN:
                        isFirst = true;
                        qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                        for (String mValue : dataField.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                            else
                            {
                                qryBuilder.append(" OR ");
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                        }
                        qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                        appendParameter(aStringBuilder, "fq", qryBuilder.toString());
                        break;
                    case NOT_IN:
                        isFirst = true;
                        qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                        for (String mValue : dataField.getValues())
                        {
                            if (isFirst)
                            {
                                isFirst = false;
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                            else
                            {
                                qryBuilder.append(" OR ");
                                qryBuilder.append(Solr.escapeValue(mValue));
                            }
                        }
                        qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                        appendParameter(aStringBuilder, "fq", qryBuilder.toString());
                        break;
                }
            }
            else if (dataField.isTypeDateOrTime())
            {
                String dateValue1, dateValue2;

                switch (aCE.getLogicalOperator())
                {
                    case EQUAL:
                        dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:%s", dataField.getName(), dateValue1);
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case NOT_EQUAL:
                        dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("-%s:%s", dataField.getName(), dateValue1);
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case GREATER_THAN:
                        dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:{%s TO *}", dataField.getName(), dateValue1);
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case GREATER_THAN_EQUAL:
                        dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dateValue1);
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case LESS_THAN:
                        dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:{* TO %s}", dataField.getName(), dateValue1);
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case LESS_THAN_EQUAL:
                        dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dateValue1);
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case BETWEEN:
                        if (dataField.getValues().size() == 2)
                        {
                            dateValue1 = Field.dateValueFormatted(aCE.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            dateValue2 = Field.dateValueFormatted(aCE.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            fieldOpValue = String.format("%s:{%s TO %s}", dataField.getName(), dateValue1, dateValue2);
                            appendParameter(aStringBuilder, "fq", fieldOpValue);
                        }
                        break;
                    case NOT_BETWEEN:
                        if (dataField.getValues().size() == 2)
                        {
                            dateValue1 = Field.dateValueFormatted(aCE.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            dateValue2 = Field.dateValueFormatted(aCE.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            fieldOpValue = String.format("-%s:{%s TO %s}", dataField.getName(), dateValue1, dateValue2);
                            appendParameter(aStringBuilder, "fq", fieldOpValue);
                        }
                        break;
                    case BETWEEN_INCLUSIVE:
                        if (dataField.getValues().size() == 2)
                        {
                            dateValue1 = Field.dateValueFormatted(aCE.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            dateValue2 = Field.dateValueFormatted(aCE.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(), dateValue1, dateValue2);
                            appendParameter(aStringBuilder, "fq", fieldOpValue);
                        }
                        break;
                    case IN:
                        isFirst = true;
                        qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                        for (String mValue : dataField.getValues())
                        {
                            dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                                  Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            if (isFirst)
                            {
                                isFirst = false;
                                qryBuilder.append(Solr.escapeValue(dateValue1));
                            }
                            else
                            {
                                qryBuilder.append(" OR ");
                                qryBuilder.append(Solr.escapeValue(dateValue1));
                            }
                        }
                        qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                        appendParameter(aStringBuilder, "fq", qryBuilder.toString());
                        break;
                    case NOT_IN:
                        isFirst = true;
                        qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                        for (String mValue : dataField.getValues())
                        {
                            dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                                  Field.FORMAT_ISO8601DATETIME_DEFAULT);
                            if (isFirst)
                            {
                                isFirst = false;
                                qryBuilder.append(Solr.escapeValue(dateValue1));
                            }
                            else
                            {
                                qryBuilder.append(" OR ");
                                qryBuilder.append(Solr.escapeValue(dateValue1));
                            }
                        }
                        qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                        appendParameter(aStringBuilder, "fq", qryBuilder.toString());
                        break;
                }
            }
            else if (dataField.isTypeBoolean())
            {
                switch (aCE.getLogicalOperator())
                {
                    case EQUAL:
                        fieldOpValue = String.format("%s:%s", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                    case NOT_EQUAL:
                        fieldOpValue = String.format("-%s:%s", dataField.getName(), dataField.getValue());
                        appendParameter(aStringBuilder, "fq", fieldOpValue);
                        break;
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }


    private void createAsQueryParameters(StringBuilder aStringBuilder, DSCriteria aCriteria)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createAsQueryParameters");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
                addCriterionEntryParameter(aStringBuilder, ce);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Creates a collapsed query string for a Solr query based on
     * <i>DSCriteria</i> instance. Use this method if you want
     * the criteria to impact the relevancy of a search request.
     *
     * @param aCriteria DS Criteria instance.
     *
     * @return Collapsed query string.
     */
    public String createAsQueryString(DSCriteria aCriteria)
    {
        Logger appLogger = mAppMgr.getLogger(this, "createAsQueryString");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        StringBuilder stringBuilder = new StringBuilder();
        createAsQueryString(stringBuilder, aCriteria);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringBuilder.toString();
    }

    /**
     * Creates Solr query URL parameters based on the
     * <i>DSCriteria</i> instance. This method is suitable
     * for appending to a prefix string that identifies the
     * Solr host name, port number, collection and request
     * handler.
     *
     * @param aCriteria DS Criteria instance.
     *
     * @return String Collapsed query string.
     *
     * @throws DSException Data source exception.
     */
    public String createAsQueryParameters(DSCriteria aCriteria)
        throws DSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createAsQueryParameters");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        StringBuilder stringBuilder = new StringBuilder();
        createAsQueryParameters(stringBuilder, aCriteria);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringBuilder.toString();
    }

    private void addFilter(SolrQuery aSolrQuery, DSCriterionEntry aCriterionEntry)
    {
        boolean isFirst;
        String fieldOpValue;
        StringBuilder qryBuilder;
        Logger appLogger = mAppMgr.getLogger(this, "addFilter");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataField dataField = aCriterionEntry.getField();

        if (dataField.isTypeText())
        {
            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    fieldOpValue = String.format("%s:%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_EQUAL:
                    fieldOpValue = String.format("-%s:%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case CONTAINS:
                    fieldOpValue = String.format("%s:*%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_CONTAINS:
                    fieldOpValue = String.format("-%s:*%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case STARTS_WITH:
                    fieldOpValue = String.format("%s:%s*", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case ENDS_WITH:
                    fieldOpValue = String.format("%s:*%s", dataField.getName(), Solr.escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case NOT_IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case SORT:
                    aSolrQuery.addSort(dataField.getName(), valueToOrder(dataField.getValue()));
                    break;
            }
        }
        else if (dataField.isTypeNumber())
        {
            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    fieldOpValue = String.format("%s:%s", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_EQUAL:
                    fieldOpValue = String.format("-%s:%s", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case GREATER_THAN:
                    fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case GREATER_THAN_EQUAL:
                    dataField.setValue(dataField.getValueAsInt() - 1);
                    fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case LESS_THAN:
                    fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case LESS_THAN_EQUAL:
                    dataField.setValue(dataField.getValueAsInt() + 1);
                    fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case BETWEEN:
                    if (dataField.getValues().size() == 2)
                    {
                        fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(),
                                                     dataField.getValue(0), dataField.getValue(1));
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case NOT_BETWEEN:
                    if (dataField.getValues().size() == 2)
                    {
                        fieldOpValue = String.format("-%s:[%s TO %s]", dataField.getName(),
                                                     dataField.getValue(0), dataField.getValue(1));
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case BETWEEN_INCLUSIVE:
                    int numValue1 = Integer.parseInt(dataField.getValue(0)) - 1;
                    int numValue2 = Integer.parseInt(dataField.getValue(1)) + 1;
                    fieldOpValue = String.format("%s:[%d TO %d]", dataField.getName(), numValue1, numValue2);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case NOT_IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(Solr.escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case SORT:
                    aSolrQuery.addSort(dataField.getName(), valueToOrder(dataField.getValue()));
                    break;
            }
        }
        else if (dataField.isTypeDateOrTime())
        {
            String dateValue1, dateValue2;

            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:%s", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_EQUAL:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("-%s:%s", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case GREATER_THAN:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:{%s TO *}", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case GREATER_THAN_EQUAL:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case LESS_THAN:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:{* TO %s}", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case LESS_THAN_EQUAL:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case BETWEEN:
                    if (dataField.getValues().size() == 2)
                    {
                        dateValue1 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        dateValue2 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:{%s TO %s}", dataField.getName(), dateValue1, dateValue2);
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case NOT_BETWEEN:
                    if (dataField.getValues().size() == 2)
                    {
                        dateValue1 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        dateValue2 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("-%s:{%s TO %s}", dataField.getName(), dateValue1, dateValue2);
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case BETWEEN_INCLUSIVE:
                    if (dataField.getValues().size() == 2)
                    {
                        dateValue1 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(0), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        dateValue2 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(1), Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(), dateValue1, dateValue2);
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                    for (String mValue : dataField.getValues())
                    {
                        dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(Solr.escapeValue(dateValue1));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(Solr.escapeValue(dateValue1));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case NOT_IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_PAREN_OPEN));
                    for (String mValue : dataField.getValues())
                    {
                        dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(Solr.escapeValue(dateValue1));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(Solr.escapeValue(dateValue1));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_PAREN_CLOSE);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case SORT:
                    aSolrQuery.addSort(dataField.getName(), valueToOrder(dataField.getValue()));
                    break;
            }
        }
        else if (dataField.isTypeBoolean())
        {
            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    fieldOpValue = String.format("%s:%s", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_EQUAL:
                    fieldOpValue = String.format("-%s:%s", dataField.getName(), dataField.getValue());
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private String requestHandlerEndpoint(DSCriteria aCriteria)
    {
        String rhEndpoint;
        Logger appLogger = mAppMgr.getLogger(this, "requestHandlerEndpoint");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = getCfgPropertyPrefix() + ".request_handler";
        String requestHandler = mAppMgr.getString(propertyName);
        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            String fieldName;

            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                fieldName = ce.getName();
                if (StringUtils.equals(fieldName, Solr.FIELD_HANDLER_NAME))
                {
                    requestHandler = ce.getValue();
                    break;
                }
            }
        }

        if (StringUtils.isEmpty(requestHandler))
            rhEndpoint = Solr.QUERY_REQUEST_HANDLER_DEFAULT;
        else
        {
            if (requestHandler.charAt(0) == StrUtl.CHAR_FORWARDSLASH)
                rhEndpoint = requestHandler;
            else
                rhEndpoint = StrUtl.CHAR_FORWARDSLASH + requestHandler;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return rhEndpoint;
    }

    /**
     * Creates a SolrQuery instance based on the contents of the URL
     * parameter.
     *
     * @param aURL Properly formed Solr URL.
     * @param aCriteria Data source criteria (used for request handler).
     * @return Populated SolrQuery instance.
     *
     * @throws MalformedURLException Improper URL.
     * @throws UnsupportedEncodingException Improper encoding.
     */
    public SolrQuery urlToSolrQuery(DSCriteria aCriteria, String aURL)
        throws MalformedURLException, UnsupportedEncodingException
    {
        int offset;
        String paramName, paramValue;
        Logger appLogger = mAppMgr.getLogger(this, "urlToSolrQuery");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setRequestHandler(requestHandlerEndpoint(aCriteria));

        URL solrURL = new URL(aURL);
        String urlQuery = solrURL.getQuery();
        String[] urlPairs = urlQuery.split("&");
        if (urlPairs.length > 0)
        {
            for (String urlPair : urlPairs)
            {
                offset = urlPair.indexOf("=");
                if (offset > 0)
                {
                    paramName = URLDecoder.decode(urlPair.substring(0, offset), "UTF-8");
                    paramValue = URLDecoder.decode(urlPair.substring(offset + 1), "UTF-8");
                    solrQuery.add(paramName, paramValue);
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrQuery;
    }

    /**
     * Creates a SolrQuery instance based on the contents of the URL
     * parameter.
     *
     * @param aURL Properly formed Solr URL.
     *
     * @return Populated SolrQuery instance.
     *
     * @throws MalformedURLException Improper URL.
     * @throws UnsupportedEncodingException Improper encoding.
     */
    public SolrQuery urlToSolrQuery(String aURL)
        throws MalformedURLException, UnsupportedEncodingException
    {
        return urlToSolrQuery(null, aURL);
    }

    /**
     * Creates a <i>SolrQuery</i> instance and populates it with the
     * search query stored within the <i>DSCriteria</i> instance.  The
     * method will assign the query string, paging details, filter
     * queries, request handler, explicit URL.  If you are using this
     * in conjunction with a QueryPlan, then the specification of a
     * request handler or explicit URL should be avoided.
     *
     * @param aCriteria DS Criteria instance.
     *
     * @return SolrQuery instance.
     *
     * @throws DSException Data source related exception.
     *
     *  @see <a href="http://lucene.apache.org/solr/guide/7_6/common-query-parameters.html">Solr Common Query Parametersr</a>
     * 	@see <a href="https://lucene.apache.org/solr/guide/7_6/the-standard-query-parser.html">Solr Standard Query Parserr</a>
     */
    public SolrQuery create(DSCriteria aCriteria)
        throws DSException
    {
        String fieldName;
        DataField dataField;
        Logger appLogger = mAppMgr.getLogger(this, "create");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SolrQuery solrQuery = new SolrQuery(Solr.QUERY_ALL_DOCUMENTS);
        solrQuery.setStart(Solr.QUERY_OFFSET_DEFAULT);
        solrQuery.setRows(Solr.QUERY_PAGESIZE_DEFAULT);

        solrQuery.setRequestHandler(requestHandlerEndpoint(aCriteria));
        if (aCriteria.isFeatureAssigned(Doc.FEATURE_OP_OFFSET))
            solrQuery.setStart(aCriteria.getFeatureAsInt(Doc.FEATURE_OP_OFFSET));
        if (aCriteria.isFeatureAssigned(Doc.FEATURE_OP_LIMIT))
            solrQuery.setRows(aCriteria.getFeatureAsInt(Doc.FEATURE_OP_LIMIT));

        if (aCriteria.count() > 0)
        {
            try
            {
                for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
                {
                    fieldName = ce.getName();
                    if (StringUtils.startsWith(fieldName, Solr.FIELD_PREFIX))
                    {
                        if (StringUtils.equals(fieldName, Solr.FIELD_QUERY_NAME))
                            solrQuery.setQuery(ce.getValue());
                        else if (StringUtils.equals(fieldName, Solr.FIELD_PARAM_NAME))
                        {
                            dataField = ce.getField();
                            if (dataField.valueCount() == 2)
                                solrQuery.add(dataField.getValue(0), dataField.getValue(1));
                        }
                        else if (StringUtils.equals(fieldName, Solr.FIELD_URL_NAME))
                            solrQuery = urlToSolrQuery(aCriteria, ce.getValue());
                    }
                    else
                        addFilter(solrQuery, ce);
                }
            }
            catch (MalformedURLException | UnsupportedEncodingException e)
            {
                throw new DSException(e.getMessage());
            }
        }

        String propertyName = getCfgPropertyPrefix() + ".echo_parameters";
        String echoParamValue = mAppMgr.getString(propertyName);
        if (StringUtils.isNotEmpty(echoParamValue))
            solrQuery.setParam("echoParams", echoParamValue);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return solrQuery;
    }
}
