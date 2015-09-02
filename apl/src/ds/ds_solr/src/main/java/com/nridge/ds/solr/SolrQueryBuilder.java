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
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Date;

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

    private String escapeValue(String aValue)
    {
        int offset1 = aValue.indexOf(StrUtl.CHAR_BACKSLASH);
        int offset2 = aValue.indexOf(StrUtl.CHAR_DBLQUOTE);
        if ((offset1 != -1) || (offset2 != -1))
        {
            StringBuilder strBuilder = new StringBuilder();
            int strLength = aValue.length();
            for (int i = 0; i < strLength; i++)
            {
                if ((aValue.charAt(i) == StrUtl.CHAR_BACKSLASH) ||
                    (aValue.charAt(i) == StrUtl.CHAR_DBLQUOTE))
                    strBuilder.append(StrUtl.CHAR_BACKSLASH);
                strBuilder.append(aValue.charAt(i));
            }
            aValue = strBuilder.toString();
        }
        offset2 = aValue.indexOf(StrUtl.CHAR_SPACE);
        int offset3 = aValue.indexOf(StrUtl.CHAR_COLON);
        int offset4 = aValue.indexOf(StrUtl.CHAR_HYPHEN);
        int offset5 = aValue.indexOf(StrUtl.CHAR_PLUS);
        int offset6 = aValue.indexOf(StrUtl.CHAR_FORWARDSLASH);
        if ((offset2 == -1) && (offset3 == -1) &&
            (offset4 == -1) && (offset5 == -1) &&
            (offset6 == -1))
            return aValue;
        else
            return "\"" + aValue + "\"";
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
    public String createAsString(DSCriteria aCriteria)
    {
        boolean isFirst;
        DataField dataField;
        StringBuilder qryBuilder;
        String fieldName, fieldOpValue;
        Logger appLogger = mAppMgr.getLogger(this, "createAsString");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        StringBuilder stringBuilder = new StringBuilder();

        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                if (stringBuilder.length() > 0)
                    stringBuilder.append(String.format(" %s ", ce.getBooleanOperator().name()));

                fieldName = ce.getName();
                if (StringUtils.startsWith(fieldName, Solr.FIELD_PREFIX))
                {
                    if (StringUtils.equals(fieldName, Solr.FIELD_QUERY_NAME))
                        stringBuilder.append(ce.getValue());
                }
                else
                {
                    dataField = ce.getField();

                    if (dataField.isTypeText())
                    {
                        switch (ce.getLogicalOperator())
                        {
                            case EQUAL:
                                fieldOpValue = String.format("%s:%s", dataField.getName(),
                                    escapeValue(dataField.getValue()));
                                stringBuilder.append(fieldOpValue);
                                break;
                            case NOT_EQUAL:
                                fieldOpValue = String.format("-%s:%s", dataField.getName(),
                                    escapeValue(dataField.getValue()));
                                stringBuilder.append(fieldOpValue);
                                break;
                            case CONTAINS:
                                fieldOpValue = String.format("%s:*%s*", dataField.getName(),
                                    escapeValue(dataField.getValue()));
                                stringBuilder.append(fieldOpValue);
                                break;
                            case NOT_CONTAINS:
                                fieldOpValue = String.format("-%s:*%s*", dataField.getName(),
                                    escapeValue(dataField.getValue()));
                                stringBuilder.append(fieldOpValue);
                                break;
                            case STARTS_WITH:
                                fieldOpValue = String.format("%s:%s*", dataField.getName(),
                                    escapeValue(dataField.getValue()));
                                stringBuilder.append(fieldOpValue);
                                break;
                            case ENDS_WITH:
                                fieldOpValue = String.format("%s:*%s", dataField.getName(),
                                    escapeValue(dataField.getValue()));
                                stringBuilder.append(fieldOpValue);
                                break;
                            case IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                                stringBuilder.append(qryBuilder);
                                break;
                            case NOT_IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                                stringBuilder.append(qryBuilder);
                                break;
                        }
                    }
                    else if (dataField.isTypeNumber())
                    {
                        switch (ce.getLogicalOperator())
                        {
                            case EQUAL:
                                fieldOpValue = String.format("%s:%s", dataField.getName(), dataField.getValue());
                                stringBuilder.append(fieldOpValue);
                                break;
                            case NOT_EQUAL:
                                fieldOpValue = String.format("-%s:%s", dataField.getName(), dataField.getValue());
                                stringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN:
                                fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dataField.getValue());
                                stringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN_EQUAL:
                                dataField.setValue(dataField.getValueAsInt() - 1);
                                fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dataField.getValue());
                                stringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN:
                                fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dataField.getValue());
                                stringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN_EQUAL:
                                dataField.setValue(dataField.getValueAsInt() + 1);
                                fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dataField.getValue());
                                stringBuilder.append(fieldOpValue);
                                break;
                            case BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(),
                                        dataField.getValue(0), dataField.getValue(1));
                                    stringBuilder.append(fieldOpValue);
                                }
                                break;
                            case NOT_BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    fieldOpValue = String.format("-%s:[%s TO %s]", dataField.getName(),
                                        dataField.getValue(0), dataField.getValue(1));
                                    stringBuilder.append(fieldOpValue);
                                }
                                break;
                            case BETWEEN_INCLUSIVE:
                                int numValue1 = Integer.parseInt(dataField.getValue(0)) - 1;
                                int numValue2 = Integer.parseInt(dataField.getValue(1)) + 1;
                                fieldOpValue = String.format("%s:[%d TO %d]", dataField.getName(), numValue1, numValue2);
                                stringBuilder.append(fieldOpValue);
                                break;
                            case IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                                stringBuilder.append(qryBuilder);
                                break;
                            case NOT_IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                                for (String mValue : dataField.getValues())
                                {
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(escapeValue(mValue));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                                stringBuilder.append(qryBuilder);
                                break;
                        }
                    }
                    else if (dataField.isTypeDateOrTime())
                    {
                        long timeValue1, timeValue2;
                        String dateValue1, dateValue2;

                        switch (ce.getLogicalOperator())
                        {
                            case EQUAL:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                    Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:%s", dataField.getName(), dateValue1);
                                stringBuilder.append(fieldOpValue);
                                break;
                            case NOT_EQUAL:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                    Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("-%s:%s", dataField.getName(), dateValue1);
                                stringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                    Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dateValue1);
                                stringBuilder.append(fieldOpValue);
                                break;
                            case GREATER_THAN_EQUAL:
                                timeValue1 = dataField.getValueAsDate().getTime();
                                dataField.setValue(new Date(timeValue1-100));
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                    Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dateValue1);
                                stringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN:
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                    Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dateValue1);
                                stringBuilder.append(fieldOpValue);
                                break;
                            case LESS_THAN_EQUAL:
                                timeValue1 = dataField.getValueAsDate().getTime();
                                dataField.setValue(new Date(timeValue1+100));
                                dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                    Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dateValue1);
                                stringBuilder.append(fieldOpValue);
                                break;
                            case BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    dateValue1 = Field.dateValueFormatted(ce.getValueAsDate(0),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    dateValue2 = Field.dateValueFormatted(ce.getValueAsDate(1),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(),
                                        dateValue1, dateValue2);
                                    stringBuilder.append(fieldOpValue);
                                }
                                break;
                            case NOT_BETWEEN:
                                if (dataField.getValues().size() == 2)
                                {
                                    dateValue1 = Field.dateValueFormatted(ce.getValueAsDate(0),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    dateValue2 = Field.dateValueFormatted(ce.getValueAsDate(1),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    fieldOpValue = String.format("-%s:[%s TO %s]", dataField.getName(),
                                        dateValue1, dateValue2);
                                    stringBuilder.append(fieldOpValue);
                                }
                                break;
                            case BETWEEN_INCLUSIVE:
                                if (dataField.getValues().size() == 2)
                                {
                                    timeValue1 = ce.getValueAsDate(0).getTime();
                                    dateValue1 = Field.dateValueFormatted(new Date(timeValue1-100),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    timeValue2 = ce.getValueAsDate(1).getTime();
                                    dateValue2 = Field.dateValueFormatted(new Date(timeValue2+100),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(),
                                        dateValue1, dateValue2);
                                    stringBuilder.append(fieldOpValue);
                                }
                                break;
                            case IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                                for (String mValue : dataField.getValues())
                                {
                                    dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(escapeValue(dateValue1));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(escapeValue(dateValue1));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                                stringBuilder.append(qryBuilder);
                                break;
                            case NOT_IN:
                                isFirst = true;
                                qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                                for (String mValue : dataField.getValues())
                                {
                                    dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                        Field.FORMAT_ISO8601DATETIME_DEFAULT);
                                    if (isFirst)
                                    {
                                        isFirst = false;
                                        qryBuilder.append(escapeValue(dateValue1));
                                    }
                                    else
                                    {
                                        qryBuilder.append(" OR ");
                                        qryBuilder.append(escapeValue(dateValue1));
                                    }
                                }
                                qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                                stringBuilder.append(qryBuilder);
                                break;
                        }
                    }
                }
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringBuilder.toString();
    }

    private void add(SolrQuery aSolrQuery, DSCriterionEntry aCriterionEntry)
    {
        boolean isFirst;
        String fieldOpValue;
        StringBuilder qryBuilder;
        Logger appLogger = mAppMgr.getLogger(this, "add");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataField dataField = aCriterionEntry.getField();

        if (dataField.isTypeText())
        {
            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    fieldOpValue = String.format("%s:%s", dataField.getName(),
                                                 escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_EQUAL:
                    fieldOpValue = String.format("-%s:%s", dataField.getName(),
                                                 escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case CONTAINS:
                    fieldOpValue = String.format("%s:*%s*", dataField.getName(),
                                                 escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_CONTAINS:
                    fieldOpValue = String.format("-%s:*%s*", dataField.getName(),
                                                 escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case STARTS_WITH:
                    fieldOpValue = String.format("%s:%s*", dataField.getName(),
                                                 escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case ENDS_WITH:
                    fieldOpValue = String.format("%s:*%s", dataField.getName(),
                                                 escapeValue(dataField.getValue()));
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case NOT_IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
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
                    qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case NOT_IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                    for (String mValue : dataField.getValues())
                    {
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(escapeValue(mValue));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(escapeValue(mValue));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case SORT:
                    aSolrQuery.addSort(dataField.getName(), valueToOrder(dataField.getValue()));
                    break;
            }
        }
        else if (dataField.isTypeDateOrTime())
        {
            long timeValue1, timeValue2;
            String dateValue1, dateValue2;

            switch (aCriterionEntry.getLogicalOperator())
            {
                case EQUAL:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:%s", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case NOT_EQUAL:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("-%s:%s", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case GREATER_THAN:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case GREATER_THAN_EQUAL:
                    timeValue1 = dataField.getValueAsDate().getTime();
                    dataField.setValue(new Date(timeValue1-100));
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:[%s TO *]", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case LESS_THAN:
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case LESS_THAN_EQUAL:
                    timeValue1 = dataField.getValueAsDate().getTime();
                    dataField.setValue(new Date(timeValue1+100));
                    dateValue1 = Field.dateValueFormatted(dataField.getValueAsDate(),
                                                          Field.FORMAT_ISO8601DATETIME_DEFAULT);
                    fieldOpValue = String.format("%s:[* TO %s]", dataField.getName(), dateValue1);
                    aSolrQuery.addFilterQuery(fieldOpValue);
                    break;
                case BETWEEN:
                    if (dataField.getValues().size() == 2)
                    {
                        dateValue1 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(0),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        dateValue2 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(1),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(),
                                                     dateValue1, dateValue2);
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case NOT_BETWEEN:
                    if (dataField.getValues().size() == 2)
                    {
                        dateValue1 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(0),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        dateValue2 = Field.dateValueFormatted(aCriterionEntry.getValueAsDate(1),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("-%s:[%s TO %s]", dataField.getName(),
                                                     dateValue1, dateValue2);
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case BETWEEN_INCLUSIVE:
                    if (dataField.getValues().size() == 2)
                    {
                        timeValue1 = aCriterionEntry.getValueAsDate(0).getTime();
                        dateValue1 = Field.dateValueFormatted(new Date(timeValue1-100),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        timeValue2 = aCriterionEntry.getValueAsDate(1).getTime();
                        dateValue2 = Field.dateValueFormatted(new Date(timeValue2+100),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        fieldOpValue = String.format("%s:[%s TO %s]", dataField.getName(),
                                                     dateValue1, dateValue2);
                        aSolrQuery.addFilterQuery(fieldOpValue);
                    }
                    break;
                case IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                    for (String mValue : dataField.getValues())
                    {
                        dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(escapeValue(dateValue1));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(escapeValue(dateValue1));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case NOT_IN:
                    isFirst = true;
                    qryBuilder = new StringBuilder(String.format("-%s:%c", dataField.getName(), StrUtl.CHAR_LEFTPAREN));
                    for (String mValue : dataField.getValues())
                    {
                        dateValue1 = Field.dateValueFormatted(Field.createDate(mValue, Field.FORMAT_DATETIME_DEFAULT),
                                                              Field.FORMAT_ISO8601DATETIME_DEFAULT);
                        if (isFirst)
                        {
                            isFirst = false;
                            qryBuilder.append(escapeValue(dateValue1));
                        }
                        else
                        {
                            qryBuilder.append(" OR ");
                            qryBuilder.append(escapeValue(dateValue1));
                        }
                    }
                    qryBuilder.append(StrUtl.CHAR_RIGHTPAREN);
                    aSolrQuery.addFilterQuery(qryBuilder.toString());
                    break;
                case SORT:
                    aSolrQuery.addSort(dataField.getName(), valueToOrder(dataField.getValue()));
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
            rhEndpoint = Solr.QUERY_RESPONSE_HANDLER_DEFAULT;
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
        String[] filterQueries;
        String paramName, paramValue;
        Logger appLogger = mAppMgr.getLogger(this, "urlToSolrQuery");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setStart(Solr.QUERY_OFFSET_DEFAULT);
        solrQuery.setRows(Solr.QUERY_PAGESIZE_DEFAULT);
        solrQuery.setRequestHandler(requestHandlerEndpoint(aCriteria));

        URL solrURL = new URL(aURL);
        String urlQuery = solrURL.getQuery();
        String[] urlPairs = urlQuery.split("&");
        if (urlPairs.length > 0)
        {
            int fqCount = 0;
            for (String urlPair : urlPairs)
            {
                if (urlPair.startsWith("fq="))
                    fqCount++;
            }
            if (fqCount > 0)
                filterQueries = new String[fqCount];
            else
                filterQueries = null;
            int fqOffset = 0;

            for (String urlPair : urlPairs)
            {
                offset = urlPair.indexOf("=");
                if (offset > 0)
                {
                    paramName = URLDecoder.decode(urlPair.substring(0, offset), "UTF-8");
                    paramValue = URLDecoder.decode(urlPair.substring(offset + 1), "UTF-8");
                    if (paramName.equals("q"))
                        solrQuery.setQuery(paramValue);
                    else if (paramName.equals("fq"))
                    {
                        if ((filterQueries != null) && (fqOffset < fqCount))
                            filterQueries[fqOffset++] = paramValue;
                    }
                    else
                        solrQuery.setParam(paramName, paramValue);
                }
            }
            if (filterQueries != null)
                solrQuery.setFilterQueries(filterQueries);
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
     * search query stored within the <i>DSCriteria</i> instance.
     *
     * @param aCriteria DS Criteria instance.
     *
     * @return SolrQuery instance.
     *
     * @throws DSException Data source related exception.
     */
    public SolrQuery create(DSCriteria aCriteria)
        throws DSException
    {
        String fieldName;
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

        if ((aCriteria != null) && (aCriteria.count() > 0))
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
                        else if (StringUtils.equals(fieldName, Solr.FIELD_URI_NAME))
                            solrQuery = urlToSolrQuery(aCriteria, ce.getValue());
                    }
                    else
                        add(solrQuery, ce);
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
