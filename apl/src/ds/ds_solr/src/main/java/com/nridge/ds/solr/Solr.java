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

import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.ds.DSCriteria;
import com.nridge.core.base.ds.DSCriterionEntry;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataTable;
import org.apache.commons.lang3.StringUtils;

/**
 * The Solr class captures the constants, enumerated types
 * and utility methods for the Solr data source package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Solr
{
    public static final String CFG_PROPERTY_PREFIX = "ds.solr";

    public static final int QUERY_OFFSET_DEFAULT = 0;
    public static final int QUERY_PAGESIZE_DEFAULT = 10;

    public static final String QUERY_RESPONSE_HANDLER_DEFAULT = "/select";

    public static final int RESPONSE_STATUS_SUCCESS = 0;

    public static final int CONNECTION_TIMEOUT_MINIMUM = 1000;

    public static final String FIELD_PREFIX = "ds_solr_";
    public static final String FIELD_URI_NAME = "ds_solr_uri_field";
    public static final String FIELD_QUERY_NAME = "ds_solr_query_field";
    public static final String FIELD_HANDLER_NAME = "ds_solr_handler_field";
    public static final String FIELD_PC_EXPAND_NAME = "ds_solr_pc_expand_field";

    public static final String PC_EXPANSION_NONE = "None";
    public static final String PC_EXPANSION_BOTH = "Both";      // Enables Parent and Child below
    public static final String PC_EXPANSION_CHILD = "Child";    // Adds all children and includes parent
    public static final String PC_EXPANSION_PARENT = "Parent";  // Adds parent and includes child hits

    public static final String DOC_OPERATION_ADD = "add";
    public static final String DOC_OPERATION_DELETE = "delete";

    public static final String DOC_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public static final String GROUP_NAME_UNDEFINED = "Undefined";

    public static final int CONTENT_LENGTH_DEFAULT = 250;

// DS Criteria Features

    public static final String FEATURE_ACCOUNT_NAME = "accountName";
    public static final String FEATURE_ACCOUNT_PASSWORD = "accountPassword";

// Solr data field feature constants.
// http://wiki.apache.org/solr/SchemaXml

    public static final String FEATURE_IS_FACET = "isFacet";
    public static final String FEATURE_IS_RESULT = "isResult";
    public static final String FEATURE_IS_STORED = "isStored";
    public static final String FEATURE_IS_DEFAULT = "isDefault";
    public static final String FEATURE_IS_INDEXED = "isIndexed";
    public static final String FEATURE_IS_CONTENT = Field.FEATURE_IS_CONTENT;
    public static final String FEATURE_IS_OMIT_NORMS = "isOmitNorms";
    public static final String FEATURE_IS_HIGHLIGHTED = "isHighlighted";

    public static final String FEATURE_SOLR_TYPE = "solrType";
    public static final String FEATURE_FIELD_BOOST = "fieldBoost";

    public static final String QUERY_ALL_DOCUMENTS = "*:*";

    public static final String DOCUMENT_TYPE = "Solr Document";

// Solr response document relationships.

    public static final String RESPONSE_GROUP = "Group";
    public static final String RESPONSE_HEADER = "Header";
    public static final String RESPONSE_DOCUMENT = "Document";
    public static final String RESPONSE_SPELLING = "Spelling";
    public static final String RESPONSE_STATISTIC = "Statistic";
    public static final String RESPONSE_FACET_FIELD = "Facet Field";
    public static final String RESPONSE_FACET_PIVOT = "Facet Pivot";
    public static final String RESPONSE_FACET_QUERY = "Facet Query";
    public static final String RESPONSE_FACET_RANGE = "Facet Range";
    public static final String RESPONSE_GROUP_FIELD = "Group Field";
    public static final String RESPONSE_GROUP_DOCUMENT = "Group Document";
    public static final String RESPONSE_GROUP_COLLECTION = "Group Collection";

    public static final long POOL_EVICTION_TIMEOUT = 900000L;   // 15 minutes

    private Solr()
    {
    }

    public static boolean isSolrReservedFieldName(String aName)
    {
        if (StringUtils.equals(aName, "_root_"))
            return true;
        else if (StringUtils.equals(aName, "_version_"))
            return true;
        else
            return false;
    }

    public static boolean isHeaderPopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship headerRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_HEADER);
            return (headerRelationship != null);
        }

        return false;
    }

    public static DataBag getHeader(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship headerRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_HEADER);
            if (headerRelationship != null)
            {
                DataBag headerBag = headerRelationship.getBag();
                return headerBag;
            }
        }

        return null;
    }

    public static boolean isResponsePopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship documentRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_DOCUMENT);
            return ((documentRelationship != null) && (documentRelationship.count() > 0));
        }

        return false;
    }

    public static DataTable getResponse(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship documentRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_DOCUMENT);
            if (documentRelationship != null)
            {
                if (documentRelationship.count() > 0)
                {
                    Document resultDocument = documentRelationship.getDocuments().get(0);
                    return resultDocument.getTable();
                }
            }
        }

        return null;
    }

    public static boolean isFacetFieldsPopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_FIELD);
            return ((facetRelationship != null) && (facetRelationship.count() > 0));
        }

        return false;
    }

    public static DataTable getFacetFields(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_FIELD);
            if (facetRelationship != null)
            {
                if (facetRelationship.count() > 0)
                {
                    Document resultDocument = facetRelationship.getDocuments().get(0);
                    return resultDocument.getTable();
                }
            }
        }

        return null;
    }

    public static boolean isFacetQueriesPopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_QUERY);
            return ((facetRelationship != null) && (facetRelationship.count() > 0));
        }

        return false;
    }

    public static DataTable getFacetQueries(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_QUERY);
            if (facetRelationship != null)
            {
                if (facetRelationship.count() > 0)
                {
                    Document resultDocument = facetRelationship.getDocuments().get(0);
                    return resultDocument.getTable();
                }
            }
        }

        return null;
    }

    public static boolean isFacetPivotPopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_PIVOT);
            return ((facetRelationship != null) && (facetRelationship.count() > 0));
        }

        return false;
    }

    public static DataTable getFacetPivot(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_PIVOT);
            if (facetRelationship != null)
            {
                if (facetRelationship.count() > 0)
                {
                    Document resultDocument = facetRelationship.getDocuments().get(0);
                    return resultDocument.getTable();
                }
            }
        }

        return null;
    }

    public static boolean isFacetRangePopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_RANGE);
            return ((facetRelationship != null) && (facetRelationship.count() > 0));
        }

        return false;
    }

    public static DataTable getFacetRange(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship facetRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_FACET_RANGE);
            if (facetRelationship != null)
            {
                if (facetRelationship.count() > 0)
                {
                    Document resultDocument = facetRelationship.getDocuments().get(0);
                    return resultDocument.getTable();
                }
            }
        }

        return null;
    }

    public static boolean isGroupsPopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship groupRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_GROUP);
            return (groupRelationship != null);
        }

        return false;
    }

    public static Relationship getGroups(Document aSolrDocument)
    {
        if (aSolrDocument != null)
            return aSolrDocument.getFirstRelationship(Solr.RESPONSE_GROUP);

        return null;
    }

    public static boolean isStatisticsPopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship statisticRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_STATISTIC);
            return ((statisticRelationship != null) && (statisticRelationship.count() > 0));
        }

        return false;
    }

    public static DataTable getStatistics(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship statisticRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_STATISTIC);
            if (statisticRelationship != null)
            {
                if (statisticRelationship.count() > 0)
                {
                    Document resultDocument = statisticRelationship.getDocuments().get(0);
                    return resultDocument.getTable();
                }
            }
        }

        return null;
    }

    public static boolean isSpellingPopulated(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship spellingRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_SPELLING);
            return (spellingRelationship != null);
        }

        return false;
    }

    public static DataBag getSpelling(Document aSolrDocument)
    {
        if (aSolrDocument != null)
        {
            Relationship spellingRelationship = aSolrDocument.getFirstRelationship(Solr.RESPONSE_SPELLING);
            if (spellingRelationship != null)
            {
                DataBag spellingBag = spellingRelationship.getBag();
                return spellingBag;
            }
        }

        return null;
    }

    public static boolean isCriteriaParentChild(DSCriteria aCriteria)
    {
        if ((aCriteria != null) && (aCriteria.count() > 0))
        {
            String fieldName;

            for (DSCriterionEntry ce : aCriteria.getCriterionEntries())
            {
                fieldName = ce.getName();
                if (StringUtils.equals(fieldName, Solr.FIELD_PC_EXPAND_NAME))
                    return true;
            }
        }

        return false;
    }
}
