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
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

/**
 * The Solr class captures the constants, enumerated types
 * and utility methods for the Solr data source package.
 *
 * @author Al Cole
 * @since 1.0
 */
@SuppressWarnings("WeakerAccess")
public class Solr
{
    public static final String CFG_PROPERTY_PREFIX = "ds.solr";

    public static final int QUERY_OFFSET_DEFAULT = 0;
    public static final int QUERY_PAGESIZE_DEFAULT = 10;

    public static final String QUERY_REQUEST_HANDLER_DEFAULT = "/select";

    public static final int RESPONSE_STATUS_SUCCESS = 0;

    public static final int CONNECTION_TIMEOUT_MINIMUM = 1000;

    public static final String FIELD_PREFIX = "ds_solr_";
    public static final String FIELD_URL_NAME = "ds_solr_url_field";
    public static final String FIELD_QUERY_NAME = "ds_solr_query_field";
    public static final String FIELD_PARAM_NAME = "ds_solr_param_field";
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
    public static final String DOCUMENT_SCHEMA_TYPE = "Solr Schema Document";

// General Solr Parameters

    public static final long POOL_EVICTION_TIMEOUT = 900000L;   // 15 minutes

// Solr Query response document relationships.

    public static final String RESPONSE_GROUP = "Group";
    public static final String RESPONSE_HEADER = "Header";
    public static final String RESPONSE_DOCUMENT = "Document";
    public static final String RESPONSE_SPELLING = "Spelling";
    public static final String RESPONSE_STATISTIC = "Statistic";
    public static final String RESPONSE_FACET_FIELD = "Facet Field";
    public static final String RESPONSE_FACET_PIVOT = "Facet Pivot";
    public static final String RESPONSE_FACET_QUERY = "Facet Query";
    public static final String RESPONSE_FACET_RANGE = "Facet Range";
    public static final String RESPONSE_MLT_DOCUMENT = "MLT Document";
    public static final String RESPONSE_MLT_COLLECTION = "MLT Collection";
    public static final String RESPONSE_MORE_LIKE_THIS = "More Like This";
    public static final String RESPONSE_GROUP_FIELD = "Group Field";
    public static final String RESPONSE_GROUP_DOCUMENT = "Group Document";
    public static final String RESPONSE_GROUP_COLLECTION = "Group Collection";

// Solr Schema response document relationships.

    public static final String RESPONSE_SCHEMA_HEADER = "Header";
    public static final String RESPONSE_SCHEMA_FIELD_TYPE = "Field Type";
    public static final String RESPONSE_SCHEMA_FIELD_NAMES = "Field Names";
    public static final String RESPONSE_SCHEMA_COPY_FIELDS = "Copy Fields";
    public static final String RESPONSE_SCHEMA_DYNAMIC_FIELDS = "Dynamic Fields";
    public static final String RESPONSE_SCHEMA_FT_ANALYZERS = "Analyzers";
    public static final String RESPONSE_SCHEMA_FT_ANALYZER = "Analyzer";
    public static final String RESPONSE_SCHEMA_FT_QUERY_ANALYZER = "Query Analyzer";
    public static final String RESPONSE_SCHEMA_FT_INDEX_ANALYZER = "Index Analyzer";
    public static final String RESPONSE_SCHEMA_FTA_TOKENIZER = "Tokenizer";
    public static final String RESPONSE_SCHEMA_FTA_FILTERS = "Filters";

// Solr Schema operations.

    public static final String SCHEMA_OPERATION_FIELD_NAME = "operation";
    public static final String SCHEMA_SEARCH_TYPE_FIELD_NAME = "search_type";
    public static final String SCHEMA_OPERATION_ADD_FIELD = "add-field";
    public static final String SCHEMA_OPERATION_DEL_FIELD = "delete-field";
    public static final String SCHEMA_OPERATION_REP_FIELD = "replace-field";
    public static final String SCHEMA_OPERATION_DYN_ADD_FIELD = "add-dynamic-field";
    public static final String SCHEMA_OPERATION_DYN_DEL_FIELD = "delete-dynamic-field";
    public static final String SCHEMA_OPERATION_DYN_REP_FIELD = "replace-dynamic-field";
    public static final String SCHEMA_OPERATION_COPY_ADD_FIELD = "add-copy-field";
    public static final String SCHEMA_OPERATION_COPY_DEL_FIELD = "delete-copy-field";
    public static final String SCHEMA_OPERATION_FT_ADD_FIELD = "add-field-type";
    public static final String SCHEMA_OPERATION_FT_DEL_FIELD = "delete-field-type";
    public static final String SCHEMA_OPERATION_FT_REP_FIELD = "replace-field-type";

// Solr Config response document relationships.

    public static final String RESPONSE_CONFIG_UPDATE_HANDLER = "Update Handler";
    public static final String RESPONSE_CONFIG_UH_INDEX_WRITER = "Index Writer";
    public static final String RESPONSE_CONFIG_UH_COMMIT_WITHIN = "Commit Within";
    public static final String RESPONSE_CONFIG_UH_AUTO_COMMIT = "Auto Commit";
    public static final String RESPONSE_CONFIG_UH_AUTO_SOFT_COMMIT = "Auto Soft Commit";
    public static final String RESPONSE_CONFIG_QUERY = "Query";
    public static final String RESPONSE_CONFIG_QUERY_FC = "Filter Cache";
    public static final String RESPONSE_CONFIG_QUERY_RC = "Results Cache";
    public static final String RESPONSE_CONFIG_QUERY_DC = "Document Cache";
    public static final String RESPONSE_CONFIG_QUERY_FVC = "Field Value Cache";
    public static final String RESPONSE_CONFIG_REQUEST_HANDLER = "Request Handler";
    public static final String RESPONSE_CONFIG_RH_SN = "Service Name";
    public static final String RESPONSE_CONFIG_RH_SN_UPDATE = "Update";
    public static final String RESPONSE_CONFIG_RH_SN_DEFAULTS = "Defaults";
    public static final String RESPONSE_CONFIG_RH_SN_INVARIANTS = "Invariants";
    public static final String RESPONSE_CONFIG_RH_SN_COMPONENTS = "Components";
    public static final String RESPONSE_CONFIG_RH_SN_LAST_COMPONENTS = "Last Components";
    public static final String RESPONSE_CONFIG_QUERY_RESPONSE_WRITER = "Query Response Writer";
    public static final String RESPONSE_CONFIG_QRW_RESPONSE_WRITER = "Response Writer";
    public static final String RESPONSE_CONFIG_SEARCH_COMPONENT = "Search Component";
    public static final String RESPONSE_CONFIG_SC_SPELLCHECK = "Spellchecker";
    public static final String RESPONSE_CONFIG_SC_SUGGEST = "Suggest";
    public static final String RESPONSE_CONFIG_SC_TERM_VECTOR = "Term Vector";
    public static final String RESPONSE_CONFIG_SC_TERMS = "Terms";
    public static final String RESPONSE_CONFIG_SC_ELEVATOR = "Elevator";
    public static final String RESPONSE_CONFIG_SC_HIGHLIGHT = "Highlight";
    public static final String RESPONSE_CONFIG_INIT_PARAMS = "Init Params";
    public static final String RESPONSE_CONFIG_INIT_PARAMETERS = "Init Parameters";
    public static final String RESPONSE_CONFIG_LISTENER = "Listener";
    public static final String RESPONSE_CONFIG_LISTENER_EVENTS = "Listener Events";
    public static final String RESPONSE_CONFIG_DIRECTORY_FACTORY = "Directory Factory";
    public static final String RESPONSE_CONFIG_DIRECTORY_ENTRIES = "Directory Entries";
    public static final String RESPONSE_CONFIG_CODEC_FACTORY = "Codec Factory";
    public static final String RESPONSE_CONFIG_CODEC_ENTRIES = "Codec Entries";
    public static final String RESPONSE_CONFIG_UPDATE_HANDLER_ULOG = "Update Handler Update Log";
    public static final String RESPONSE_CONFIG_UHUL_ENTRIES = "UHUL Entries";
    public static final String RESPONSE_CONFIG_REQUEST_DISPATCHER = "Request Dispatcher";
    public static final String RESPONSE_CONFIG_RD_ENTRIES = "Request Dispatcher Entries";
    public static final String RESPONSE_CONFIG_INDEX_CONFIG = "Index Configuration";
    public static final String RESPONSE_CONFIG_IC_ENTRIES = "Index Configuration Entries";
    public static final String RESPONSE_CONFIG_PEER_SYNC = "Peer Synchronization";
    public static final String RESPONSE_CONFIG_PS_ENTRIES = "Peer Synchronization Entries";

// Solr Config operations.

    public static final String CONFIG_OPERATION_FIELD_NAME = "operation";
    public static final String CONFIG_OPERATION_ADD_REQ_HANDLER = "add-requesthandler";
    public static final String CONFIG_OPERATION_UPD_REQ_HANDLER = "update-requesthandler";
    public static final String CONFIG_OPERATION_DEL_REQ_HANDLER = "delete-requesthandler";
    public static final String CONFIG_OPERATION_ADD_SC_HANDLER = "add-searchcomponent";
    public static final String CONFIG_OPERATION_UPD_SC_HANDLER = "update-searchcomponent";
    public static final String CONFIG_OPERATION_DEL_SC_HANDLER = "delete-searchcomponent";

    private Solr()
    {
    }

    public static boolean isSolrReservedFieldName(String aName)
    {
        if (StringUtils.equals(aName, "_root_"))
            return true;
        else if (StringUtils.equals(aName, "_version_"))
            return true;
        else if (StringUtils.equals(aName, "_src_"))
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

    public static void replaceResponseDocument(Document aSolrDocument, DataTable aResponseTable)
    {
        if ((aSolrDocument != null) && (aResponseTable != null)  & (Solr.isResponsePopulated(aSolrDocument)))
        {
        	DataTable curResponseTable = Solr.getResponse(aSolrDocument);
        	aResponseTable.setName(curResponseTable.getName());
            Document newResponseDocument = new Document(Solr.RESPONSE_DOCUMENT, aResponseTable);
            ArrayList<Relationship> curRelationships = aSolrDocument.getRelationships();
            for (Relationship relationship : curRelationships)
            {
                if (StringUtils.equals(relationship.getType(), Solr.RESPONSE_DOCUMENT))
				{
					ArrayList<Document> documentList = new ArrayList<>();
					documentList.add(newResponseDocument);
					relationship.setDocuments(documentList);
				}
            }
        }
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

    /**
     * This method will replace non-ascii characters and remove non-printable characters from the string.
     *
     * @see <a href="https://howtodoinjava.com/regex/java-clean-ascii-text-non-printable-chars/">Removing Non-printable Characters</a>
     *
     * @param aString aString to clean.
     *
     * @return A cleaned string.
     */
    public static String cleanContent(String aString)
    {
        String cleanString = StringUtils.EMPTY;

        if (StringUtils.isNotEmpty(aString))
        {

// Strips off all non-ASCII characters.

            cleanString = aString.replaceAll("[^\\x00-\\x7F]", "");

// Erases all the ASCII control characters.

            cleanString = cleanString.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");

// Removes non-printable characters from Unicode.

            cleanString = cleanString.replaceAll("\\p{C}", "");

// Removes consecutive white spaces.

            cleanString = cleanString.replaceAll("\\s+", " ");

// Removes leading and trailing spaces.

            cleanString = cleanString.trim();
        }

        return cleanString;
    }

	/**
	 * Escapes a value string used in a Solr query.
	 *
	 * @param aValue String value.
	 *
	 * @return Escaped Solr query value if reserved characters were found.
	 */
	public static String escapeValue(String aValue)
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
}
