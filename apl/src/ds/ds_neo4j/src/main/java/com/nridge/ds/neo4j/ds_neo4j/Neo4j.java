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

package com.nridge.ds.neo4j.ds_neo4j;

/**
 * The Neo4j class captures the constants, enumerated types
 * and utility methods for the Neo4j data source package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Neo4j
{
    public static final String CFG_PROPERTY_PREFIX = "ds.neo4j";

// Criteria field names.

    public static final String FIELD_PREFIX = "ds_neo4j_";
    public static final String FIELD_NODE_ID = "ds_neo4j_node_id";
    public static final String FIELD_REL_NAME = "ds_neo4j_rel_name";
    public static final String FIELD_REL_DEPTH = "ds_neo4j_rel_depth";
    public static final String FIELD_RESOLVE_TO = "ds_neo4j_resolve_to";
    public static final String FIELD_REL_DIRECTION = "ds_neo4j_rel_direction";
    public static final String FIELD_REL_TRAVERSAL = "ds_neo4j_rel_traversal";
    public static final String FIELD_REL_UNIQUENESS = "ds_neo4j_rel_uniqueness";

    public static final int LIMIT_UNLIMITED = 0;

    public static final String DIRECTION_BOTH = "Both";
    public static final String DIRECTION_INBOUND = "Inbound";
    public static final String DIRECTION_OUTBOUND = "Outbound";

    public static final String TRAVERSAL_DEPTH_FIRST = "DepthFirst";
    public static final String TRAVERSAL_BREADTH_FIRST = "BreathFirst";

    public static final String RESOLVE_TO_NODE_LIST = "NodeList";           // List of nodes (no relationships)
    public static final String RESOLVE_TO_NODE_TREE = "NodeTree";           // List of nodes (include relationships)
    public static final String RESOLVE_TO_RELATIONSHIP_LIST = "RelList";    // List of relationships (no nodes)

// The following are derived directly from Neo4j JavaDoc Uniqueness class.  They are described there.

    public static final String UNIQUE_NONE = "NONE";
    public static final String UNIQUE_NODE_PATH = "NODE_PATH";
    public static final String UNIQUE_NODE_LEVEL = "NODE_LEVEL";
    public static final String UNIQUE_NODE_GLOBAL = "NODE_GLOBAL";
    public static final String UNIQUE_RELATIONSHIP_PATH = "RELATIONSHIP_PATH";
    public static final String UNIQUE_RELATIONSHIP_LEVEL = "RELATIONSHIP_LEVEL";
    public static final String UNIQUE_RELATIONSHIP_GLOBAL = "RELATIONSHIP_GLOBAL";

    public static final String DOCUMENT_TYPE = "Neo4j Document";

    public static final int STATUS_CODE_SUCCESS = 0;
    public static final int STATUS_CODE_FAILURE = 1;
    public static final String STATUS_MESSAGE_SUCCESS = "Successfully completed operation.";

// Application service constants.

    public static final String CONTENT_TYPE_XML = "application/xml";
    public static final String CONTENT_TYPE_JSON = "application/json";

    public static final String ENCRYPTION_LEVEL_NONE = "none";
    public static final String ENCRYPTION_LEVEL_STANDARD = "standard";

// Neo4j response document relationships.

    public static final String RESPONSE_HEADER = "Header";
    public static final String RESPONSE_DOCUMENT = "Document";

    private Neo4j()
    {
    }
}
