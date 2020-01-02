/**
 * Copyright (C) 2018 BlobCity Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.blobcity.lib.query;

/**
 * Represents valid types of queries supported by the database, along with a query code that is used to identify the query
 *
 * @author sanketsarang
 */
public enum QueryType {
    BULK_SELECT("BULK-SELECT"),
    CONTAINS("CONTAINS"),
    DELETE("DELETE"),
    INSERT("INSERT"),
    SAVE("SAVE"),
    SELECT("SELECT"),
    SELECT_ALL("SELECT-ALL"),
    UPDATE("UPDATE"),

    //TABLE
    CREATE_TABLE("CREATE-TABLE"),
    CREATE_COLLECTION("CREATE-COLLECTION"),
    DROP_COLLECTION("DROP-COLLECTION"),
    DROP_TABLE("DROP-TABLE"),
    LIST_SCHEMA("LIST-COLS"),
    LIST_COLLECTIONS("LIST-COLLECTIONS"),
    LIST_TABLES("LIST-TABLES"),
    RENAME_COLLECTION("RENAME-COLLECTION"),
    RENAME_TABLE("RENAME-TABLE"),
    COLLECTION_EXISTS("COLLECTION-EXISTS"),
    TABLE_EXISTS("TABLE-EXISTS"),
    TRUNCATE_COLLECTION("TRUNCATE-COLLECTION"),
    TRUNCATE_TABLE("TRUNCATE-TABLE"),

    //COLUMN
    ADD_COLUMN("ADD-COLUMN"),
    ALTER_COLUMN("ALTER-COLUMN"),
    CHANGE_DATA_TYPE("CHANGE-DATA-TYPE"),
    DROP_COLUMN("DROP-COLUMN"),
    DROP_INDEX("DROP-INDEX"),
    DROP_UNIQUE("DROP-UNIQUE"),
    INDEX("INDEX"),
    RENAME_COLUMN("RENAME-COLUMN"),

    //SEARCH
    SEARCH("SEARCH"),
    SEARCH_AND("SEARCH-AND"),
    SEARCH_AND_LOAD("SEARCH-AND-LOAD"),
    SEARCH_OR("SEARCH-OR"),
    SEARCH_OR_LOAD("SEARCH-OR-LOAD"),
    SEARCH_FILTERED("SEARCH-FILTERED"),
    INSERT_CUSTOM("INSERT-CUSTOM"),

    //SQL
    SQL("SQL"),

    //DATASTORE
    CREATE_DB("CREATE-DB"),
    CREATE_DS("CREATE-DS"),
    DROP_DB("DROP-DB"),
    DROP_DS("DROP-DS"),
    LIST_DS("LIST-DS"),
    TRUNCATE_DS("TRUNCATE-DS"),
    DS_EXISTS("DS-EXISTS"),

    APPLY_LICENSE("APPLY-LICENSE"),
    LICENSE_STATUS("LICENSE-STATUS"),
    REVOKE_LICENSE("REVOKE-LICENSE"),

    BULK_EXPORT("BULK-EXPORT"),
    BULK_IMPORT("BULK-IMPORT"),
    LIST_OPS("LIST-OPS"),
    RESET_USAGE("RESET-USAGE"),
    SET_LIMITS("SET-LIMITS"),
    USAGE("USAGE"),

    //USER-CODE
    CHANGE_TRIGGER("CHANGE-TRIGGER"),
    LIST_FILTERS("LIST-FILTERS"),
    LIST_TRIGGERS("LIST-TRIGGERS"),
    LOAD_CODE("LOAD-CODE"),
    REGISTER_PROCEDURE("REGISTER-PROCEDURE"),
    REGISTER_TRIGGER("REGISTER-TRIGGER"),
    UNREGISTER_PROCEDURE("UNREGISTER-PROCEDURE"),
    UNREGISTER_TRIGGER("UNREGISTER-TRIGGER"),
    SP("SP"),

    //USER
    ADD_USER("ADD-USER"),
    CHANGE_PASSWORD("CHANGE-PASSWORD"),
    DROP_USER("DROP-USER"),
    VERIFY_CREDENTIALS("VERIFY-CREDENTIALS"),

    //USER GROUPS
    CREATE_GROUP("CREATE-GROUP"),
    DROP_GROUP("DROP-GROUP"),
    ADD_TO_GROUP("ADD-TO-GROUP"),
    REMOVE_FROM_GROUP("REMOVE-FROM-GROUP"),

    // CLUSTER
    NODE_ID("NODE-ID"),
    ADD_NODE("ADD-NODE"),
    LIST_NODES("LIST-NODES"),
    DROP_NODE("DROP-NODE"),

    /**
     * First message sent to a new node, from the cluster. Mentions the network topology of the cluster
     */
    ONB1("ONB1"),
    /**
     * Receives acknowledgement from node that cluster topology is received
     */
    ONB1_ACK("ONB1_ACK"),

    /**
     * Second step, send schema of all datastores and collections within each datastore. The node will replicate this
     * schema structure locally
     */
    ONB2("ONB2"),
    /**
     * New node confirms success / failure of replicating the schema structure
     */
    ONB2_ACK("ONB2_ACK"),

    /**
     * SYSTEMDB data sync. User and SpJars table only
     */
    ONB3("ONB3"),
    /**
     * New node to confirm sync of SYSTEMDB
     */
    ONB3_ACK("ONB3_ACK"),

    /**
     * Sync stored procedure jars to the new node, so that that stored procedures can be loaded onto the new node
     */
    ONB4("ONB4"),
    /**
     * New node to confirm back with receiving jars and loading them
     */
    ONB4_ACK("ONB4_ACK"),

    /**
     * Inform other nodes to add this node and verify a connection with the node
     */
    ONB5_1("ONB5_1"),

    /**
     * Inform new node of the complete current active node list, and ask it to confirm back when it receives a connection
     * from each of the nodes
     */
    ONB5_2("ONB5_2"),

    ONB5_1_ACK("ONB5_1_ACK"),
    ONB5_2_ACK("ONB5_2_ACK"),

    /**
     * Broadcast to all nodes that node onboarding was successful. No response necessary. The node will change status
     * to CONNECTED if it receives the message, and will remain in SYNCING state if does not receive the message.
     * A SYNCING state is auto converted to DOWN state if no action post ONB5_2 for 30 seconds
     */
    ONB_SUCCESS("ONB_SUCCESS"),


    // INTERNAL QUERIES
    ROLLBACK("R"),
    COMMIT("C"), //this is a confirmation to commit the soft commit
    SOFT_COMMIT_SUCCESS("SCS"),
    COMMIT_SUCCESS("CS"),
    ROLLBACK_SUCCESS("RS"),
    QUERY_RESPONSE("QR"), //response for select / read queries
    PING("PING"), //ping on on-going commit to check if thigns are still running
    MEM_FLUSH("MEM-FLUSH"); //flush a memory table to disk

    final String queryCode;

    QueryType(final String queryCode) {
        this.queryCode = queryCode;
    }

    public final String getQueryCode() {
        return queryCode;
    }

    public static QueryType fromString(final String commandString) {
        for (QueryType queryType : QueryType.values()) {
            if (queryType.getQueryCode().equalsIgnoreCase(commandString)) {
                return queryType;
            }
        }

        return null;
    }
}
