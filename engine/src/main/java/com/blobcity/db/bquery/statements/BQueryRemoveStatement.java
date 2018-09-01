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

package com.blobcity.db.bquery.statements;

import com.blobcity.db.constants.BQueryCommands;
import com.blobcity.db.constants.BQueryParameters;
import com.blobcity.util.json.Jsonable;
import org.json.JSONObject;

/**
 *
 * @author sanketsarang
 */
public class BQueryRemoveStatement implements Jsonable {
    
    private String app;
    private String table;
    private String pk;
    
    public BQueryRemoveStatement() {
        //do nothing
    }
    
    public BQueryRemoveStatement(final String app, final String table, final String pk) {
        this.app = app;
        this.table = table;
        this.pk = pk;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }
    
    @Override
    public JSONObject toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(BQueryParameters.ACCOUNT, app);
        jsonObject.put(BQueryParameters.TABLE, table);
        jsonObject.put(BQueryParameters.QUERY, BQueryCommands.DELETE.getCommand());
        jsonObject.put(BQueryParameters.PRIMARY_KEY, pk);
        return jsonObject;
    }

    public static BQueryRemoveStatement fromJson(JSONObject jsonObject) {
        BQueryRemoveStatement bQuerySelectStatement = new BQueryRemoveStatement();
        bQuerySelectStatement.setApp(jsonObject.getString(BQueryParameters.ACCOUNT));
        bQuerySelectStatement.setTable(jsonObject.getString(BQueryParameters.TABLE));
        bQuerySelectStatement.setPk(jsonObject.getString(BQueryParameters.PRIMARY_KEY));
        return bQuerySelectStatement;
    }
}
