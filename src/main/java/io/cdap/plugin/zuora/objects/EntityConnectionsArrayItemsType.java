/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */
package io.cdap.plugin.zuora.objects;

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.zuora.restobjects.annotations.ObjectDefinition;
import io.cdap.plugin.zuora.restobjects.annotations.ObjectFieldDefinition;
import io.cdap.plugin.zuora.restobjects.objects.BaseObject;


import javax.annotation.Nullable;

/**
* Object name: EntityConnectionsArrayItemsType (EntityConnectionsArrayItemsType)
* Related objects: 
**/
@SuppressWarnings("unused")
@ObjectDefinition(
  Name = "EntityConnectionsArrayItemsType",
  ObjectType = ObjectDefinition.ObjectDefinitionType.NESTED
)
public class EntityConnectionsArrayItemsType extends BaseObject {
  /**
  * Name: id (id), Type: string
  * Options (custom, update, select): false, false, false
  **/
  @Nullable
  @SerializedName("id")
  @ObjectFieldDefinition(FieldType = Schema.Type.STRING)
  private String id;

  /**
  * Name: sourceEntityId (sourceEntityId), Type: string
  * Options (custom, update, select): false, false, false
  **/
  @Nullable
  @SerializedName("sourceEntityId")
  @ObjectFieldDefinition(FieldType = Schema.Type.STRING)
  private String sourceEntityId;

  /**
  * Name: status (status), Type: string
  * Options (custom, update, select): false, false, false
  **/
  @Nullable
  @SerializedName("status")
  @ObjectFieldDefinition(FieldType = Schema.Type.STRING)
  private String status;

  /**
  * Name: targetEntityId (targetEntityId), Type: string
  * Options (custom, update, select): false, false, false
  **/
  @Nullable
  @SerializedName("targetEntityId")
  @ObjectFieldDefinition(FieldType = Schema.Type.STRING)
  private String targetEntityId;


  @Override
  public void addFields() {
    addCustomField("id", id, String.class);
    addCustomField("sourceEntityId", sourceEntityId, String.class);
    addCustomField("status", status, String.class);
    addCustomField("targetEntityId", targetEntityId, String.class);
  }
}
