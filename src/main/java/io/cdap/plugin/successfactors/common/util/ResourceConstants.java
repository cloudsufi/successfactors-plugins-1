/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.successfactors.common.util;

import javax.annotation.Nullable;

/**
 * ResourceConstants class
 */
public enum ResourceConstants {

  ERR_MISSING_PARAM_PREFIX(null, "err.missing.param.prefix"),
  ERR_INVALID_EXPIRE_TIME(null, "err.invalid.expire.time.prefix"),
  ERR_MISSING_PARAM_OR_MACRO_ACTION(null, "err.missing.param.or.macro.action"),
  ERR_INVALID_BASE_URL(null, "err.invalid.base.url"),
  ERR_FEATURE_NOT_SUPPORTED("CDF_SAP_ODATA_01500", "err.feature.not.supported"),
  ERR_INVALID_ENTITY_CALL(null, "err.invalid.entityCall"),
  ROOT_CAUSE_LOG(null, "root.cause.log"),
  ERR_ODATA_SERVICE_CALL("CDF_SAP_ODATA_01532", "err.odata.service.call"),
  ERR_ODATA_ENTITY_FAILURE("CDF_SAP_ODATA_01534", "err.odata.entity.failure"),
  ERR_INVALID_CREDENTIAL(null, "err.invalid.credential"),
  ERR_UNSUPPORTED_VERSION("CDF_SAP_ODATA_01501", "err.unsupported.version"),
  ERR_MISSING_DATASERVICE_VERSION("CDF_SAP_ODATA_01502", "err.missing.dataservice.version"),
  ERR_INVALID_ENTITY_NAME("", "err.invalid.entity.name"),
  ERR_CALL_SERVICE_FAILURE(null, "err.call.service.failure"),
  ERR_FAILED_ENTITY_VALIDATION(null, "err.failed.entity.validation"),
  ERR_CHECK_ADVANCED_PARAM(null, "err.check.advanced.parameter"),
  ERR_NOT_FOUND(null, "err.resource.not.found"),
  ERR_NO_COLUMN_FOUND(null, "err.no.column.found"),
  ERR_UNSUPPORTED_ASSOCIATED_ENTITY(null, "err.unsupported.associated.entity"),
  ERR_BUILDING_COLUMNS(null, "err.building.columns"),
  DEBUG_NAVIGATION_NOT_FOUND(null, "debug.navigation.not.found"),
  DEBUG_NAV_PROP_NOT_FOUND(null, "debug.nav.prop.not.found"),
  DEBUG_ENTITY_NOT_FOUND(null, "debug.entity.not.found"),
  ERR_READING_METADATA(null, "err.reading.metadata"),
  ERR_MACRO_INPUT("CDF_SAP_SUCCESSFACTORS_01505", "err.macro.input"),
  ERR_NO_RECORD_FOUND("CDF_SAP_SUCCESSFACTORS_01506", "err.no.record.found"),
  ERR_FETCH_RECORD_COUNT("CDF_SAP_SUCCESSFACTORS_01503", "err.fetch.record.count"),
  ERR_METADATA_ENCODED_STRING("CDF_SAP_SUCCESSFACTORS_01504", "err.metadata.encoded.string"),
  ERR_METADATA_DECODE("CDF_SAP_SUCCESSFACTORS_01533", "err.metadata.decode"),
  ERR_RECORD_PULL("CDF_SAP_SUCCESSFACTORS_01536", "err.record.pull"),
  ERR_RECORD_PROCESSING("CDF_SAP_SUCCESSFACTORS_01537", "err.record.processing");

  private final String code;
  private final String key;

  ResourceConstants(String code, String key) {
    this.code = code;
    this.key = key;
  }

  @Nullable
  public String getCode() {
    return code;
  }

  public String getKey() {
    return key;
  }

  public String getMsgForKeyWithCode() {
    return getMsgForKey(code);
  }

  public String getMsgForKeyWithCode(Object... params) {
    Object[] destArr = new Object[params.length + 1];
    destArr[0] = code;
    System.arraycopy(params, 0, destArr, 1, params.length);

    return getMsgForKey(destArr);
  }

  public String getMsgForKey() {
    return ResourceText.getString(key);
  }

  public String getMsgForKey(Object... params) {
    return ResourceText.getString(key, params);
  }
}
