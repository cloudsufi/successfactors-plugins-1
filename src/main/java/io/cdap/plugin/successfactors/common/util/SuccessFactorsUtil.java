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

import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsTransporter;

import javax.annotation.Nullable;

/**
 * SuccessFactors Utility Class
 */
public class SuccessFactorsUtil {

  private SuccessFactorsUtil() {
  }

  /**
   * Checks a {@code CharSequence} instance for {@code NOT null && NOT empty}.
   *
   * @param charSeq which needs to be checked
   * @return the boolean result of
   * {@code (charSeq != null && !charSeq.toString().isEmpty())}
   */
  public static boolean isNotNullOrEmpty(@Nullable CharSequence charSeq) {
    return charSeq != null && !charSeq.toString().isEmpty();
  }

  /**
   * Checks a {@code CharSequence} instance for {@code null || empty}.
   *
   * @param charSeq which needs to be checked
   * @return the boolean result of
   * {@code (charSeq == null || charSeq.toString().isEmpty())}
   */
  public static boolean isNullOrEmpty(@Nullable CharSequence charSeq) {
    return !isNotNullOrEmpty(charSeq);
  }

  /**
   * Removes any linebreak from the given string
   *
   * @param rawString
   * @return refactored String or null
   */
  public static String removeLinebreak(String rawString) {
    if (isNotNullOrEmpty(rawString)) {
      return rawString.replaceAll("[\n\r]", " ");
    }
    return rawString;
  }

  /**
   * Removes any whitespace character (spaces, tabs, line breaks) from the given string.
   *
   * @param rawString
   * @return refactored String or null
   */
  public static String removeWhitespace(String rawString) {
    if (isNotNullOrEmpty(rawString)) {
      return rawString.replaceAll("\\s", "");
    }
    return rawString;
  }

  /**
   * Trim whitespace from the beginning and end of a string.
   *
   * @param rawString
   * @return trimmed String or null
   */
  public static String trim(String rawString) {
    if (SuccessFactorsUtil.isNotNullOrEmpty(rawString)) {
      return rawString.trim();
    }
    return rawString;
  }

  /**
   * Get the SuccessFactorsService instance.
   *
   * @param pluginConfig
   * @return SuccessFactorsService instance
   */
  public static SuccessFactorsService getSuccessFactorsService(SuccessFactorsPluginConfig pluginConfig) {
    SuccessFactorsTransporter transporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    SuccessFactorsService successFactorsService = new SuccessFactorsService(pluginConfig, transporter);
    return successFactorsService;
  }
}
