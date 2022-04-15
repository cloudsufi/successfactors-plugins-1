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
package io.cdap.plugin.successfactors.source.metadata;

import com.google.gson.Gson;
import io.cdap.plugin.successfactors.common.exception.proto.SuccessFactorsError;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class TestSuccessFactorsUtil {
  public static InputStream readResource(String resourceName) {
    return Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
  }

  @Nullable
  public static String convertInputStreamToString(InputStream responseStream) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining(""));
    } catch (IOException ioe) {
      //op-op
    }
    return null;
  }

  @Test
  public void testSuccessFactorsErrorStructure() {
    String errorJson = TestSuccessFactorsUtil.convertInputStreamToString(TestSuccessFactorsUtil
      .readResource("successfactors-error.json"));

    SuccessFactorsError successFactorsError = new Gson().fromJson(errorJson, SuccessFactorsError.class);

    Assert.assertNotNull(successFactorsError.getError().getInnerError());
    Assert.assertEquals("C83CB3D2A1420000E00609D31E196BD4", successFactorsError.getError().getInnerError()
      .getTransactionId());

    Assert.assertNotNull(successFactorsError.getError().getInnerError().getApplication());
    Assert.assertEquals("ZPURCHASEORDER_SRV_X", successFactorsError.getError().getInnerError().getApplication()
      .getServiceId());

    Assert.assertNotNull(successFactorsError.getError().getInnerError().getErrorResolution());
    Assert.assertEquals("See SAP Note 1797736 for error analysis (https://service.sap.com/sap/support/notes/1797736)",
                        successFactorsError.getError().getInnerError().getErrorResolution().getSapNote());

    Assert.assertNotNull(successFactorsError.getError().getInnerError().getInnerErrorDetails());
    Assert.assertEquals("'TEAM_012345678' is not a valid ID.",
                        successFactorsError.getError().getInnerError().getInnerErrorDetails().get(0).getMessage());
  }
}
