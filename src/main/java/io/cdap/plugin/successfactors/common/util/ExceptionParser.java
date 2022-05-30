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

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import io.cdap.plugin.successfactors.common.exception.SuccessFactorsServiceException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.exception.proto.SuccessFactorsError;
import io.cdap.plugin.successfactors.source.transport.SuccessFactorsResponseContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * This {@code ExceptionParser} parse and forms the relevant error messages.
 */
public class ExceptionParser {

  private static final Logger LOG = LoggerFactory.getLogger(ExceptionParser.class);

  public static final int NO_VERSION_FOUND = 1;
  public static final int INVALID_VERSION_FOUND = 2;

  private static final Gson GSON = new Gson();
  private static final String SUPPORTED_DATASERVICE_VERSION = "2.0";

  private ExceptionParser() {
  }

  /**
   * Checks the {@code SuccessFactorsResponseContainer} status code and build appropriate error message
   *
   * @param msg               stage wise error message for example the caller is
   *                          - failure while testing SuccessFactors URL
   *                          - failure while reading metadata
   *                          - failure while reading total available record count
   *                          - failure while reading actual data
   * @param responseContainer {@code SuccessFactorsResponseContainer} contains the http response details after calling
   *                          SAP SuccessFactors service
   * @throws SuccessFactorsServiceException in case of any error scenario, it prepares and throw this exception.
   */
  public static void checkAndThrowException(String msg, SuccessFactorsResponseContainer responseContainer)
    throws SuccessFactorsServiceException {

    String failureMessage = msg;
    SuccessFactorsError error = null;

    if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
      failureMessage += ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey()
        .concat(ResourceConstants.ERR_INVALID_CREDENTIAL.getMsgForKey());

      throw new SuccessFactorsServiceException(failureMessage, responseContainer.getHttpStatusCode());
    } else if (responseContainer.getHttpStatusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      failureMessage += ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey()
        .concat(ResourceConstants.ERR_INVALID_ENTITY_NAME.getMsgForKey());

      throw new SuccessFactorsServiceException(failureMessage, responseContainer.getHttpStatusCode());
    } else if (responseContainer.getHttpStatusCode() != HttpURLConnection.HTTP_OK) {
      String rawResponseString = "";
      InputStream rawStream = responseContainer.getResponseStream();
      if (rawStream != null) {
        try (BufferedReader bufferedReader = new BufferedReader(
          new InputStreamReader(rawStream, StandardCharsets.UTF_8))) {

          rawResponseString = bufferedReader.lines().collect(Collectors.joining(" "));
          // For handling broken hyperlinks to SAP docs in error messages returned by SAP SuccessFactors OData v2 API
          if (rawResponseString.contains("Refer to https")) {
            rawResponseString = rawResponseString.substring(0, rawResponseString.indexOf("Refer to https"));
          }
        } catch (IOException ioe) {
          // no-ops
          LOG.error("Exception thrown while reading input stream ", ioe);
        }
      }

      LOG.error("HTTP Code: {}", responseContainer.getHttpStatusCode());
      LOG.error("Detailed error message: {} {}", msg, rawResponseString);

      try {
        error = GSON.fromJson(rawResponseString, SuccessFactorsError.class);
      } catch (JsonSyntaxException | JsonIOException je) {
        // html errors are only found in case of invalid SuccessFactors service namespace
        if (rawResponseString.startsWith("<html>")) {
          failureMessage += ResourceConstants.ERR_INVALID_ENTITY_NAME.getMsgForKey();
          throw new SuccessFactorsServiceException(failureMessage, responseContainer.getHttpStatusCode());
        }

        // this exception may occur when the rawResponseString contains non-json format such as text | html and in
        // SuccessFactors it is possible to receive text | html format response based on the type of errors.
        failureMessage += ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey().concat(rawResponseString);
      }
      throw new SuccessFactorsServiceException(failureMessage, responseContainer.getHttpStatusCode(), error);
    }

    if (SuccessFactorsUtil.isNullOrEmpty(responseContainer.getDataServiceVersion())) {
      failureMessage += ResourceConstants.ERR_MISSING_DATASERVICE_VERSION.getMsgForKey();
      throw new SuccessFactorsServiceException(failureMessage, NO_VERSION_FOUND);
    }

    if (!responseContainer.getDataServiceVersion().equals(SUPPORTED_DATASERVICE_VERSION)) {
      failureMessage += ResourceConstants.ERR_UNSUPPORTED_VERSION
        .getMsgForKey(responseContainer.getDataServiceVersion(), SUPPORTED_DATASERVICE_VERSION);

      throw new SuccessFactorsServiceException(failureMessage, INVALID_VERSION_FOUND);
    }
  }

  /**
   * Builds a user friendly error message for any {@code TransportException} exception
   *
   * @param te {@code TransportException}
   * @return user friendly error message
   */
  public static String buildTransportError(TransportException te) {
    StringBuilder errorDetails = new StringBuilder()
      .append(te.getMessage())
      .append(" ")
      .append(ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey());

    if (te.getCause() instanceof SocketTimeoutException) {
      errorDetails.append("Connection timeout. Please verify that the given base URL is up and running.");
    } else {
      errorDetails.append(te.getCause().getMessage());
    }

    return errorDetails.toString();
  }

  /**
   * Builds a user friendly error message for any {@code SuccessFactorsServiceException} exception
   *
   * @param ose {@code SuccessFactorsServiceException}
   * @return user friendly error message
   */
  public static String buildSuccessFactorsServiceError(SuccessFactorsServiceException ose) {
    StringBuilder errorDetails = new StringBuilder()
      .append(ose.getMessage())
      .append(" ");

    if (ose.getCause() != null) {
      errorDetails.append(ose.getCause().getMessage());
    }
    if (ose.getSuccessFactorsError() != null && ose.getSuccessFactorsError().getError() != null) {
      errorDetails
        .append(ResourceConstants.ROOT_CAUSE_LOG.getMsgForKey())
        .append(ose.getSuccessFactorsError().getError().getMessage().getValue());
    }

    return errorDetails.toString();
  }
}
