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

package io.cdap.plugin.successfactors.source.transport;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.MediaType;

/**
 * This {@code SuccessFactorsTransporter} class is used to
 * make a rest web service call to the SAP SuccessFactors exposed services.
 */
public class SuccessFactorsTransporter {
  public static final String SERVICE_VERSION = "dataserviceversion";
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsTransporter.class);
  private static final long CONNECTION_TIMEOUT = 300;
  private static final long WAIT_TIME = 5;
  private static final long MAX_NUMBER_OF_RETRY_ATTEMPTS = 5;
  
  private final String username;
  private final String password;
  private Response response;
  
  public SuccessFactorsTransporter(String username, String password) {
    this.username = username;
    this.password = password;

  }

  /**
   * Calls the Successfactors entity for the given URL and returns the respective response.
   * Supported calls are:
   * - testing the URL correctness
   * - fetching the SuccessFactors entity metadata
   * - fetching the total available records count
   *
   * @param endpoint  type of URL
   * @param mediaType mediaType for Accept header property, supported types are 'application/json' & 'application/xml'
   * @param fetchType type of call i.e. TEST / METADATA / COUNT, used for logging purpose.
   * @return {@code SuccessFactorsResponseContainer}
   * @throws TransportException any http client exceptions are wrapped under it
   */
  public SuccessFactorsResponseContainer callSuccessFactorsEntity(URL endpoint, String mediaType, String fetchType)
    throws TransportException {

    try {
      LOG.debug(ResourceConstants.DEBUG_CALL_SERVICE_START.getMsgForKey(fetchType));
      Response res = transport(endpoint, mediaType);
      LOG.debug(ResourceConstants.DEBUG_CALL_SERVICE_END.getMsgForKey(fetchType));

      return prepareResponseContainer(res);
    } catch (IOException ioe) {
      throw new TransportException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(), ioe);
    }
  }

  /**
   * Calls the Successfactors entity to fetch the records with subsequent retries in case of failure.
   * Retry modes are:
   * - any HTTP code equal or above 500
   * - max retry is 5 times
   *
   * @param endpoint record fetch URL
   * @return {@code SuccessFactorsResponseContainer}
   * @throws IOException        any http client exceptions
   * @throws TransportException any error while preparing the {@code OkHttpClient}
   */
  public SuccessFactorsResponseContainer callSuccessFactorsWithRetry(URL endpoint)
    throws IOException, TransportException {

    LOG.debug(ResourceConstants.DEBUG_CALL_SERVICE_START.getMsgForKey(SuccessFactorsService.DATA));
    Response res = retrySapTransportCall(endpoint, MediaType.APPLICATION_JSON);
    LOG.debug(ResourceConstants.DEBUG_CALL_SERVICE_END.getMsgForKey(SuccessFactorsService.DATA));

    try {
      return prepareResponseContainer(res);
    } catch (IOException ioe) {
      res.close();
      throw new TransportException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(), ioe);
    }
  }

  /**
   * Calls the given URL with retry logic.
   *
   * @param endpoint  record fetch URL
   * @param mediaType mediaType for Accept header property
   * @return {@code Response}
   * @throws IOException if all retries fail
   */
  public Response retrySapTransportCall(URL endpoint, String mediaType) throws IOException {
    Callable<Boolean> fetchRecords = () -> {
      response = transport(endpoint, mediaType);
      if (response != null && response.code() >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
        throw new RetryableException();
      }
      return true;
    };

    Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
      .retryIfExceptionOfType(RetryableException.class)
      .withWaitStrategy(WaitStrategies.exponentialWait(WAIT_TIME, TimeUnit.SECONDS))
      .withStopStrategy(StopStrategies.stopAfterAttempt((int) MAX_NUMBER_OF_RETRY_ATTEMPTS))
      .build();

    try {
      retryer.call(fetchRecords);
    } catch (RetryException | ExecutionException e) {
      LOG.error("Data Recovery failed for URL {}.", endpoint);
      throw new IOException();
    }

    return response;
  }

  /**
   * Make an HTTP/S call to the given URL.
   *
   * @param endpoint  SuccessFactors URL
   * @param mediaType mediaType for Accept header property
   * @return {@code Response}
   * @throws IOException        any http client exceptions
   * @throws TransportException any error while preparing the {@code OkHttpClient}
   */
  private Response transport(URL endpoint, String mediaType) throws IOException, TransportException {
    OkHttpClient enhancedOkHttpClient = getConfiguredClient().build();
    Request req = buildRequest(endpoint, mediaType);

    return enhancedOkHttpClient.newCall(req).execute();
  }

  /**
   * Prepares the {@code SuccessFactorsResponseContainer} from the given {@code Response}.
   *
   * @param res {@code Response}
   * @return {@code SuccessFactorsResponseContainer}
   * @throws IOException any IO exception while setting up the response body bytes
   */
  private SuccessFactorsResponseContainer prepareResponseContainer(Response res) throws IOException {
    return SuccessFactorsResponseContainer.builder()
      .httpStatusCode(res.code())
      .httpStatusMsg(res.message())
      .dataServiceVersion(res.header(SERVICE_VERSION))
      .responseStream(res.body() != null ? res.body().bytes() : null)
      .build();
  }

  /**
   * Prepares request for metadata and data calls.
   *
   * @param mediaType supported types 'application/json' & 'application/xml'
   * @return Request
   */
  private Request buildRequest(URL endpoint, String mediaType) {
    return new Request.Builder()
      .addHeader("Authorization", getAuthenticationKey())
      .addHeader("Accept", mediaType)
      .get()
      .url(endpoint)
      .build();
  }

  /**
   * Builds the {@code OkHttpClient.Builder} with following optimized configuration parameters as per the SAP Gateway
   * recommendations.
   * <p>
   * Connection Timeout in seconds: 300
   * Read Timeout in seconds: 300
   * Write Timeout in seconds: 300
   *
   * @return {@code OkHttpClient.Builder}
   */
  private OkHttpClient.Builder getConfiguredClient() throws TransportException {

    // Setting up base timeout of 300 secs as per timeout configuration in SAP to
    // maximize the connection wait time
    OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder()
      .readTimeout(CONNECTION_TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(CONNECTION_TIMEOUT, TimeUnit.SECONDS)
      .connectTimeout(CONNECTION_TIMEOUT, TimeUnit.SECONDS);

    return httpClientBuilder;
  }

  /**
   * Builds the Base64 encoded key for given Basic authorization parameters.
   *
   * @return returns the Base64 encoded username:password
   */
  private String getAuthenticationKey() {
    return "Basic " + Base64.getEncoder()
      .encodeToString(username
                        .concat(":")
                        .concat(password)
                        .getBytes(StandardCharsets.UTF_8)
      );
  }

  /**
   * Calls the SuccessFactors entity for the given URL and returns the respective response.
   * Supported calls are:
   * - testing the URL correctness
   * - fetching the SuccessFactors entity metadata
   * - fetching the total available records count
   *
   * @param endpoint  type of URL
   * @param mediaType mediaType for Accept header property, supported types are 'application/json' & 'application/xml'
   * @param fetchType type of call i.e. TEST / METADATA / COUNT, used for logging purpose.
   * @return {@code SuccessFactorsResponseContainer}
   * @throws TransportException any http client exceptions are wrapped under it
   */
  public SuccessFactorsResponseContainer callSuccessFactors(URL endpoint, String mediaType, String fetchType)
    throws TransportException {

    try {
      LOG.debug(ResourceConstants.DEBUG_CALL_SERVICE_START.getMsgForKey(fetchType));
      Response res = transport(endpoint, mediaType);
      LOG.debug(ResourceConstants.DEBUG_CALL_SERVICE_END.getMsgForKey(fetchType));

      return prepareResponseContainer(res);
    } catch (IOException ioe) {
      throw new TransportException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(), ioe);
    }
  }
}
