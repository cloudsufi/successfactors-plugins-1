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

import com.google.common.base.Strings;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsAccessToken;
import io.cdap.plugin.successfactors.common.util.SuccessFactorsUtil;
import io.cdap.plugin.successfactors.connector.SuccessFactorsConnectorConfig;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * This {@code SuccessFactorsTransporter} class is used to
 * make a rest web service call to the SAP SuccessFactors exposed services.
 */
public class SuccessFactorsTransporter {
  static final String SERVICE_VERSION = "dataserviceversion";
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsTransporter.class);
  private static final long CONNECTION_TIMEOUT = 300;
  private static final long WAIT_TIME = 5;
  private static final long MAX_NUMBER_OF_RETRY_ATTEMPTS = 5;
  private static String accessToken;
  private Response response;
  private final SuccessFactorsConnectorConfig config;

  public SuccessFactorsTransporter(SuccessFactorsConnectorConfig config) {
    this.config = config;
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
   * @return {@code SuccessFactorsResponseContainer}
   * @throws TransportException any http client exceptions are wrapped under it
   */
  public SuccessFactorsResponseContainer callSuccessFactorsEntity(URL endpoint, String mediaType)
    throws TransportException {

    try {
      Response res = transport(endpoint, mediaType);
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
   * @throws TransportException any error while preparing the {@code OkHttpClient}
   */
  public SuccessFactorsResponseContainer callSuccessFactorsWithRetry(URL endpoint, String mediaType,
                                                                     int initialRetryDuration, int maxRetryDuration,
                                                                     int retryMultiplier, int maxRetryCount)
    throws TransportException {
    LOG.debug(
      "Retrying the call to SuccessFactors with initialRetryDuration: {}, maxRetryDuration: {}, retryMultiplier: {}, "
        + "maxRetryCount: {}",
      initialRetryDuration, maxRetryDuration, retryMultiplier, maxRetryCount);
    LOG.debug("Endpoint: {}, MediaType: {}", endpoint, mediaType);
    Response res;
    try {
      res = Failsafe.with(getRetryPolicy(initialRetryDuration, maxRetryDuration, retryMultiplier, maxRetryCount))
        .get(() -> retrySapTransportCall(endpoint, mediaType));
    } catch (FailsafeException e) {
      if (e.getCause() != null) {
        throw new RuntimeException(e.getCause());
      }
      throw e;
    }

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
   */
  public Response retrySapTransportCall(URL endpoint, String mediaType) {
    try {
      response = transport(endpoint, mediaType);
      if (response != null && response.code() >= HttpURLConnection.HTTP_INTERNAL_ERROR) {
        throw new RetryableException();
      }
    } catch (Exception e) {
      LOG.error("Data Recovery failed for URL {}.", endpoint);
      if (e instanceof RetryableException) {
        throw (RetryableException) e;
      } else if (e instanceof IOException) {
        throw new RetryableException("IOException occurred while calling SuccessFactors.");
      } else {
        throw new RuntimeException(e);
      }
    }
    return response;
  }

  private RetryPolicy<Object> getRetryPolicy(int initialRetryDuration, int maxRetryDuration, int retryMultiplier,
                                             int maxRetryCount) {
    return RetryPolicy.builder()
      .handle(RetryableException.class)
      .withBackoff(Duration.ofSeconds(initialRetryDuration),
                   Duration.ofSeconds(maxRetryDuration), retryMultiplier)
      .withMaxRetries(maxRetryCount)
      .onRetry(event -> LOG.debug("Retrying SapTransportCall. Retry count: {}", event.getAttemptCount()))
      .onSuccess(event -> LOG.debug("SapTransportCall executed successfully."))
      .onRetriesExceeded(event -> LOG.error("Retry limit reached for SapTransportCall."))
      .build();
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
    OkHttpClient enhancedOkHttpClient =
      buildConfiguredClient(config.getProxyUrl(), config.getProxyUsername(), config.getProxyPassword());
    Request req;

    if (SuccessFactorsConnectorConfig.BASIC_AUTH.equals(config.getAuthType())) {
      req = buildRequest(endpoint, mediaType);
    } else {
      if (Strings.isNullOrEmpty(accessToken)) {
        accessToken = getAccessToken();
      }
      req = buildRequestWithBearerToken(endpoint, mediaType, accessToken);
      try {
        Response response = enhancedOkHttpClient.newCall(req).execute();
        // If the response code is 403 (Forbidden), attempt to refresh access token
        if (response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
          LOG.info("refreshing access token");
          accessToken = getAccessToken(); // Refresh access token
          req = buildRequestWithBearerToken(endpoint, mediaType, accessToken);
          response = enhancedOkHttpClient.newCall(req).execute();
        }
        return response;
      } catch (IOException e) {
        throw new IOException("Failed to execute the request", e);
      }
    }
    return enhancedOkHttpClient.newCall(req).execute();
  }

  private String getAccessToken() throws IOException {
    SuccessFactorsAccessToken token = new SuccessFactorsAccessToken(config);

    try {
      if (config.getAssertionToken() == null) {
        return token.getAccessToken(token.getAssertionToken());
      } else {
        return token.getAccessToken(config.getAssertionToken());
      }
    } catch (IOException e) {
      throw new IOException("Unable to fetch access token", e);
    }
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
   * Builds and configures an OkHttpClient with the specified proxy settings and authentication credentials.
   * @param proxyUrl The URL of the proxy server (e.g., "http://proxy.example.com:8080").
   * Set to null or an empty string to bypass proxy configuration.
   * @param proxyUsername  The username for proxy authentication. Set to null or an empty string if not required.
   * @param proxyPassword  The password for proxy authentication. Set to null or an empty string if not required.
   * @return An OkHttpClient configured with the specified proxy settings and authentication credentials.
   */
  private OkHttpClient buildConfiguredClient(String proxyUrl, String proxyUsername, String proxyPassword)
    throws MalformedURLException, TransportException {
    OkHttpClient.Builder builder = getConfiguredClient();

    if (SuccessFactorsUtil.isNotNullOrEmpty(proxyUrl)) {
      URL url = new URL(proxyUrl);
      Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(url.getHost(), url.getPort()));
      builder.proxy(proxy);

      if (SuccessFactorsUtil.isNotNullOrEmpty(proxyUsername) && SuccessFactorsUtil.isNotNullOrEmpty(proxyPassword)) {
        builder.proxyAuthenticator(new Authenticator() {
          @Override
          public Request authenticate(Route route, Response response) {
            String credential = Credentials.basic(proxyUsername, proxyPassword);
            return response.request().newBuilder()
              .header("Proxy-Authorization", credential)
              .build();
          }
        });
      }
    }

    return builder.build();
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
      .encodeToString(config.getUsername()
                        .concat(":")
                        .concat(config.getPassword())
                        .getBytes(StandardCharsets.UTF_8)
      );
  }

  private Request buildRequestWithBearerToken(URL endpoint, String mediaType, String accessToken) {
    return new Request.Builder()
      .addHeader("Authorization", "Bearer " + accessToken)
      .addHeader("Accept", mediaType)
      .get()
      .url(endpoint)
      .build();
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
      Response res = transport(endpoint, mediaType);
      return prepareResponseContainer(res);
    } catch (IOException ioe) {
      throw new TransportException(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(), ioe);
    }
  }
}
