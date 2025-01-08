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

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.plugin.successfactors.common.exception.TransportException;
import io.cdap.plugin.successfactors.common.util.ResourceConstants;
import io.cdap.plugin.successfactors.source.config.SuccessFactorsPluginConfig;
import io.cdap.plugin.successfactors.source.metadata.TestSuccessFactorsUtil;
import io.cdap.plugin.successfactors.source.service.SuccessFactorsService;
import mockit.Mock;
import mockit.MockUp;
import okhttp3.OkHttpClient;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.core.MediaType;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SuccessFactorsTransporterTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort().dynamicHttpsPort());
  private SuccessFactorsPluginConfig.Builder pluginConfigBuilder;
  private SuccessFactorsTransporter transporter;
  private SuccessFactorsPluginConfig pluginConfig;
  private SuccessFactorsUrlContainer successFactorsURL;
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsTransporterTest.class);

  @BeforeClass
  public static void classSetup() {
    new MockUp<SuccessFactorsTransporter>() {
      @Mock
      public OkHttpClient buildConfiguredClient(String proxyUrl, String proxyUsername, String proxyPassword) {
        return allowAllSSL().build();
      }
    };
  }

  private static OkHttpClient.Builder allowAllSSL() {
    TrustManager[] trustAllCerts = new TrustManager[]{
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[]{};
        }
      }
    };

    try {
      SSLContext trustAllSslContext = SSLContext.getInstance("SSL");
      trustAllSslContext.init(null, trustAllCerts, new java.security.SecureRandom());
      SSLSocketFactory trustAllSslSocketFactory = trustAllSslContext.getSocketFactory();

      HostnameVerifier customHostVerifier = new HostnameVerifier() {
        @Override
        public boolean verify(String hostname, SSLSession session) {
          return true;
        }
      };

      OkHttpClient.Builder builder = new OkHttpClient().newBuilder();
      builder.sslSocketFactory(trustAllSslSocketFactory, (X509TrustManager) trustAllCerts[0]);
      builder.hostnameVerifier(customHostVerifier);
      return builder;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      LOG.error("Failed due to exception", e);
      return null;
    }
  }

  @Before
  public void setUp() {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .baseURL("https://localhost:" + wireMockRule.httpsPort())
      .entityName("Entity")
      .username("test")
      .password("secret")
      .authType("basicAuth")
      .expandOption("Products/Supplier");
    pluginConfig = pluginConfigBuilder.build();
    successFactorsURL = new SuccessFactorsUrlContainer(pluginConfig);
    transporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
  }

  @Test
  public void testCallSuccessFactors() throws TransportException {
    String expectedBody = "{\"d\": [{\"ID\": 0,\"Name\": \"Bread\"}}]}";
    WireMock.stubFor(WireMock.get("/Entity?%24expand=Products%2FSupplier&%24top=1")
                       .withBasicAuth(pluginConfig.getConnection().getUsername(),
                                      pluginConfig.getConnection().getPassword())
                       .willReturn(WireMock.ok()
                                     .withHeader(SuccessFactorsTransporter.SERVICE_VERSION, "2.0")
                                     .withBody(expectedBody)));
    SuccessFactorsResponseContainer response = transporter
      .callSuccessFactors(successFactorsURL.getTesterURL(), MediaType.APPLICATION_JSON, SuccessFactorsService.TEST);

    Assert.assertEquals("SuccessFactors Service data version is not same.",
                        "2.0",
                        response.getDataServiceVersion());
    Assert.assertEquals("HTTP status code is not same.",
                        HttpURLConnection.HTTP_OK,
                        response.getHttpStatusCode());
    Assert.assertEquals("HTTP response body is not same.",
                        expectedBody,
                        TestSuccessFactorsUtil.convertInputStreamToString(response.getResponseStream()));
    Assert.assertEquals("HTTP status is not same", "OK", response.getHttpStatusMsg());
  }

  @Test
  public void testCallSuccessFactorsWithOauth2() throws TransportException {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .baseURL("https://localhost:" + wireMockRule.httpsPort())
      .entityName("Entity")
      .username("test")
      .password("secret")
      .authType("oAuth2")
      .expandOption("Products/Supplier");
    pluginConfig = pluginConfigBuilder.build();
    String expectedBody = "{\"d\": [{\"ID\": 0,\"Name\": \"Bread\"}}]}";
    WireMock.stubFor(WireMock.get("/Entity?%24expand=Products%2FSupplier&%24top=1")
      .withBasicAuth(pluginConfig.getConnection().getUsername(),
        pluginConfig.getConnection().getPassword())
      .willReturn(WireMock.ok()
        .withHeader(SuccessFactorsTransporter.SERVICE_VERSION, "2.0")
        .withBody(expectedBody)));
    SuccessFactorsResponseContainer response = transporter
      .callSuccessFactors(successFactorsURL.getTesterURL(), MediaType.APPLICATION_JSON, SuccessFactorsService.TEST);

    Assert.assertEquals("SuccessFactors Service data version is not same.",
      "2.0",
      response.getDataServiceVersion());
    Assert.assertEquals("HTTP status code is not same.",
      HttpURLConnection.HTTP_OK,
      response.getHttpStatusCode());
    Assert.assertEquals("HTTP response body is not same.",
      expectedBody,
      TestSuccessFactorsUtil.convertInputStreamToString(response.getResponseStream()));
    Assert.assertEquals("HTTP status is not same", "OK", response.getHttpStatusMsg());
  }

  @Test
  public void testCallSuccessFactorsWithProxy() throws TransportException {
    pluginConfigBuilder = SuccessFactorsPluginConfig.builder()
      .baseURL("https://localhost:" + wireMockRule.httpsPort())
      .entityName("Entity")
      .username("test")
      .password("secret")
      .proxyUrl("https://proxy")
      .proxyUsername("user")
      .proxyPassword("password")
      .expandOption("Products/Supplier");
    pluginConfig = pluginConfigBuilder.build();
    successFactorsURL = new SuccessFactorsUrlContainer(pluginConfig);
    transporter = new SuccessFactorsTransporter(pluginConfig.getConnection());
    String expectedBody = "{\"d\": [{\"ID\": 0,\"Name\": \"Bread\"}}]}";
    WireMock.stubFor(WireMock.get("/Entity?%24expand=Products%2FSupplier&%24top=1")
      .withBasicAuth(pluginConfig.getConnection().getUsername(),
        pluginConfig.getConnection().getPassword())
      .willReturn(WireMock.ok()
        .withHeader(SuccessFactorsTransporter.SERVICE_VERSION, "2.0")
        .withBody(expectedBody)));
    SuccessFactorsResponseContainer response = transporter
      .callSuccessFactors(successFactorsURL.getTesterURL(), MediaType.APPLICATION_JSON, SuccessFactorsService.TEST);

    Assert.assertEquals("SuccessFactors Service data version is not same.",
      "2.0",
      response.getDataServiceVersion());
    Assert.assertEquals("HTTP status code is not same.",
      HttpURLConnection.HTTP_OK,
      response.getHttpStatusCode());
    Assert.assertEquals("HTTP response body is not same.",
      expectedBody,
      TestSuccessFactorsUtil.convertInputStreamToString(response.getResponseStream()));
    Assert.assertEquals("HTTP status is not same", "OK", response.getHttpStatusMsg());
  }

  @Test
  public void testUnAuthorized() throws TransportException {
    WireMock.stubFor(WireMock.get("/Entity/$metadata")
                       .willReturn(WireMock.unauthorized()));
    SuccessFactorsResponseContainer response = transporter
      .callSuccessFactors(successFactorsURL.getMetadataURL(), MediaType.APPLICATION_XML,
                          SuccessFactorsService.METADATA);
    WireMock.verify(1, WireMock.getRequestedFor(WireMock.urlEqualTo("/Entity/$metadata")));
    Assert.assertEquals("HTTP status code is not matching.",
                        HttpURLConnection.HTTP_UNAUTHORIZED,
                        response.getHttpStatusCode());
  }

  @Test
  public void testInvalidHost() throws TransportException {
    successFactorsURL = new SuccessFactorsUrlContainer(pluginConfigBuilder.baseURL("http://INVALID-HOST").build());
    exception.expectCause(CoreMatchers.isA(UnknownHostException.class));
    transporter.callSuccessFactors(successFactorsURL.getMetadataURL(), MediaType.APPLICATION_XML,
                                   SuccessFactorsService.METADATA);
  }

  @Test
  public void testConnectionTimeout() throws TransportException {
    WireMock.stubFor(WireMock.get("/Entity?%24expand=Products%2FSupplier&%24top=1")
                       .willReturn(WireMock.aResponse()
                                     .withFixedDelay(61_000)));

    exception.expectMessage(ResourceConstants.ERR_CALL_SERVICE_FAILURE.getMsgForKey(SuccessFactorsService.TEST));
    exception.expectCause(CoreMatchers.isA(SocketTimeoutException.class));
    transporter.callSuccessFactors(successFactorsURL.getTesterURL(), MediaType.APPLICATION_JSON,
                                   SuccessFactorsService.TEST);
  }

  @Test(expected = RetryableException.class)
  public void testValidNumberOfRetry() throws Exception {
    SuccessFactorsTransporter transporterSpy = Mockito.spy(transporter);
    int retryCount = 2;
    try {
      transporterSpy.callSuccessFactorsWithRetry(new URL("http://127.0.0.1/"), "TEST",
                                                 1, 3, 2, retryCount);
    } finally {
      verify(transporterSpy, times(retryCount + 1))
        .retrySapTransportCall(Mockito.any(URL.class), Mockito.anyString());
    }
  }
}
