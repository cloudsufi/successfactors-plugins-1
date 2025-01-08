/*
 * Copyright © 2025 Cask Data, Inc.
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
import com.google.gson.JsonObject;
import io.cdap.plugin.successfactors.connector.SuccessFactorsConnectorConfig;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.joda.time.DateTime;
import org.opensaml.Configuration;
import org.opensaml.DefaultBootstrap;
import org.opensaml.common.SAMLVersion;
import org.opensaml.saml1.core.NameIdentifier;
import org.opensaml.saml2.core.Assertion;
import org.opensaml.saml2.core.Attribute;
import org.opensaml.saml2.core.AttributeStatement;
import org.opensaml.saml2.core.AttributeValue;
import org.opensaml.saml2.core.Audience;
import org.opensaml.saml2.core.AudienceRestriction;
import org.opensaml.saml2.core.AuthnContext;
import org.opensaml.saml2.core.AuthnContextClassRef;
import org.opensaml.saml2.core.AuthnStatement;
import org.opensaml.saml2.core.Conditions;
import org.opensaml.saml2.core.Issuer;
import org.opensaml.saml2.core.NameID;
import org.opensaml.saml2.core.Subject;
import org.opensaml.saml2.core.SubjectConfirmation;
import org.opensaml.saml2.core.SubjectConfirmationData;
import org.opensaml.saml2.core.impl.AssertionMarshaller;
import org.opensaml.xml.ConfigurationException;
import org.opensaml.xml.Namespace;
import org.opensaml.xml.XMLObjectBuilder;
import org.opensaml.xml.io.MarshallingException;
import org.opensaml.xml.schema.XSString;
import org.opensaml.xml.schema.impl.XSStringBuilder;
import org.opensaml.xml.security.SecurityConfiguration;
import org.opensaml.xml.security.SecurityException;
import org.opensaml.xml.security.SecurityHelper;
import org.opensaml.xml.security.x509.BasicX509Credential;
import org.opensaml.xml.signature.Signature;
import org.opensaml.xml.signature.SignatureConstants;
import org.opensaml.xml.signature.SignatureException;
import org.opensaml.xml.signature.Signer;
import org.opensaml.xml.util.XMLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.List;
import java.util.UUID;

import javax.xml.namespace.QName;


/**
 * AccessToken class
 */
public class SuccessFactorsAccessToken {
  private static final Logger LOG = LoggerFactory.getLogger(SuccessFactorsAccessToken.class);
  private final SuccessFactorsConnectorConfig config;
  private final Gson gson = new Gson();


  public SuccessFactorsAccessToken(SuccessFactorsConnectorConfig config) {
    this.config = config;
  }

  /**
   * Generates a signed SAML assertion for authentication purposes.
   *
   * @param clientId            The client ID associated with the application.
   * @param username            The username of the user for whom the assertion is generated.
   * @param tokenUrl            The URL for obtaining the authentication token.
   * @param privateKeyString    The private key used for signing the assertion.
   * @param expireInMinutes     The validity period of the assertion in minutes.
   * @param userUserNameAsUserId A boolean indicating whether to use the username as the User ID in the assertion.
   * @return The signed SAML assertion as a string.
   * @throws Exception If an error occurs during the generation or signing of the SAML assertion.
   */
  public static String generateSignedSAMLAssertion(String clientId, String username, String tokenUrl,
                                                   String privateKeyString, int expireInMinutes,
                                                   boolean userUserNameAsUserId) {

    Assertion unsignedAssertion = buildDefaultAssertion(clientId, username, tokenUrl, expireInMinutes,
      userUserNameAsUserId);
    PrivateKey privateKey = generatePrivateKey(privateKeyString);
    Assertion assertion = sign(unsignedAssertion, privateKey);
    String signedAssertion = getSAMLAssertionString(assertion);

    return signedAssertion;
  }

  /**
   * Builds a default SAML assertion with specified parameters for authentication purposes.
   *
   * @param clientId              The client ID associated with the application.
   * @param userId                The user ID for whom the assertion is generated.
   * @param tokenUrl              The URL for obtaining the authentication token.
   * @param expireInMinutes       The validity period of the assertion in minutes.
   * @param userUserNameAsUserId  A boolean indicating whether to use the username as the User ID in the assertion.
   * @return The constructed SAML assertion.
   * @throws RuntimeException if an error occurs during the construction of the SAML assertion.
   */
  private static Assertion buildDefaultAssertion(String clientId, String userId, String tokenUrl, int expireInMinutes,
                                                 boolean userUserNameAsUserId) {
    try {
      DateTime currentTime = new DateTime();
      DefaultBootstrap.bootstrap();

      // Create the assertion and set Id, namespace etc.
      Assertion assertion = create(Assertion.class, Assertion.DEFAULT_ELEMENT_NAME);
      assertion.setIssueInstant(currentTime);
      assertion.setID(UUID.randomUUID().toString());
      assertion.setVersion(SAMLVersion.VERSION_20);
      Namespace xsNS = new Namespace("http://www.w3.org/2001/XMLSchema", "xs");
      assertion.addNamespace(xsNS);
      Namespace xsiNS = new Namespace("http://www.w3.org/2001/XMLSchema-instance", "xsi");
      assertion.addNamespace(xsiNS);

      Issuer issuer = create(Issuer.class, Issuer.DEFAULT_ELEMENT_NAME);
      issuer.setValue("www.successfactors.com");
      assertion.setIssuer(issuer);

      // Create the subject and add it to assertion
      Subject subject = create(Subject.class, Subject.DEFAULT_ELEMENT_NAME);
      NameID nameID = create(NameID.class, NameID.DEFAULT_ELEMENT_NAME);
      nameID.setValue(userId);
      nameID.setFormat(NameIdentifier.UNSPECIFIED);
      subject.setNameID(nameID);
      SubjectConfirmation subjectConfirmation = create(SubjectConfirmation.class,
        SubjectConfirmation.DEFAULT_ELEMENT_NAME);
      subjectConfirmation.setMethod("urn:oasis:names:tc:SAML:2.0:cm:bearer");
      SubjectConfirmationData sconfData = create(SubjectConfirmationData.class,
        SubjectConfirmationData.DEFAULT_ELEMENT_NAME);
      sconfData.setNotOnOrAfter(currentTime.plusMinutes(expireInMinutes));
      sconfData.setRecipient(tokenUrl);
      subjectConfirmation.setSubjectConfirmationData(sconfData);
      subject.getSubjectConfirmations().add(subjectConfirmation);
      assertion.setSubject(subject);

      // Create the Conditions
      Conditions conditions = buildConditions(currentTime, expireInMinutes);

      AudienceRestriction audienceRestriction = create(AudienceRestriction.class,
        AudienceRestriction.DEFAULT_ELEMENT_NAME);
      Audience audience = create(Audience.class, Audience.DEFAULT_ELEMENT_NAME);
      audience.setAudienceURI("www.successfactors.com");
      List<Audience> audienceList = audienceRestriction.getAudiences();
      audienceList.add(audience);
      List<AudienceRestriction> audienceRestrictions = conditions.getAudienceRestrictions();
      audienceRestrictions.add(audienceRestriction);
      assertion.setConditions(conditions);

      // Create the AuthnStatement and add it to assertion
      AuthnStatement authnStatement = create(AuthnStatement.class, AuthnStatement.DEFAULT_ELEMENT_NAME);
      authnStatement.setAuthnInstant(currentTime);
      authnStatement.setSessionIndex(UUID.randomUUID().toString());
      AuthnContext authContext = create(AuthnContext.class, AuthnContext.DEFAULT_ELEMENT_NAME);
      AuthnContextClassRef authnContextClassRef = create(AuthnContextClassRef.class,
        AuthnContextClassRef.DEFAULT_ELEMENT_NAME);
      authnContextClassRef.setAuthnContextClassRef(AuthnContext.PPT_AUTHN_CTX);
      authContext.setAuthnContextClassRef(authnContextClassRef);
      authnStatement.setAuthnContext(authContext);
      assertion.getAuthnStatements().add(authnStatement);

      // Create the attribute statement
      AttributeStatement attributeStatement = create(AttributeStatement.class,
        AttributeStatement.DEFAULT_ELEMENT_NAME);
      Attribute apiKeyAttribute = createAttribute("api_key", clientId);
      attributeStatement.getAttributes().add(apiKeyAttribute);
      assertion.getAttributeStatements().add(attributeStatement);

      // Set user_username as true while using username as userId
      if (userUserNameAsUserId) {
        AttributeStatement useUserNameAsUserIdStatement = create(AttributeStatement.class,
          AttributeStatement.DEFAULT_ELEMENT_NAME);
        Attribute useUserNameKeyAttribute = createAttribute("use_username", "true");
        useUserNameAsUserIdStatement.getAttributes().add(useUserNameKeyAttribute);
        assertion.getAttributeStatements().add(useUserNameAsUserIdStatement);
      }

      return assertion;
    } catch (ConfigurationException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * helper method to create open saml objects.
   * @param cls   class type
   * @param qname qualified name
   * @param <T>   class type
   * @return the saml object
   */
  @SuppressWarnings("unchecked")
  public static <T> T create(Class<T> cls, QName qname) {
    return (T) ((XMLObjectBuilder) Configuration.getBuilderFactory().getBuilder(qname)).buildObject(qname);
  }

  private static Attribute createAttribute(String name, String value) {
    Attribute result = create(Attribute.class, Attribute.DEFAULT_ELEMENT_NAME);
    result.setName(name);
    XSStringBuilder stringBuilder = (XSStringBuilder) Configuration.getBuilderFactory()
      .getBuilder(XSString.TYPE_NAME);
    XSString stringValue = stringBuilder.buildObject(AttributeValue.DEFAULT_ELEMENT_NAME, XSString.TYPE_NAME);
    stringValue.setValue(value);
    result.getAttributeValues().add(stringValue);
    return result;
  }

  private static Conditions buildConditions(DateTime currentTime, int expireInMinutes) {
    Conditions conditions = create(Conditions.class, Conditions.DEFAULT_ELEMENT_NAME);
    conditions.setNotBefore(currentTime.minusMinutes(10));
    conditions.setNotOnOrAfter(currentTime.plusMinutes(expireInMinutes));
    return conditions;
  }

  private static String getSAMLAssertionString(Assertion assertion) {
    AssertionMarshaller marshaller = new AssertionMarshaller();
    Element element = null;
    try {
      element = marshaller.marshall(assertion);
    } catch (MarshallingException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    String unencodedSAMLAssertion = XMLHelper.nodeToString(element);

    Base64 base64 = new Base64();
    try {
      return base64.encodeToString(unencodedSAMLAssertion.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * Signs a SAML assertion using the provided private key.
   *
   * @param assertion   The unsigned SAML assertion to be signed.
   * @param privateKey  The private key used for signing the assertion.
   * @return The signed SAML assertion.
   * @throws Exception If an error occurs during the signing process.
   *                   - If the SAML assertion is already signed.
   *                   - If an invalid X.509 private key is provided.
   *                   - If there is a failure in signing the SAML2 assertion.
   */
  private static Assertion sign(Assertion assertion, PrivateKey privateKey) {
    BasicX509Credential credential = new BasicX509Credential();
    credential.setPrivateKey(privateKey);

    if (assertion.getSignature() != null) {
      throw new RuntimeException("SAML assertion is already signed");
    }

    if (privateKey == null) {
      throw new RuntimeException("Invalid X.509 private key");
    }

    try {
      Signature signature = (Signature) Configuration.getBuilderFactory()
        .getBuilder(Signature.DEFAULT_ELEMENT_NAME).buildObject(Signature.DEFAULT_ELEMENT_NAME);
      signature.setSigningCredential(credential);
      SecurityConfiguration secConfig = Configuration.getGlobalSecurityConfiguration();
      String keyInfoGeneratorProfile = null; // "XMLSignature";
      SecurityHelper.prepareSignatureParams(signature, credential, secConfig, keyInfoGeneratorProfile);

      // Support sha256 signing algorithm for external oauth saml assertion
      signature.setSignatureAlgorithm(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256);

      assertion.setSignature(signature);
      Configuration.getMarshallerFactory().getMarshaller(assertion).marshall(assertion);
      Signer.signObject(signature);
    } catch (MarshallingException | SecurityException | SignatureException e) {
      throw new RuntimeException("Failure in signing the SAML2 assertion", e);
    }
    return assertion;
  }

  private static PrivateKey generatePrivateKey(String privateKeyString) {
    try {
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");

      // Decode the base64-encoded private key string
      String pk2 = new String(Base64.decodeBase64(privateKeyString), "UTF-8");

      // Extract the actual private key string if it is in a format like "privateKey###additionalInfo"
      String[] strs = pk2.split("###");
      if (strs.length == 2) {
        privateKeyString = strs[0];
      }

      // Generate the private key from the decoded key string
      PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Base64.decodeBase64(privateKeyString));
      return keyFactory.generatePrivate(privateKeySpec);
    } catch (NoSuchAlgorithmException | UnsupportedEncodingException | InvalidKeySpecException e) {
      // Throw a runtime exception if an error occurs during the private key generation process
      throw new RuntimeException("Error generating private key", e);
    }
  }

  public String getAssertionToken() {

    /**
     * Below code is to produce signed assertion via code directly using provided
     * input
     */
    String tokenUrl = config.getTokenURL(), clientId = config.getClientId(),
      privateKey = config.getPrivateKey(), userId = config.getUserId();
    boolean useUserNameAsUserId = false;
    if (tokenUrl != null && clientId != null && privateKey != null && userId != null) {
      LOG.info("All properties are set, generating the SAML Assertion...");

      String signedSAMLAssertion = generateSignedSAMLAssertion(clientId, userId, tokenUrl, privateKey,
              config.getExpireInMinutes(), useUserNameAsUserId);
      LOG.info("Signed SAML Assertion is generated");
      return signedSAMLAssertion;
    }
    return null;
  }

  public String getAccessToken(String assertionToken) throws IOException {
    HttpClient client = HttpClientBuilder.create().build();

    // Build POST request
    HttpPost request = new HttpPost(URI.create(config.getTokenURL()));

    // Set headers
    request.setHeader("Authorization", "none");
    request.setHeader("Content-Type", "application/x-www-form-urlencoded");

    // Build request body
    StringBuilder body = new StringBuilder();
    body.append("client_id=").append(config.getClientId());
    body.append("&company_id=").append(config.getCompanyId());
    body.append("&grant_type=").append("urn:ietf:params:oauth:grant-type:saml2-bearer");
    body.append("&assertion=").append(assertionToken);

    // Set request entity
    request.setEntity(new StringEntity(body.toString()));

    // Execute request and get response
    HttpResponse response = client.execute(request);
    String accessToken = null;
    JsonObject jsonObject = null;

    // Read response body
    if (response.getEntity() != null) {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))) {
        jsonObject = gson.fromJson(reader, JsonObject.class);

        // Check if "access_token" is present in the JSON response
        if (jsonObject != null && jsonObject.has("access_token")) {
          accessToken = jsonObject.get("access_token").getAsString();
        }
      }
    }
    return accessToken;
  }
}
