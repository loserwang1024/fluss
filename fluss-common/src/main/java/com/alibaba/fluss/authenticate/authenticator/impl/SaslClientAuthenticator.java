package com.alibaba.fluss.authenticate.authenticator.impl;

import com.alibaba.fluss.authenticate.authenticator.ClientAuthenticator;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.Map;

// todo: remove to fluss-auth-plugin later
public class SaslClientAuthenticator implements ClientAuthenticator {
    private final String mechanism;


   SaslClient saslClient;

    public SaslClientAuthenticator(String mechanism) {
        this.mechanism = mechanism;
    }

    @Override
    public byte[] authenticate(byte[] data) {
        return new byte[0];
    }


    private void init() throws SaslException {
        String[] mechs = {mechanism};
        // nodeConnectionId
//        if (mechanism.equals(SaslConfigs.GSSAPI_MECHANISM))
//            this.clientPrincipalName = firstPrincipal(subject);
//        else
//            this.clientPrincipalName = null;
        String clientPrincipalName = null;
        String servicePrincipal= null;
        String serverHost= null;
        Map<String, ?> saslConfigs= null;
        CallbackHandler authenticatecallbackHandler= null;
        saslClient = Sasl.createSaslClient(mechs, clientPrincipalName, servicePrincipal,serverHost, saslConfigs, authenticatecallbackHandler);
        if(saslClient == null){
            //  throw new SaslAuthenticationException("Failed to create SaslClient with mechanism " + mechanism);
        }

    }
}
