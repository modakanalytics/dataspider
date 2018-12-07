/*
   Copyright 2018 modakanalytics.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.modak.dataspider.utils;

import java.security.PrivateKey;
import java.security.PublicKey;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class is used for generating RSA keys i.e., public and private keys and encrypting and decrypting the password
 */
public class RSAEncryptionUtils {

    private static final Logger logger = LogManager.getLogger(RSAEncryptionUtils.class.getSimpleName());

    /**
     * This method decrypts the password using privateKey
     * @return returns the decrypted string formatted password
     */
    public static String decryptPassword(String password,String filepath) throws Exception {
        return password;
    }

}
