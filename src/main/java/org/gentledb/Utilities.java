/*
 * Copyright (C) 2011  Felix Rabe  (www.felixrabe.net)
 *
 * GentleDB is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * GentleDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with GentleDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gentledb;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Random;

import org.apache.commons.codec.binary.Hex;


public class Utilities {

    private Utilities() {
        // No instances please
    }

    public static class GentleDBException extends Exception {

        private static final long serialVersionUID = 1L;

        public GentleDBException(String message, Throwable cause) {
            super(message, cause);
        }

        public GentleDBException(String message) {
            super(message);
        }

        public GentleDBException(Throwable cause) {
            super(cause);
        }

    }

    public static class InvalidIdentifierException extends GentleDBException {

        private static final long serialVersionUID = 1L;

        public InvalidIdentifierException(String message, Throwable cause) {
            super(message, cause);
        }

        public InvalidIdentifierException(String message) {
            super(message);
        }

        public InvalidIdentifierException(Throwable cause) {
            super(cause);
        }

    }

    private static final int IDENTIFIER_LENGTH = 256 / 4;
    private static final String IDENTIFIER_DIGITS = "0123456789abcdef";

    private static Random rng = new SecureRandom();

    public static String random() {
        final byte[] randomBytes = new byte[32];
        rng.nextBytes(randomBytes);
        return hex(randomBytes);
    }
    
    public static MessageDigest sha256() throws GentleDBException {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new GentleDBException(e);
		}
    }
    
    public static String hex(byte[] byteArray) {
    	return new String(Hex.encodeHex(byteArray));
    }
    
    public static void mkdir700(File directory) throws GentleDBException {
		if (!directory.mkdir()) {
			throw new GentleDBException("Could not create directory '" + directory.getPath() + "'");
		}
		// chmod 0700
		directory.setReadable(false, false);
		directory.setReadable(true, true);
		directory.setWritable(false, false);
		directory.setWritable(true, true);
		directory.setExecutable(false, false);
		directory.setExecutable(true, true);
    }

    public static boolean isIdentifierValid(String identifier, boolean partial) {
    	if (identifier == null)
    		return false;
        if (identifier.length() != IDENTIFIER_LENGTH)
            return false;
        for (final char c : identifier.toCharArray()) {
            if (IDENTIFIER_DIGITS.indexOf(c) == -1)
                return false;
        }
        return true;
    }
    
    public static boolean isIdentifierValid(String identifier) {
    	return isIdentifierValid(identifier, false);
    }

    public static void validateIdentifier(String identifier, boolean partial) throws InvalidIdentifierException {
        if (!isIdentifierValid(identifier, partial)) {
            throw new InvalidIdentifierException(identifier);
        }
    }
    
    public static void validateIdentifier(String identifier) throws InvalidIdentifierException {
    	validateIdentifier(identifier, false);
    }

}
