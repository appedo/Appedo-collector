package com.appedo.webcollector.webserver.util;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.Key;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import com.appedo.manager.LogManager;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

/**
 * This class does the encryption-decryption operation required for the application.
 * 
 * @author Ramkumar
 *
 */
public class EncrpytManager {
	
	private static final String ALGO = "AES";
	private static final byte[] CRYPTION_KEY_VALUE = "1234567890123456".getBytes("UTF-8");
	
	/**
	 * Encrypt the input string with the global Cryption-Key.
	 * 
	 * @param Data
	 * @return
	 * @throws Exception
	 */
	public static String encrypt(String Data) throws Exception {
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.ENCRYPT_MODE, key);
		byte[] encVal = c.doFinal(Data.getBytes());
		String encryptedValue = new BASE64Encoder().encode(encVal);
		return encryptedValue;
	}
	
	/**
	 * Encrypt the input string with the global Cryption-Key. And Encode it to use it in URL.
	 * 
	 * @param Data
	 * @return
	 * @throws Exception
	 */
	public static String encrypt_URLEncode(String Data) throws Exception {
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.ENCRYPT_MODE, key);
		byte[] encVal = c.doFinal(Data.getBytes());
		String encryptedValue = new BASE64Encoder().encode(encVal);
		
		encryptedValue = URLEncoder.encode(encryptedValue, "UTF-8");

		// presence of %2F in URL, throws request back as 400-Bad Request.
		encryptedValue = encryptedValue.replaceAll("%2F", "APD2F");
		
		return encryptedValue;
	}
	
	/**
	 * Decode the encrypted input to normal String with the global Cryption-Key.
	 * 
	 * @param encryptedData
	 * @return
	 * @throws Exception
	 */
	public static String decrypt(String encryptedData) throws Exception {
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.DECRYPT_MODE, key);
		byte[] decordedValue = new BASE64Decoder().decodeBuffer(encryptedData);
		byte[] decValue = c.doFinal(decordedValue);
		String decryptedValue = new String(decValue);
		return decryptedValue;
	}
	
	/**
	 * Decode the URL encryption.
	 * And Decode the encrypted input to normal String with the global Cryption-Key.
	 * 
	 * @param encryptedData
	 * @return
	 * @throws Exception
	 */
	public static String decrypt_URLDecode(String encryptedData) throws Exception {
		// To avoid 400, %2F is replaced as APD2F
		encryptedData = encryptedData.replaceAll("PCA", "%");
		
		encryptedData = URLDecoder.decode(encryptedData, "UTF-8");
		
		Key key = generateKey();
		Cipher c = Cipher.getInstance(ALGO);
		c.init(Cipher.DECRYPT_MODE, key);
		byte[] decordedValue = new BASE64Decoder().decodeBuffer(encryptedData);
		byte[] decValue = c.doFinal(decordedValue);
		String decryptedValue = new String(decValue);
		return decryptedValue;
	}
	
	/**
	 * Generate a key with global Cryption-Key.
	 * 
	 * @return
	 * @throws Exception
	 */
	private static Key generateKey() throws Exception {
		Key key = new SecretKeySpec(CRYPTION_KEY_VALUE, ALGO);
		return key;
	}
	
	public static void main(String args[]) throws Exception {
		LogManager.infoLog(EncrpytManager.encrypt("5"));
		LogManager.infoLog(EncrpytManager.encrypt_URLEncode("5"));
		LogManager.infoLog(EncrpytManager.decrypt("61mO46RVNJxhHFfqrZrNqA=="));
	}
}

