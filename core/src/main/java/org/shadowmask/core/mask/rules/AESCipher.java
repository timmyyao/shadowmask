package org.shadowmask.core.mask.rules;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;
import java.security.InvalidAlgorithmParameterException;
import java.security.spec.InvalidKeySpecException;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AESCipher extends CipherEngine {
    private static final int IV_LENGTH = 16;
    // private static SecureRandom rand = new SecureRandom();
    // private static byte[] seed = rand.generateSeed(IV_LENGTH);
    // private static KeyGenerator keygen_;
    // private static byte[] iv_ = new byte[IV_LENGTH];
    private SecretKeySpec skey_spec_;
    private static byte[] iv_ = "odjyvjdhgksefncx".getBytes();
    private static IvParameterSpec ivspec_ = new IvParameterSpec(iv_);

    public AESCipher() {
        this.method = "AES";
    }

    /*********
    static {
      try {
        keygen_ = KeyGenerator.getInstance(this.method);

        keygen_.init(128);
        skey_ = keygen_.generateKey();

        rand.setSeed(seed);
        rand.nextBytes(iv_);
        ivspec_ = new IvParameterSpec(iv_);
      } catch(NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
    }
    ***********/

    public String evaluate(String content, int mode, String key) {
      if (content == null) return null;
      if (key == null || key != null && (key.length() % 8) != 0) {
        throw new RuntimeException("Please input correct encrypting/decrypting key!\n" +
            "Key length is the times of 8!");
      }
      // validate key and geberate the secret key
      try {
        skey_spec_ = new SecretKeySpec(key.toString().getBytes("UTF-8"), this.method);
      }catch(UnsupportedEncodingException e) {
        e.printStackTrace();
      }

      String result = null;
      switch(mode) {
        case Cipher.ENCRYPT_MODE:
          result = encrypt(content.getBytes());
          System.out.println("Encrypted text is " + result.toString());
          break;
        case Cipher.DECRYPT_MODE:
          result = decrypt(Base64.getDecoder().decode(content.toString()));
          System.out.println("Decrypted text is " + result.toString());
          break;
        default:
          throw new RuntimeException("Unknown mode! [1: encryption, 2: decryption]");
      }
      return result;
    }


    protected String encrypt(byte[] content) {
      String result = null;
      try {
        Cipher aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        aesCipher.init(Cipher.ENCRYPT_MODE, skey_spec_, ivspec_);

        result = Base64.getEncoder().encodeToString(aesCipher.doFinal(content));
        return result;
      } catch(NoSuchAlgorithmException e) {
        e.printStackTrace();
        return null;
      } catch(InvalidKeyException e) {
        e.printStackTrace();
        return null;
      } catch(InvalidAlgorithmParameterException e) {
        e.printStackTrace();
        return null;
      } catch(NoSuchPaddingException e) {
        e.printStackTrace();
        return null;
      } catch(BadPaddingException e) {
        e.printStackTrace();
        return null;
      } catch(IllegalBlockSizeException e) {
        e.printStackTrace();
        return null;
      }
    }


    protected String decrypt(byte[] content) {
      try {
        Cipher aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        aesCipher.init(Cipher.DECRYPT_MODE, skey_spec_, ivspec_);

        return new String(aesCipher.doFinal(content));
      } catch(NoSuchAlgorithmException e) {
        e.printStackTrace();
        return null;
      } catch(InvalidKeyException e) {
        e.printStackTrace();
        return null;
      } catch(InvalidAlgorithmParameterException e) {
        e.printStackTrace();
        return null;
      } catch(NoSuchPaddingException e) {
        e.printStackTrace();
        return null;
      } catch(BadPaddingException e) {
        e.printStackTrace();
        return null;
      } catch(IllegalBlockSizeException e) {
        e.printStackTrace();
        return null;
      }
    }

}
