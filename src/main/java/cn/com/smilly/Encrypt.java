package cn.com.smilly;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.*;

@Slf4j
public class Encrypt {

    /*
    *
    * */
    private static String MODEL = "AES/ECB/PKCS5Padding";
    private static String useKey = Config.ENCRYPT_KEY;
    private final static Map<String, Set<String>> encryptMap = new HashMap();
    static {
        encryptMap.put("uc", new HashSet<>(Arrays.asList("mobile_phone","identity","id_card","bank_card","phone","company_phone","relative_mobile","second_relative_mobile","new_info","old_info","bank_card_code","bank_card_phone","bank_card_identity","id_card_no","user_phone","identity_code","userinfo_data","medium_id")));
        encryptMap.put("loan", new HashSet<>(Arrays.asList("phone","d_value","receive_account","repay_account","payer_mobile","request","data_json")));
        encryptMap.put("ccs", new HashSet<>(Arrays.asList("cert_no","mobile","bank_card_no","request_json")));
        encryptMap.put("sign", new HashSet<>(Arrays.asList("identity","mobile","contract_template_param")));
        encryptMap.put("wallet", new HashSet<>(Arrays.asList("bank_card_code","phone","cellphone","account_bank_card_code","credential_no","mobile_no","account_no","medium_id")));
        encryptMap.put("payment", new HashSet<>(Arrays.asList("cert_no","card_phone","card_no","medium_no","send_info")));
        encryptMap.put("sms", new HashSet<>(Arrays.asList("identity","mobile")));
        encryptMap.put("plutus", new HashSet<>(Arrays.asList("phone")));
        encryptMap.put("credit", new HashSet<>(Arrays.asList("mobile","pid","home_phone")));

    }

    //判断是否需要加密
    public static boolean needEncrypt(String database,String column){
        Set<String> set = encryptMap.get(database);
        if(set==null){
            return false;
        }
        return set.contains(column);
    }

    public static String encrypt(String content) {
        if(StringUtils.isEmpty(content)){
            return content;
        }
        String result = content;
        try {
            byte[] contentBytes = content.getBytes("UTF-8");
            SecretKeySpec skeySpec = new SecretKeySpec(useKey.getBytes("UTF-8"), "AES");
            Cipher cipher = Cipher.getInstance(MODEL);
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
            byte[] encryptResult = cipher.doFinal(contentBytes);
            result = org.apache.commons.codec.binary.Base64.encodeBase64String(encryptResult);
            //替换\r \n
            result = result.replace("\n", "").replace("\r", "");
        } catch (Exception ex) {
            log.error("进行自动加密时出错，加密内容为"+content+"，异常信息"+ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
        return result;
    }
    public static String decrypt(String content){
        if(StringUtils.isEmpty(content)){
            return content;
        }
        String result = content;
        byte[] contentBytes =null;
        try{
            if(content.length()%4==0){
                contentBytes = Base64.decodeBase64(content);
            }else{
                log.error("进行自动解密时出错，字符串{}不是base64编码过的字符串！",content);
                //throw new RuntimeException("字符串"+content+"不是base64编码过的字符串！");
            }
        } catch (Exception ex) {
            log.error("进行自动解密时出错，字符串"+content+"不是base64编码过的字符串，进行base64解码出错！出错信息："+ex.getMessage(),ex);
            //throw new RuntimeException(ex);
        }
        if(contentBytes!=null){
            try{
                SecretKeySpec skeySpec = new SecretKeySpec(useKey.getBytes("UTF-8"), "AES");
                Cipher cipher = Cipher.getInstance(MODEL);
                cipher.init(Cipher.DECRYPT_MODE, skeySpec);
                byte[] decryptResult = cipher.doFinal(contentBytes);
                if (decryptResult != null) {
                    result = new String(decryptResult, "UTF-8");
                }
            } catch (Exception ex) {
                log.error("进行自动解密时出错，加密内容为"+content+"，异常信息"+ex.getMessage());
                //throw new RuntimeException(ex);
                //cbbuXr3h9EWL0QOSDreFsw==
                //cbbuXr3h9EWL0QOSDreFsw==
            }
        }
        return result;
    }

}
