import org.apache.hadoop.hive.ql.exec.UDF;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;


/**
 * Describe: class description
 * Author:   Angelo.Liu
 * Date:     17/2/8.
 */
public class LaiUserDecryption extends UDF {

    public String evaluate(String str) {
        try {
            System.out.println(decryption(parseHexStr2Byte(str)));
            System.out.println(new String(decryption(parseHexStr2Byte(str)), "UTF-8"));
            return new String(decryption(parseHexStr2Byte(str)), "UTF-8");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            return str;
        }
    }

    public static byte[] decryption(byte[] data) throws Exception {
        return cipherIV(Cipher.DECRYPT_MODE, data);
    }

    private static byte[] cipherIV(int mode, byte[] data) throws Exception {
//        //定义加密因子
        byte[] KEY = "qJRC2yf6p6cuz8Am".getBytes();
        byte[] IV = "Ta46BAY4GgpgP9tE".getBytes();
//        byte[] KEY = "856402f9148f066c".getBytes();
//        byte[] IV = "0bae3f6060e20359".getBytes();
        SecretKeySpec sekey = new SecretKeySpec(KEY, "AES");
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(mode, sekey, new IvParameterSpec(IV));
        byte[] decrypted = cipher.doFinal(data);
        try {
            System.out.println("=========" + new String(data, "UTF-8") + "*********:" + new String(decrypted, "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return decrypted;
    }

    /**
     * 将16进制转换为二进制
     *
     * @param hexStr
     * @return
     */
    public static byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1)
            return null;

        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
            result[i] = (byte) (high * 16 + low);
        }

        return result;
    }


    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        //GetBusinessWeekFirst st=new GetBusinessWeekFirst();
        //String s=st.evaluate("aaa");
        //System.out.println(s);
        String str1 = "ce65472b1bd6729621829786e8b5fed2";
        String str2 = "F1EDF46B71BD07D47E0808436DACD243";
        LaiUserDecryption ud = new LaiUserDecryption();
        //QuUserDecryption ud = new QuUserDecryption();
        System.out.println(ud.evaluate(str1));
        //CheckIsRegisterUser cir=new CheckIsRegisterUser();
        //int res=cir.evaluate(ud.evaluate(str1));
        //System.out.println(res);


    }

}
