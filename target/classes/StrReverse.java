/**
 * @program: BigData
 * @description: 字符串反转
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/3
 **/
public class StrReverse {
    public static String getNewStr(String str){
        StringBuffer sb = new StringBuffer(str);
        String newStr = sb.reverse().toString();
        return newStr;
    }
    public static void main(String[] args) {
        System.out.println(getNewStr("thjymhr"));
    }

}
