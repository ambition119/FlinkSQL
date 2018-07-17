package ambition.util;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @Author: wpl
 */
public class StringUtil {
    private static final Pattern NO_VERSION_PATTERN = Pattern.compile("([a-zA-Z]+).*");

    /**
     * Split the specified string delimiter --- ignored quotes delimiter
     * @param str
     * @param delimiter
     * @return
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter){
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder();
        for (char c : str.toCharArray()) {
            if(c == delimiter){
                if (inQuotes) {
                    b.append(c);
                } else if(inSingleQuotes){
                    b.append(c);
                }else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            }else if(c == '\"'){
                inQuotes = !inQuotes;
                b.append(c);
            }else if(c == '\''){
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            }else{
                b.append(c);
            }
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    /***
     * Split the specified string delimiter --- ignored in brackets and quotation marks delimiter
     * @param str
     * @param delimter
     * @return
     */
    public static String[] splitIgnoreQuotaBrackets(String str, String delimter){
        String splitPatternStr = delimter + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])(?=(?:[^\"]|\"[^\"]*\")*$)";
        return str.split(splitPatternStr);
    }

    public static String replaceIgnoreQuota(String str, String oriStr, String replaceStr){
        String splitPatternStr = oriStr + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)(?=(?:[^']*'[^']*')*[^']*$)";
        return str.replaceAll(splitPatternStr, replaceStr);
    }




    /**
     * add specify params to dbUrl
     * @param dbUrl
     * @param addParams
     * @param isForce true:replace exists param
     * @return
     */
    public static String addJdbcParam(String dbUrl, Map<String, String> addParams, boolean isForce){

        if(Strings.isNullOrEmpty(dbUrl)){
            throw new RuntimeException("dburl can't be empty string, please check it.");
        }

        if(addParams == null || addParams.size() == 0){
            return dbUrl;
        }

        String[] splits = dbUrl.split("\\?");
        String preStr = splits[0];
        Map<String, String> params = Maps.newHashMap();
        if(splits.length > 1){
            String existsParamStr = splits[1];
            String[] existsParams = existsParamStr.split("&");
            for(String oneParam : existsParams){
                String[] kv = oneParam.split("=");
                if(kv.length != 2){
                    throw new RuntimeException("illegal dbUrl:" + dbUrl);
                }

                params.put(kv[0], kv[1]);
            }
        }

        for(Map.Entry<String, String> addParam : addParams.entrySet()){
            if(!isForce && params.containsKey(addParam.getKey())){
                continue;
            }

            params.put(addParam.getKey(), addParam.getValue());
        }

        //rebuild dbURL
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for(Map.Entry<String, String> param : params.entrySet()){
            if(!isFirst){
                sb.append("&");
            }

            sb.append(param.getKey()).append("=").append(param.getValue());
            isFirst = false;
        }
        return preStr + "?" + sb.toString();
    }
}
