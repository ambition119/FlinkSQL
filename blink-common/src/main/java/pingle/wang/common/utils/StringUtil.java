package pingle.wang.common.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wpl
 */
public class StringUtil {

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

   public static List<String> splitSemiColon(String sqlContext) {
      boolean inQuotes = false;
      boolean escape = false;

      List<String> ret = new ArrayList<>();

      char quoteChar = '"';
      int beginIndex = 0;
      for (int index = 0; index < sqlContext.length(); index++) {
         char c = sqlContext.charAt(index);
         switch (c) {
            case ';':
               if (!inQuotes) {
                  ret.add(sqlContext.substring(beginIndex, index));
                  beginIndex = index + 1;
               }
               break;
            case '"':
            case '\'':
               if (!escape) {
                  if (!inQuotes) {
                     quoteChar = c;
                     inQuotes = !inQuotes;
                  } else {
                     if (c == quoteChar) {
                        inQuotes = !inQuotes;
                     }
                  }
               }
               break;
            default:
               break;
         }

         if (escape) {
            escape = false;
         } else if (c == '\\') {
            escape = true;
         }
      }

      if (beginIndex < sqlContext.length()) {
         ret.add(sqlContext.substring(beginIndex));
      }

      return ret;
   }
}
