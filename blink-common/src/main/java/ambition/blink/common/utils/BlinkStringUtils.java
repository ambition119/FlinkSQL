/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ambition.blink.common.utils;

import java.util.ArrayList;
import java.util.List;

public class BlinkStringUtils {

   public static String[] splitIgnoreQuotaBrackets(String str, String delimter){
      String splitPatternStr = delimter + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])(?=(?:[^\"]|\"[^\"]*\")*$)";
      return str.split(splitPatternStr);
   }

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

   public static List<String> splitSemiColon(String str) {
      boolean inQuotes = false;
      boolean escape = false;

      List<String> ret = new ArrayList<>();

      char quoteChar = '"';
      int beginIndex = 0;
      for (int index = 0; index < str.length(); index++) {
         char c = str.charAt(index);
         switch (c) {
            case ';':
               if (!inQuotes) {
                  ret.add(str.substring(beginIndex, index));
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

      if (beginIndex < str.length()) {
         ret.add(str.substring(beginIndex));
      }

      return ret;
   }

   public static String getReplaceString(String str) {
      str = str.replaceAll("--.*", "")
          .replaceAll("\r\n", " ")
          .replaceAll("\n", " ")
          .replace("\t", " ").trim();
      return str;
   }

   public static String getString(Object obj){
      if(obj == null){
         return null;
      }

      if(obj instanceof String){
         return (String) obj;
      }else {
         return obj.toString();
      }
   }

   public static boolean isChinese(String str) {
      if (str == null) return false;
      for (char c : str.toCharArray()) {
         if (isChinese(c)) return true;
      }
      return false;
   }

   private static boolean isChinese(char c) {
      Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
      if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS

          || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS

          || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A

          || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION

          || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION

          || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {

         return true;
      }
      return false;
   }
}
