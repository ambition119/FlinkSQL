package pingle.wang.common.api;

/**
 * @Author: wpl
 */
public class Result<T> {
   private String message;
   private String code;
   private T result;

   public Result() {
   }

   public Result(String message, String code, T result) {
      this.message = message;
      this.code = code;
      this.result = result;
   }

   public T getResult() {
      return result;
   }

   public void setResult(T result) {
      this.result = result;
   }

   public String getMessage() {
      return message;
   }

   public void setMessage(String message) {
      this.message = message;
   }

   public String getCode() {
      return code;
   }

   public void setCode(String code) {
      this.code = code;
   }
}
