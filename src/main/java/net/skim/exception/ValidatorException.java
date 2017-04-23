package net.skim.exception;

/**
 * Created by sunghokim on 4/22/2017.
 */
public class ValidatorException extends Exception {
    public ValidatorException() {
        super();
    }

    public ValidatorException(String message) {
        super(message);
    }

    public ValidatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidatorException(Throwable cause) {
        super(cause);
    }
}
