package de.ingef.eva.utility;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateFormatValidator {

    private final SimpleDateFormat formatter;

    public DateFormatValidator(String pattern) {
        formatter = new SimpleDateFormat(pattern);
    }

    public boolean isValid(String date) {
        boolean isValid = true;
        try {
            formatter.parse(date);
        } catch (ParseException e) {
            isValid = false;
        }
        return isValid;
    }
}
