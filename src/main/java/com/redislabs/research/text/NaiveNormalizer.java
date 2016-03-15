package com.redislabs.research.text;
import java.text.Normalizer;
import java.util.regex.Pattern;

import javafx.util.StringConverter;

/**
 * Created by dvirsky on 08/02/16.
 */
public class NaiveNormalizer implements TextNormalizer {
    private static Pattern nopunct = Pattern.compile("\\{Punct}");
    private static Pattern dedupSpaces = Pattern.compile("\\p{Space}+");

    @Override
    public String normalize(String s) {


        s = Normalizer.normalize(s.toLowerCase(), Normalizer.Form.NFD);
        return dedupSpaces.matcher(
                nopunct.matcher(s)
                .replaceAll(" ")
            ).replaceAll(" ").trim();

    }
}
