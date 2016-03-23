package com.redislabs.research.text;
import java.text.Normalizer;
import javafx.util.StringConverter;

/**
 * Created by dvirsky on 08/02/16.
 */
public class NaiveNormalizer implements TextNormalizer {
    @Override
    public String normalize(String s) {

        return Normalizer.normalize(s.toLowerCase(), Normalizer.Form.NFD).
                replaceAll("\\p{Punct}", " ").
                replaceAll("\\p{Space}+", " ").
                replaceAll("\\p{M}", "").trim();
    }
}
