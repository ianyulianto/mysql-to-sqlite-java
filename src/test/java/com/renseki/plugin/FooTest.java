package com.renseki.plugin;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class FooTest {
    @Test
    public void test() throws Exception {

        final String regex = "ir_.*";

        final List<String> foo = Arrays.asList(
                "ir_model_data",
                "ir_model",
                "res_partner"
        );

        for ( String asd : foo ) {
            if ( asd.matches(regex) ) {
                System.out.println("Masuk: " + asd);
            }
            else {
                System.out.println("Gagal: " + asd);
            }
        }


    }
}
