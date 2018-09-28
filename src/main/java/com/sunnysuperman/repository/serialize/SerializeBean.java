package com.sunnysuperman.repository.serialize;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface SerializeBean {

    String value();

    boolean camel2underline() default true;

}
