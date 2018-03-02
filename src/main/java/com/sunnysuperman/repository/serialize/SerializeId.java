package com.sunnysuperman.repository.serialize;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.FIELD })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface SerializeId {

    IdGenerator strategy() default IdGenerator.INCREMENT;

    Class<? extends Number> incrementClass() default Long.class;

}
