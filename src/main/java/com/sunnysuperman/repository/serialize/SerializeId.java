package com.sunnysuperman.repository.serialize;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.FIELD })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface SerializeId {

    IdGenerator generator();

    Class<? extends Number> type() default Long.class;

}
