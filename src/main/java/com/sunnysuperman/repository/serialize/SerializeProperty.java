package com.sunnysuperman.repository.serialize;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.sunnysuperman.commons.util.StringUtil;

@Target({ ElementType.FIELD })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface SerializeProperty {

    boolean insertable() default true;

    boolean updatable() default true;

    String column() default StringUtil.EMPTY;

    Relation relation() default Relation.NONE;

    String joinProperty() default StringUtil.EMPTY;

}
