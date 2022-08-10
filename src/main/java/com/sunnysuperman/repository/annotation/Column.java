package com.sunnysuperman.repository.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.sunnysuperman.commons.util.StringUtil;

@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {

	String name() default StringUtil.EMPTY;

	boolean insertable() default true;

	boolean updatable() default true;

	Class<?> converter() default void.class;

	boolean converterCache() default true;

}
