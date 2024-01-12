package com.sunnysuperman.repository.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.sunnysuperman.commons.util.StringUtil;

@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {

	String name();

	String comment() default StringUtil.EMPTY;

	boolean mapCamelToUnderscore() default true;
}
