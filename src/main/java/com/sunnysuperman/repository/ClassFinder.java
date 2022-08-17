package com.sunnysuperman.repository;

import java.io.File;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ClassFinder {

	public static interface ClassFilter {

		boolean filter(Class<?> clazz);

	}

	public static Set<Class<?>> find(String packageName, ClassFilter filter) throws Exception {
		Set<Class<?>> classes = new HashSet<>();
		String packagePath = packageNameToPath(packageName);
		Enumeration<URL> dirs = Thread.currentThread().getContextClassLoader().getResources(packagePath);
		while (dirs.hasMoreElements()) {
			URL url = dirs.nextElement();
			String protocol = url.getProtocol();
			if ("file".equals(protocol)) {
				File dir = new File(URLDecoder.decode(url.getFile(), "UTF-8"));
				findClassInDirectory(classes, filter, dir, packageName);
			} else if ("jar".equals(protocol)) {
				JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
				findClassInJarFile(classes, filter, jar, packageName);
			} else {
				throw new RuntimeException("Bad url: " + url);
			}
		}
		return classes;
	}

	private static String packageNameToPath(String packageName) {
		if (packageName == null || packageName.isEmpty()) {
			return "";
		}
		return packageName.replaceAll("\\.", "/");
	}

	private static String makePackageOrClassName(String packageName, String name) {
		if (packageName == null || packageName.isEmpty()) {
			return name;
		}
		return packageName + "." + name;
	}

	private static void findClassInDirectory(Set<Class<?>> classes, ClassFilter filter, File dir, String packageName)
			throws Exception {
		File[] files = dir.listFiles(file -> file.isDirectory() || file.getName().endsWith(".class"));
		for (File file : files) {
			String fileName = file.getName();
			if (file.isDirectory()) {
				String subPackageName = makePackageOrClassName(packageName, file.getName());
				findClassInDirectory(classes, filter, file, subPackageName);
			} else {
				String classsName = makePackageOrClassName(packageName,
						fileName.substring(0, fileName.lastIndexOf(".")));
				addClass(classes, filter, classsName);
			}
		}
	}

	private static void findClassInJarFile(Set<Class<?>> classes, ClassFilter filter, JarFile jar, String packageName) {
		Enumeration<JarEntry> entries = jar.entries();
		// com.sunnysuperman -> com/sunnysuperman/
		String packagePrefix = packageNameToPath(packageName);
		if (!packagePrefix.endsWith("/")) {
			packagePrefix = packagePrefix + "/";
		}
		while (entries.hasMoreElements()) {
			JarEntry entry = entries.nextElement();
			if (entry.isDirectory()) {
				continue;
			}
			String entryName = entry.getName();
			// com/sunnysuperman/Z.class
			if (!entryName.startsWith(packagePrefix)) {
				continue;
			}
			// com/sunnysuperman/Z.class -> com.sunnysuperman.Z
			String className = entryName.substring(0, entryName.lastIndexOf('.')).replaceAll("/", "\\.");
			addClass(classes, filter, className);
		}
	}

	private static void addClass(Set<Class<?>> classes, ClassFilter filter, String classsName) {
		Class<?> clazz;
		try {
			clazz = Thread.currentThread().getContextClassLoader().loadClass(classsName);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to load class " + classsName, e);
		}
		if (filter.filter(clazz)) {
			classes.add(clazz);
		}
	}

}
