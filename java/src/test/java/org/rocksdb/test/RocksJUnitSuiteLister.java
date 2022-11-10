package org.rocksdb.test;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.jar.*;

import org.junit.Test;

public class RocksJUnitSuiteLister {
	private static class RecursiveFilenameIterator implements Iterator<String> {
		private List<RecursiveFilenameIterator> innerIterators;
		private int prefixLength;
		private File root;
		private boolean alreadyUsed = false;
		private int index = 0;

		public RecursiveFilenameIterator(File root) {
			this(root, root.getAbsolutePath().length() + 1);
		}

		private RecursiveFilenameIterator(File root, int prefixLength) {
			this.root = root;
			this.prefixLength = prefixLength;
			if (!isRootFile()) {
				innerIterators = getInnerIterators(root);
			}
		}

		private boolean isRootFile() {
			return this.root.isFile();
		}

		private List<RecursiveFilenameIterator> getInnerIterators(File root) {
			List<RecursiveFilenameIterator> iterators = new ArrayList<RecursiveFilenameIterator>();
			for (File each : root.listFiles()) {
				iterators.add(new RecursiveFilenameIterator(each, prefixLength));
			}
			return iterators;
		}

		public boolean hasNext() {
			if (isRootFile()) {
				return !alreadyUsed;
			}
			if (index >= innerIterators.size()) {
				return false;
			}
			if (currentIterator().hasNext()) {
				return true;
			}
			index++;
			return hasNext();
		}

		private RecursiveFilenameIterator currentIterator() {
			return innerIterators.get(index);
		}

		public String next() {
			if (isRootFile()) {
				if (alreadyUsed) {
					throw new NoSuchElementException();
				}
				alreadyUsed = true;
				return root.getAbsolutePath().substring(prefixLength);
			}
			if (hasNext()) {
				return currentIterator().next();
			}
			throw new NoSuchElementException();
		}
	}

	private static class JarFilenameIterator implements Iterator<String> {
		private Enumeration<JarEntry> entries;
		private JarEntry next;

		public JarFilenameIterator(File jarFile) throws IOException {
			JarFile jar = new JarFile(jarFile);
			entries = jar.entries();
			retrieveNextElement();
		}

		private void retrieveNextElement() {
			next = null;
			while (entries.hasMoreElements()) {
				next = entries.nextElement();
				if (!next.isDirectory()) {
					break;
				}
			}
		}

		public boolean hasNext() {
			return next != null;
		}

		public String next() {
			if (next == null) {
				throw new NoSuchElementException();
			}
			String value = next.getName();
			retrieveNextElement();
			return value;
		}
	}

	private static final int CLASS_SUFFIX_LENGTH = ".class".length();

	public static void main(final String[] args) {
		List<String> classes = new ArrayList<String>();
		for (String root : getRoots()) {
			Iterator<String> iter = getIterator(new File(root));
			while (iter.hasNext()) {
				String entry = iter.next();
				if (!entry.endsWith(".class")) {
					continue;
				}

				if (entry.contains("$")) {
					continue;
				}

				String className = classNameFromFileName(entry);
				try {
					Class<?> klass = Class.forName(className, false, RocksJUnitSuiteLister.class.getClassLoader());
					if (klass == null || klass.isLocalClass() || klass.isAnonymousClass()) {
						continue;
					}
					if (isTestClass(klass)) {
						classes.add(klass.getName());
					}
				} catch (ClassNotFoundException cnfe) {
					// ignore not instantiable classes
				} catch (NoClassDefFoundError ncdfe) {
					// ignore not instantiable classes
				} catch (ExceptionInInitializerError ciie) {
					// ignore not instantiable classes
				} catch (UnsatisfiedLinkError ule) {
					// ignore not instantiable classes
				}
			}
		}

		Collections.sort(classes);

		if (args.length == 0 || args[0].equals("-list")) {
			for (String klass : classes) {
				System.out.println(klass);
			}
		} else if (args[0].equals("-runAll")) {
			String[] arrClasses = new String[classes.size()];
			RocksJunitRunner.main(classes.toArray(arrClasses));
		}
	}

	private static String classNameFromFileName(String entry) {
		entry = entry.substring(0, entry.length() - CLASS_SUFFIX_LENGTH);
		entry = entry.replace(File.separator, ".");
		if (!File.separator.equals("/")) {
			entry = entry.replace('/', '.');
		}
		if (entry.startsWith(".")) {
			return entry.substring(1);
		}
		return entry;
	}

	private static Iterator<String> getIterator(File classRoot) {
		Iterator<String> relativeFilenames = Collections.emptyIterator();
		if (classRoot.getName().toLowerCase().endsWith(".jar")) {
			try {
				relativeFilenames = new JarFilenameIterator(classRoot);
			} catch (IOException e) {
				// Don't iterate unavailable ja files
				e.printStackTrace();
			}
		} else if (classRoot.isDirectory()) {
			relativeFilenames = new RecursiveFilenameIterator(classRoot);
		}
		return relativeFilenames;
	}

	private static Iterable<String> getRoots() {
		final String separator = System.getProperty("path.separator");
		final String classPath = System.getProperty("java.class.path");
		return Arrays.asList(classPath.split(separator));
	}

	private static boolean isTestClass(Class<?> klass) {
		if ((klass.getModifiers() & Modifier.ABSTRACT) != 0) {
			return false;
		}
		try {
			for (Method method : klass.getMethods()) {
				if (method.getAnnotation(Test.class) != null) {
					return true;
				}
			}
		} catch (NoClassDefFoundError ignore) {
		}
		return false;
	}
}