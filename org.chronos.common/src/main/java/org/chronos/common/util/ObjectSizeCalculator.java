// =================================================================================================
// Copyright 2011 Twitter, Inc.
// -------------------------------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file, or at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================================================

package org.chronos.common.util;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;

/**
 * This class was originally contained in the Twitter Commons library. It has been copied and adapted.
 *
 * @author Martin Häusler -- Adaptions for Chronos
 * @author Attila Szegedi -- Initial contribution and API
 */
public class ObjectSizeCalculator {

	/**
	 * Describes constant memory overheads for various constructs in a JVM implementation.
	 */
	public interface MemoryLayoutSpecification {

		/**
		 * Returns the fixed overhead of an array of any type or length in this JVM.
		 *
		 * @return the fixed overhead of an array.
		 */
		int getArrayHeaderSize();

		/**
		 * Returns the fixed overhead of for any {@link Object} subclass in this JVM.
		 *
		 * @return the fixed overhead of any object.
		 */
		int getObjectHeaderSize();

		/**
		 * Returns the quantum field size for a field owned by an object in this JVM.
		 *
		 * @return the quantum field size for an object.
		 */
		int getObjectPadding();

		/**
		 * Returns the fixed size of an object reference in this JVM.
		 *
		 * @return the size of all object references.
		 */
		int getReferenceSize();

		/**
		 * Returns the quantum field size for a field owned by one of an object's ancestor superclasses in this JVM.
		 *
		 * @return the quantum field size for a superclass field.
		 */
		int getSuperclassFieldPadding();
	}

	private static class CurrentLayout {
		private static final MemoryLayoutSpecification SPEC = getEffectiveMemoryLayoutSpecification();
	}

	private static final MemoryLayoutSpecification MEMORY_LAYOUT_64BIT_UNCOMPRESSED = new MemoryLayoutSpecification() {
		@Override
		public int getArrayHeaderSize() {
			return 24;
		}

		@Override
		public int getObjectHeaderSize() {
			return 16;
		}

		@Override
		public int getObjectPadding() {
			return 8;
		}

		@Override
		public int getReferenceSize() {
			return 8;
		}

		@Override
		public int getSuperclassFieldPadding() {
			return 8;
		}
	};

	private static final MemoryLayoutSpecification MEMORY_LAYOUT_64BIT_COMPRESSED = new MemoryLayoutSpecification() {
		@Override
		public int getArrayHeaderSize() {
			return 16;
		}

		@Override
		public int getObjectHeaderSize() {
			return 12;
		}

		@Override
		public int getObjectPadding() {
			return 8;
		}

		@Override
		public int getReferenceSize() {
			return 4;
		}

		@Override
		public int getSuperclassFieldPadding() {
			return 4;
		}
	};

	private static final MemoryLayoutSpecification MEMORY_LAYOUT_32BIT = new MemoryLayoutSpecification() {
		@Override
		public int getArrayHeaderSize() {
			return 12;
		}

		@Override
		public int getObjectHeaderSize() {
			return 8;
		}

		@Override
		public int getObjectPadding() {
			return 8;
		}

		@Override
		public int getReferenceSize() {
			return 4;
		}

		@Override
		public int getSuperclassFieldPadding() {
			return 4;
		}
	};

	/**
	 * Given an object, returns the total allocated size, in bytes, of the object and all other objects reachable from
	 * it. Attempts to to detect the current JVM memory layout, but may fail with {@link UnsupportedOperationException};
	 *
	 * @param obj
	 *            the object; can be null. Passing in a {@link java.lang.Class} object doesn't do anything special, it
	 *            measures the size of all objects reachable through it (which will include its class loader, and by
	 *            extension, all other Class objects loaded by the same loader, and all the parent class loaders). It
	 *            doesn't provide the size of the static fields in the JVM class that the Class object represents.
	 * @return the total allocated size of the object and all other objects it retains.
	 * @throws UnsupportedOperationException
	 *             if the current vm memory layout cannot be detected.
	 */
	public static long getObjectSize(final Object obj) throws UnsupportedOperationException {
		return obj == null ? 0 : new ObjectSizeCalculator(CurrentLayout.SPEC).calculateObjectSize(obj);
	}

	// Fixed object header size for arrays.
	private final int arrayHeaderSize;
	// Fixed object header size for non-array objects.
	private final int objectHeaderSize;
	// Padding for the object size - if the object size is not an exact multiple
	// of this, it is padded to the next multiple.
	private final int objectPadding;
	// Size of reference (pointer) fields.
	private final int referenceSize;
	// Padding for the fields of superclass before fields of subclasses are
	// added.
	private final int superclassFieldPadding;

	private final LoadingCache<Class<?>, ClassSizeInfo> classSizeInfos = CacheBuilder.newBuilder()
			.build(new CacheLoader<Class<?>, ClassSizeInfo>() {
				@Override
				public ClassSizeInfo load(final Class<?> clazz) {
					return new ClassSizeInfo(clazz);
				}
			});

	private final Set<Object> alreadyVisited = Sets.newIdentityHashSet();
	private final Deque<Object> pending = new ArrayDeque<Object>(16 * 1024);
	private long size;

	/**
	 * Creates an object size calculator that can calculate object sizes for a given {@code memoryLayoutSpecification}.
	 *
	 * @param memoryLayoutSpecification
	 *            a description of the JVM memory layout.
	 */
	public ObjectSizeCalculator(final MemoryLayoutSpecification memoryLayoutSpecification) {
		Preconditions.checkNotNull(memoryLayoutSpecification);
		this.arrayHeaderSize = memoryLayoutSpecification.getArrayHeaderSize();
		this.objectHeaderSize = memoryLayoutSpecification.getObjectHeaderSize();
		this.objectPadding = memoryLayoutSpecification.getObjectPadding();
		this.referenceSize = memoryLayoutSpecification.getReferenceSize();
		this.superclassFieldPadding = memoryLayoutSpecification.getSuperclassFieldPadding();
	}

	/**
	 * Given an object, returns the total allocated size, in bytes, of the object and all other objects reachable from
	 * it.
	 *
	 * @param obj
	 *            the object; can be null. Passing in a {@link java.lang.Class} object doesn't do anything special, it
	 *            measures the size of all objects reachable through it (which will include its class loader, and by
	 *            extension, all other Class objects loaded by the same loader, and all the parent class loaders). It
	 *            doesn't provide the size of the static fields in the JVM class that the Class object represents.
	 * @return the total allocated size of the object and all other objects it retains.
	 */
	public synchronized long calculateObjectSize(Object obj) {
		// Breadth-first traversal instead of naive depth-first with recursive
		// implementation, so we don't blow the stack traversing long linked lists.
		try {
			for (;;) {
				this.visit(obj);
				if (this.pending.isEmpty()) {
					return this.size;
				}
				obj = this.pending.removeFirst();
			}
		} finally {
			this.alreadyVisited.clear();
			this.pending.clear();
			this.size = 0;
		}
	}

	private void visit(final Object obj) {
		if (this.alreadyVisited.contains(obj)) {
			return;
		}
		final Class<?> clazz = obj.getClass();
		if (clazz == ArrayElementsVisitor.class) {
			((ArrayElementsVisitor) obj).visit(this);
		} else {
			this.alreadyVisited.add(obj);
			if (clazz.isArray()) {
				this.visitArray(obj);
			} else {
				this.classSizeInfos.getUnchecked(clazz).visit(obj, this);
			}
		}
	}

	private void visitArray(final Object array) {
		final Class<?> componentType = array.getClass().getComponentType();
		final int length = Array.getLength(array);
		if (componentType.isPrimitive()) {
			this.increaseByArraySize(length, getPrimitiveFieldSize(componentType));
		} else {
			this.increaseByArraySize(length, this.referenceSize);
			// If we didn't use an ArrayElementsVisitor, we would be enqueueing every
			// element of the array here instead. For large arrays, it would
			// tremendously enlarge the queue. In essence, we're compressing it into
			// a small command object instead. This is different than immediately
			// visiting the elements, as their visiting is scheduled for the end of
			// the current queue.
			switch (length) {
			case 0: {
				break;
			}
			case 1: {
				this.enqueue(Array.get(array, 0));
				break;
			}
			default: {
				this.enqueue(new ArrayElementsVisitor((Object[]) array));
			}
			}
		}
	}

	private void increaseByArraySize(final int length, final long elementSize) {
		this.increaseSize(roundTo(this.arrayHeaderSize + length * elementSize, this.objectPadding));
	}

	private static class ArrayElementsVisitor {
		private final Object[] array;

		ArrayElementsVisitor(final Object[] array) {
			this.array = array;
		}

		public void visit(final ObjectSizeCalculator calc) {
			for (Object elem : this.array) {
				if (elem != null) {
					calc.visit(elem);
				}
			}
		}
	}

	void enqueue(final Object obj) {
		if (obj != null) {
			this.pending.addLast(obj);
		}
	}

	void increaseSize(final long objectSize) {
		this.size += objectSize;
	}

	@VisibleForTesting
	static long roundTo(final long x, final int multiple) {
		return (x + multiple - 1) / multiple * multiple;
	}

	private class ClassSizeInfo {
		// Padded fields + header size
		private final long objectSize;
		// Only the fields size - used to calculate the subclasses' memory
		// footprint.
		private final long fieldsSize;
		private final Field[] referenceFields;

		public ClassSizeInfo(final Class<?> clazz) {
			long fieldsSize = 0;
			final List<Field> referenceFields = new LinkedList<Field>();
			for (Field f : clazz.getDeclaredFields()) {
				if (Modifier.isStatic(f.getModifiers())) {
					continue;
				}
				final Class<?> type = f.getType();
				if (type.isPrimitive()) {
					fieldsSize += getPrimitiveFieldSize(type);
				} else {
					f.setAccessible(true);
					referenceFields.add(f);
					fieldsSize += ObjectSizeCalculator.this.referenceSize;
				}
			}
			final Class<?> superClass = clazz.getSuperclass();
			if (superClass != null) {
				final ClassSizeInfo superClassInfo = ObjectSizeCalculator.this.classSizeInfos.getUnchecked(superClass);
				fieldsSize += roundTo(superClassInfo.fieldsSize, ObjectSizeCalculator.this.superclassFieldPadding);
				referenceFields.addAll(Arrays.asList(superClassInfo.referenceFields));
			}
			this.fieldsSize = fieldsSize;
			this.objectSize = roundTo(ObjectSizeCalculator.this.objectHeaderSize + fieldsSize,
					ObjectSizeCalculator.this.objectPadding);
			this.referenceFields = referenceFields.toArray(new Field[referenceFields.size()]);
		}

		void visit(final Object obj, final ObjectSizeCalculator calc) {
			calc.increaseSize(this.objectSize);
			this.enqueueReferencedObjects(obj, calc);
		}

		public void enqueueReferencedObjects(final Object obj, final ObjectSizeCalculator calc) {
			for (Field f : this.referenceFields) {
				try {
					calc.enqueue(f.get(obj));
				} catch (IllegalAccessException e) {
					final AssertionError ae = new AssertionError("Unexpected denial of access to " + f);
					ae.initCause(e);
					throw ae;
				}
			}
		}
	}

	private static long getPrimitiveFieldSize(final Class<?> type) {
		if (type == boolean.class || type == byte.class) {
			return 1;
		}
		if (type == char.class || type == short.class) {
			return 2;
		}
		if (type == int.class || type == float.class) {
			return 4;
		}
		if (type == long.class || type == double.class) {
			return 8;
		}
		throw new AssertionError("Encountered unexpected primitive type " + type.getName());
	}

	@VisibleForTesting
	static MemoryLayoutSpecification getEffectiveMemoryLayoutSpecification() {
		final String vmName = System.getProperty("java.vm.name");
		if (vmName == null || !(vmName.startsWith("Java HotSpot(TM) ") || vmName.startsWith("OpenJDK")
				|| vmName.startsWith("TwitterJDK"))) {
			// we don't know this JVM, so we assume the worst case, which is 64 bit uncompressed
			return MEMORY_LAYOUT_64BIT_UNCOMPRESSED;
		}

		final String dataModel = System.getProperty("sun.arch.data.model");
		if ("32".equals(dataModel)) {
			// Running with 32-bit data model
			return MEMORY_LAYOUT_32BIT;
		} else if (!"64".equals(dataModel)) {
			// we interpret 32-Bit-Models as 64 bit here, it's less precise but okay for an estimate.
			return MEMORY_LAYOUT_64BIT_UNCOMPRESSED;
		}

		final String strVmVersion = System.getProperty("java.vm.version");
		final int vmVersion = Integer.parseInt(strVmVersion.substring(0, strVmVersion.indexOf('.')));
		if (vmVersion >= 17) {
			long maxMemory = 0;
			for (MemoryPoolMXBean mp : ManagementFactory.getMemoryPoolMXBeans()) {
				maxMemory += mp.getUsage().getMax();
			}
			if (maxMemory < 30L * 1024 * 1024 * 1024) {
				// HotSpot 17.0 and above use compressed OOPs below 30GB of RAM total
				// for all memory pools (yes, including code cache).
				return MEMORY_LAYOUT_64BIT_COMPRESSED;
			}
		}

		// In other cases, it's a 64-bit uncompressed OOPs object model
		return MEMORY_LAYOUT_64BIT_UNCOMPRESSED;
	}

}