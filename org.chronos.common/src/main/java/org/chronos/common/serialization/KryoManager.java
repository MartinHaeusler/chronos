package org.chronos.common.serialization;

import static com.google.common.base.Preconditions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.List;

import org.chronos.common.exceptions.ChronosIOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;

public class KryoManager {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	private static final int KRYO_REUSE_WRITTEN_BYTES_THRESHOLD_BYTES = 1024 * 1024 * 2; // 2MB
	private static final int KRYO_REUSE_USAGE_COUNT_THRESHOLD = 2000;

	// =====================================================================================================================
	// STATIC FIELDS
	// =====================================================================================================================

	private static final ThreadLocal<KryoWrapper> THREAD_LOCAL_KRYO = new ThreadLocal<KryoWrapper>();

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public static void destroyKryo() {
		THREAD_LOCAL_KRYO.remove();
	}

	public static byte[] serialize(final Object object) {
		return getKryo().serialize(object);
	}

	public static <T> T deserialize(final byte[] serialForm) {
		return getKryo().deserialize(serialForm);
	}

	public static <T> T deepCopy(final T element) {
		return getKryo().deepCopy(element);
	}

	public static void serializeObjectsToFile(final File file, final Object... objects) {
		checkNotNull(objects, "Precondition violation - argument 'objects' must not be NULL!");
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
		checkArgument(file.isFile(),
				"Precondition violation - argument 'file' must refer to a file (not a directory)!");
		checkArgument(file.canWrite(), "Precondition violation - argument 'file' must be writable!");
		try {
			getKryo().serializeToFile(file, objects);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to serialize object to file!", e);
		}
	}

	public static <T> T deserializeObjectFromFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
		checkArgument(file.isFile(),
				"Precondition violation - argument 'file' must refer to a file (not a directory)!");
		checkArgument(file.canRead(), "Precondition violation - argument 'file' must be readable!");
		try {
			return getKryo().deserializeObjectFromFile(file);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to deserialize object from file!", e);
		}
	}

	public static List<Object> deserializeObjectsFromFile(final File file) {
		checkArgument(file.exists(), "Precondition violation - argument 'file' must refer to an existing file!");
		checkArgument(file.isFile(),
				"Precondition violation - argument 'file' must refer to a file (not a directory)!");
		checkArgument(file.canRead(), "Precondition violation - argument 'file' must be readable!");
		try {
			return getKryo().deserializeObjectsFromFile(file);
		} catch (IOException e) {
			throw new ChronosIOException("Failed to deserialize object(s) from file!", e);
		}
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private static KryoWrapper getKryo() {
		KryoWrapper kryoWrapper = THREAD_LOCAL_KRYO.get();
		if (kryoWrapper == null) {
			return produceKryoWrapper();
		}
		return kryoWrapper;
	}

	private static KryoWrapper produceKryoWrapper() {
		KryoWrapper wrapper = new KryoWrapper();
		THREAD_LOCAL_KRYO.set(wrapper);
		return wrapper;
	}

	// =====================================================================================================================
	// INNER CLASSES
	// =====================================================================================================================

	private static class KryoWrapper {

		private WeakReference<Kryo> kryoReference = null;
		private int usageCount = 0;
		private long serializedBytes = 0;

		// =================================================================================================================
		// PUBLIC API
		// =================================================================================================================

		public byte[] serialize(final Object object) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Output output = new Output(baos);
			this.getKryo().writeClassAndObject(output, object);
			output.flush();
			byte[] result = baos.toByteArray();
			output.close();
			this.serializedBytes += result.length;
			this.usageCount++;
			this.destroyKryoIfNecessary();
			return result;
		}

		public void serializeToFile(final File file, final Object... objects) throws IOException {
			try (Output out = new Output(new FileOutputStream(file))) {
				for (Object object : objects) {
					this.getKryo().writeClassAndObject(out, object);
				}
				out.flush();
				this.serializedBytes += file.length();
				this.usageCount++;
				this.destroyKryoIfNecessary();
			}
		}

		@SuppressWarnings("unchecked")
		public <T> T deserialize(final byte[] serialForm) {
			ByteArrayInputStream bais = new ByteArrayInputStream(serialForm);
			Input input = new Input(bais);
			Object object = this.getKryo().readClassAndObject(input);
			input.close();
			this.usageCount++;
			this.destroyKryoIfNecessary();
			return (T) object;
		}

		@SuppressWarnings("unchecked")
		public <T> T deserializeObjectFromFile(final File file) throws IOException {
			try (Input input = new Input(new FileInputStream(file))) {
				Object object = this.getKryo().readClassAndObject(input);
				this.usageCount++;
				this.destroyKryoIfNecessary();
				return (T) object;
			}
		}

		public List<Object> deserializeObjectsFromFile(final File file) throws IOException {
			try (Input input = new Input(new FileInputStream(file))) {
				List<Object> resultList = Lists.newArrayList();
				Kryo kryo = this.getKryo();
				while (input.canReadInt()) {
					Object element = kryo.readClassAndObject(input);
					resultList.add(element);
				}
				this.usageCount++;
				this.destroyKryoIfNecessary();
				return resultList;
			}
		}

		public <T> T deepCopy(final T element) {
			Kryo kryo = this.getKryo();
			T copy = kryo.copy(element);
			// we have no idea how big the copied object is;
			// destroy our kryo instance as a precautionary measure
			this.destroyKryo();
			return copy;
		}

		// =================================================================================================================
		// INTERNAL API
		// =================================================================================================================

		private Kryo produceKryo() {
			Kryo kryo = new Kryo();
			this.kryoReference = new WeakReference<Kryo>(kryo);
			return kryo;
		}

		private Kryo getKryo() {
			if (this.kryoReference == null) {
				return this.produceKryo();
			}
			Kryo kryo = this.kryoReference.get();
			if (kryo == null) {
				return this.produceKryo();
			}
			return kryo;
		}

		private void destroyKryo() {
			this.kryoReference = null;
			this.usageCount = 0;
			this.serializedBytes = 0;
		}

		private void destroyKryoIfNecessary() {
			if (this.serializedBytes >= KRYO_REUSE_WRITTEN_BYTES_THRESHOLD_BYTES) {
				this.destroyKryo();
			}
			if (this.usageCount >= KRYO_REUSE_USAGE_COUNT_THRESHOLD) {
				this.destroyKryo();
			}
		}
	}

}
