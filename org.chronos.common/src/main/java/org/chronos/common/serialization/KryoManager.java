package org.chronos.common.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.ref.WeakReference;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

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
