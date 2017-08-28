package org.chronos.chronodb.internal.util;

public class Quadruple<A, B, C, D> {

	public static <A, B, C, D> Quadruple<A, B, C, D> of(final A a, final B b, final C c, final D d) {
		return new Quadruple<>(a, b, c, d);
	}

	private final A a;
	private final B b;
	private final C c;
	private final D d;

	public Quadruple(final A a, final B b, final C c, final D d) {
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
	}

	public A getA() {
		return this.a;
	}

	public B getB() {
		return this.b;
	}

	public C getC() {
		return this.c;
	}

	public D getD() {
		return this.d;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.a == null ? 0 : this.a.hashCode());
		result = prime * result + (this.b == null ? 0 : this.b.hashCode());
		result = prime * result + (this.c == null ? 0 : this.c.hashCode());
		result = prime * result + (this.d == null ? 0 : this.d.hashCode());
		return result;
	}

	@Override
	@SuppressWarnings("rawtypes")
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		Quadruple other = (Quadruple) obj;
		if (this.a == null) {
			if (other.a != null) {
				return false;
			}
		} else if (!this.a.equals(other.a)) {
			return false;
		}
		if (this.b == null) {
			if (other.b != null) {
				return false;
			}
		} else if (!this.b.equals(other.b)) {
			return false;
		}
		if (this.c == null) {
			if (other.c != null) {
				return false;
			}
		} else if (!this.c.equals(other.c)) {
			return false;
		}
		if (this.d == null) {
			if (other.d != null) {
				return false;
			}
		} else if (!this.d.equals(other.d)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "Quadruple [a=" + this.a + ", b=" + this.b + ", c=" + this.c + ", d=" + this.d + "]";
	}

}
