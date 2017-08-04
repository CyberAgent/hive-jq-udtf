package jp.co.cyberagent.hive.udtf.jsonquery.internal;

public class Pair<T, U> {
	public final T _1;
	public final U _2;

	public Pair(final T _1, final U _2) {
		this._1 = _1;
		this._2 = _2;
	}

	public static <T, U> Pair<T, U> of(final T _1, final U _2) {
		return new Pair<>(_1, _2);
	}

	@Override
	public String toString() {
		return "(" + _1 + ", " + _2 + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_1 == null) ? 0 : _1.hashCode());
		result = prime * result + ((_2 == null) ? 0 : _2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("rawtypes")
		final Pair other = (Pair) obj;
		if (_1 == null) {
			if (other._1 != null)
				return false;
		} else if (!_1.equals(other._1))
			return false;
		if (_2 == null) {
			if (other._2 != null)
				return false;
		} else if (!_2.equals(other._2))
			return false;
		return true;
	}
}