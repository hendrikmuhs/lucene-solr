package org.apache.lucene.util.keyvi;

public interface MinimizationHashEntry {

	public interface Key {

		void set(int offset, int hashcode, int numberOutgoingStatesAndCookie);

		int getExtra();

		int recalculateExtra(int extra, int newCookie);

		int getCookie();

		void setCookie(int value);

		int getOffset();

		boolean isEmpty();
	}
	
	int getCookie(int numberOutgoingStatesAndCookie);

	int getMaxCookieSize();
}