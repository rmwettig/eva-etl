package de.ingef.eva.utility;

/**
 * Creates alphabetic aliases, e.g. a -> z, aa -> zz
 */
public final class Alias {

	private int _position = 0;
	private char _currentChar = 'a';
	private char[] _alias;

	public Alias(int maxLength) {
		_alias = new char[maxLength];
	}

	public boolean hasNext() {
		// if not all positions are used yet
		if (_position < _alias.length - 1)
			return true;

		// otherwise test if any position is not a 'z' character
		for (int i = _position; i >= 0; i--) {
			if (_alias[i] != 'z')
				return true;
		}
		return false;
	}

	public String findNextAlias() {
		if (_currentChar > 'z') {
			handleOverflow();
		}
		_alias[_position] = _currentChar++;
		return new String(_alias).trim();
	}

	private void handleOverflow() {
		_currentChar = 'a';

		// find a non-'z' character at any previous position
		int i = _position - 1;
		while (i >= 0) {
			if (_alias[i] != 'z') {
				_alias[i]++;
				break;
			}
			i--;
		}
		// if no 'z' was found begin a new position
		if (i < 0)
			_alias[++_position] = _currentChar;
	}

	public void reset() {
		_position = 0;
		_currentChar = 'a';
		int i = 0;
		while (i < _alias.length &&_alias[i] != '\u0000')
			_alias[i++] = '\u0000';
	}
}
