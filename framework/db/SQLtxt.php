<?php

/**
 * Class used to represent a string that should be written as VARCHAR (text)
 * durning ODBC database driver's autocasting
 *
 * @author Gabriele Tozzi <gabriele@tozzi.eu>
 */
class SQLtxt {
	private $text;

	public function __construct( $text ) {
		$this->text = $text;
	}
	public function __toString() {
		return $this->text;
	}
}
