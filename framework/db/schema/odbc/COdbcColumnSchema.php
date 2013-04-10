<?php
/**
 * COdbcColumnSchema class file.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @author Christophe Boulain <Christophe.Boulain@gmail.com>
 * @author Gabriele Tozzi <gabriele@tozzi.eu>
 * @link http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

/**
 * COdbcColumnSchema class describes the column meta data of a ODBC table.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @author Christophe Boulain <Christophe.Boulain@gmail.com>
 * @author Gabriele Tozzi <gabriele@tozzi.eu>
 * @version $Id$
 * @package system.db.schema.mssql
 * @since 1.1.7
 */
class COdbcColumnSchema extends CDbColumnSchema
{
	/**
	 * Extracts the PHP type from DB type.
	 * @param string $dbType DB type
	 */
	protected function extractType($dbType)
	{
		if(strpos($dbType,'float')!==false || strpos($dbType,'real')!==false)
			$this->type='double';
		else if(strpos($dbType,'bigint')===false && (strpos($dbType,'int')!==false || strpos($dbType,'smallint')!==false || strpos($dbType,'tinyint')))
			$this->type='integer';
		else if(strpos($dbType,'bit')!==false)
			$this->type='boolean';
		else
			$this->type='string';
	}

	/*
	 * Extracts the default value for the column.
	 * The value is typecasted to correct PHP type.
	 * @param mixed $defaultValue the default value obtained from metadata
	 */
	protected function extractDefault($defaultValue)
	{
		if($this->dbType==='timestamp' )
			$this->defaultValue=null;
		else
			parent::extractDefault(str_replace(array('(',')',"'"), '', $defaultValue));
	}

	/**
	 * Extracts size, precision and scale information from column's DB type.
	 * We do nothing here, since sizes and precisions have been computed before.
	 * @param string $dbType the column's DB type
	 */
	protected function extractLimit($dbType)
	{
	}

	/**
	 * Converts the input value to the type that this column is of.
	 * @param mixed $value input value
	 * @return mixed converted value
	 */
	public function typecast($value)
	{
		if($this->type==='boolean')
			return $value===null ? null : ( $value ? 1 : 0 );
		else
			return parent::typecast($value);
	}
}
