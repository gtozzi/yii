<?php
/**
 * CMsCommandBuilder class file.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @author Christophe Boulain <Christophe.Boulain@gmail.com>
 * @author Wei Zhuo <weizhuo[at]gmail[dot]com>
 * @author Gabriele Tozzi <gabriele@tozzi.eu>
 * @link http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

/**
 * COdbcCommandBuilder provides basic methods to create query commands for tables for Odbc Servers.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @author Christophe Boulain <Christophe.Boulain@gmail.com>
 * @author Wei Zhuo <weizhuo[at]gmail[dot]com>
 * @author Gabriele Tozzi <gabriele@tozzi.eu>
 * @author Carlo Denaro <carlo.denaro@gmail.com>
 * @version $Id$
 * @package system.db.schema.odbc
 * @since 1.1.7
 */
class COdbcCommandBuilder extends CDbCommandBuilder
{
	/**
	 * Creates a COUNT(*) command for a single table.
	 * @param mixed $table the table schema ({@link CDbTableSchema}) or the table name (string).
	 * @param CDbCriteria $criteria the query criteria
	 * @param string $alias the alias name of the primary table. Defaults to 't'.
	 * @return CDbCommand query command.
	 */
	public function createCountCommand($table,$criteria,$alias='t')
	{
		$this->ensureTable($table);
		if($criteria->alias!='')
			$alias=$criteria->alias;
		$alias=$this->getSchema()->quoteTableName($alias);

		// Workaround for PHP bug #44643 (all parameters are binded as TEXT by PDO)
		$this->fixCriteria($criteria);

		if(!empty($criteria->group) || !empty($criteria->having))
		{
			$select=is_array($criteria->select) ? implode(', ',$criteria->select) : $criteria->select;
			if($criteria->alias!='')
				$alias=$criteria->alias;
			$sql=($criteria->distinct ? 'SELECT DISTINCT':'SELECT')." {$select} FROM {$table->rawName} $alias";
			$sql=$this->applyJoin($sql,$criteria->join);
			$sql=$this->applyCondition($sql,$criteria->condition);
			$sql=$this->applyGroup($sql,$criteria->group);
			$sql=$this->applyHaving($sql,$criteria->having);
			$sql="SELECT COUNT(*) FROM ($sql) sq";
		}
		else
		{
			if(is_string($criteria->select) && stripos($criteria->select,'count')===0)
				$sql="SELECT ".$criteria->select;
			else if($criteria->distinct)
			{
				if(is_array($table->primaryKey))
				{
					$pk=array();
					foreach($table->primaryKey as $key)
						$pk[]=$alias.'.'.$key;
					$pk=implode(', ',$pk);
				}
				else
					$pk=$alias.'.'.$table->primaryKey;
				$sql="SELECT COUNT(DISTINCT $pk)";
			}
			else
				$sql="SELECT COUNT(*)";
			$sql.=" FROM {$table->rawName} $alias";
			$sql=$this->applyJoin($sql,$criteria->join);
			$sql=$this->applyCondition($sql,$criteria->condition);
		}

		$command=$this->getDbConnection()->createCommand($sql);
		$this->bindValues($command,$criteria->params);
		return $command;
	}

	/**
	 * Creates a SELECT command for a single table.
	 * Override parent implementation to check if an orderby clause if specified when querying with an offset
	 * @param CDbTableSchema $table the table metadata
	 * @param CDbCriteria $criteria the query criteria
	 * @param string $alias the alias name of the primary table. Defaults to 't'.
	 * @return CDbCommand query command.
	 */
	public function createFindCommand($table,$criteria,$alias='t')
	{
		$criteria=$this->checkCriteria($table,$criteria);
		//return parent::createFindCommand($table,$criteria,$alias);
		$this->ensureTable($table);
		$select=is_array($criteria->select) ? implode(', ',$criteria->select) : $criteria->select;
		if($criteria->alias!='')
			$alias=$criteria->alias;

		// issue 1432: need to expand * when SQL has JOIN
        /*
		if($select==='*' && !empty($criteria->join))
		{
			$prefix=$alias.'.';
			$select=array();
			foreach($table->getColumnNames() as $name)
				$select[]=$prefix.$this->_schema->quoteColumnName($name);
			$select=implode(', ',$select);
		}
        */

		$sql=($criteria->distinct ? 'SELECT DISTINCT':'SELECT')." {$select} FROM {$table->rawName} $alias";
		$sql=$this->applyJoin($sql,$criteria->join);
		$sql=$this->applyCondition($sql,$criteria->condition);
		$sql=$this->applyGroup($sql,$criteria->group);
		$sql=$this->applyHaving($sql,$criteria->having);
		$sql=$this->applyOrder($sql,$criteria->order);
		$sql=$this->applyLimit($sql,$criteria->limit,$criteria->offset);

        $command=$this->getDbConnection()->createCommand($sql);
		$this->bindValues($command,$criteria->params);
		return $command;

	}

	/**
	 * Creates an INSERT command.
	 * @param mixed $table the table schema ({@link CDbTableSchema}) or the table name (string).
	 * @param array $data data to be inserted (column name=>column value). If a key is not a valid column name, the corresponding value will be ignored.
	 * @return CDbCommand insert command
	 */
	public function createInsertCommand($table,$data)
	{
		$this->ensureTable($table);
		$fields=array();
		$values=array();
		$placeholders=array();
		$i=0;
		foreach($data as $name=>$value)
		{
			if(($column=$table->getColumn($name))!==null && ($value!==null || $column->allowNull))
			{
				$fields[]=$column->rawName;
				if($value instanceof CDbExpression)
				{
					$placeholders[]=$value->expression;
					foreach($value->params as $n=>$v)
						$values[$n]=$v;
				}
				else
				{
					// Workaround for PHP bug #44643 (all parameters are binded as TEXT by PDO)
					$v = $column->typecast($value);
					$p = self::PARAM_PREFIX.$i;
					$this->castBindParameter($v, $p);
					// ----
					$placeholders[]=$p;
					$values[self::PARAM_PREFIX.$i]=$v;
					$i++;
				}
			}
		}
		if($fields===array())
		{
			$pks=is_array($table->primaryKey) ? $table->primaryKey : array($table->primaryKey);
			foreach($pks as $pk)
			{
				$fields[]=$table->getColumn($pk)->rawName;
				$placeholders[]='NULL';
			}
		}
		$sql="INSERT INTO {$table->rawName} (".implode(', ',$fields).') VALUES ('.implode(', ',$placeholders).')';
		$command=$this->getDbConnection()->createCommand($sql);

		foreach($values as $name=>$value)
			$command->bindValue($name,$value);

		return $command;
	}

	/**
	 * Creates an UPDATE command.
	 * Override parent implementation because odbc don't want to update an identity column
	 * @param CDbTableSchema $table the table metadata
	 * @param array $data list of columns to be updated (name=>value)
	 * @param CDbCriteria $criteria the query criteria
	 * @return CDbCommand update command.
	 */
	public function createUpdateCommand($table,$data,$criteria)
	{
		$criteria=$this->checkCriteria($table,$criteria);
		$fields=array();
		$values=array();
		$bindByPosition=isset($criteria->params[0]);
		$i=0;
		foreach($data as $name=>$value)
		{
			if(($column=$table->getColumn($name))!==null)
			{
				if ($table->sequenceName !== null && $column->isPrimaryKey === true) continue;
				if ($column->dbType === 'timestamp') continue;
				if($value instanceof CDbExpression)
					$fields[]=$column->rawName.'='.$value->expression;
				else if($bindByPosition)
				{
					$fields[]=$column->rawName.'=?';
					$values[]=$column->typecast($value);
				}
				else
				{
					// Workaround for PHP bug #44643 (all parameters are binded as TEXT by PDO)
					$v = $column->typecast($value);
					$p = self::PARAM_PREFIX.$i;
					$this->castBindParameter($v, $p);
					// ----

					$fields[]=$column->rawName.'='.$p;
					$values[self::PARAM_PREFIX.$i]=$v;
					$i++;
				}
			}
		}
		if($fields===array())
			throw new CDbException(Yii::t('yii','No columns are being updated for table "{table}".',
				array('{table}'=>$table->name)));
		$sql="UPDATE {$table->rawName} SET ".implode(', ',$fields);
		$sql=$this->applyJoin($sql,$criteria->join);
		$sql=$this->applyCondition($sql,$criteria->condition);
		$sql=$this->applyOrder($sql,$criteria->order);
		$sql=$this->applyLimit($sql,$criteria->limit,$criteria->offset);

		$command=$this->getDbConnection()->createCommand($sql);
		$this->bindValues($command,array_merge($values,$criteria->params));

		return $command;
	}

	/**
	 * Creates a DELETE command.
	 * Override parent implementation to check if an orderby clause if specified when querying with an offset
	 * @param CDbTableSchema $table the table metadata
	 * @param CDbCriteria $criteria the query criteria
	 * @return CDbCommand delete command.
	 */
	public function createDeleteCommand($table,$criteria)
	{
		$criteria=$this->checkCriteria($table, $criteria);
		return parent::createDeleteCommand($table, $criteria);
	}

	/**
	 * Creates an UPDATE command that increments/decrements certain columns.
	 * Override parent implementation to check if an orderby clause if specified when querying with an offset
	 * @param CDbTableSchema $table the table metadata
	 * @param CDbCriteria $counters the query criteria
	 * @param array $criteria counters to be updated (counter increments/decrements indexed by column names.)
	 * @return CDbCommand the created command
	 * @throws CException if no counter is specified
	 */
	public function createUpdateCounterCommand($table,$counters,$criteria)
	{
		$criteria=$this->checkCriteria($table, $criteria);
		return parent::createUpdateCounterCommand($table, $counters, $criteria);
	}

	/**
	 * This is a port from Prado Framework.
	 *
	 * Overrides parent implementation. Alters the sql to apply $limit and $offset.
	 * The idea for limit with offset is done by modifying the sql on the fly
	 * with numerous assumptions on the structure of the sql string.
	 * The modification is done with reference to the notes from
	 * http://troels.arvin.dk/db/rdbms/#select-limit-offset
	 *
	 * <code>
	 * SELECT * FROM (
	 *  SELECT TOP n * FROM (
	 *    SELECT TOP z columns      -- (z=n+skip)
	 *    FROM tablename
	 *    ORDER BY key ASC
	 *  ) AS FOO ORDER BY key DESC -- ('FOO' may be anything)
	 * ) AS BAR ORDER BY key ASC    -- ('BAR' may be anything)
	 * </code>
	 *
	 * <b>Regular expressions are used to alter the SQL query. The resulting SQL query
	 * may be malformed for complex queries.</b> The following restrictions apply
	 *
	 * <ul>
	 *   <li>
	 * In particular, <b>commas</b> should <b>NOT</b>
	 * be used as part of the ordering expression or identifier. Commas must only be
	 * used for separating the ordering clauses.
	 *  </li>
	 *  <li>
	 * In the ORDER BY clause, the column name should NOT be be qualified
	 * with a table name or view name. Alias the column names or use column index.
	 * </li>
	 * <li>
	 * No clauses should follow the ORDER BY clause, e.g. no COMPUTE or FOR clauses.
	 * </li>
	 *
	 * @param string $sql SQL query string.
	 * @param integer $limit maximum number of rows, -1 to ignore limit.
	 * @param integer $offset row offset, -1 to ignore offset.
	 * @return string SQL with limit and offset.
	 *
	 * @author Wei Zhuo <weizhuo[at]gmail[dot]com>
	 */
	public function applyLimit($sql, $limit, $offset)
	{
		$limit = $limit!==null ? intval($limit) : -1;
		$offset = $offset!==null ? intval($offset) : -1;
		if ($limit > 0 && $offset <= 0) //just limit
			$sql = preg_replace('/^([\s(])*SELECT( DISTINCT)?(?!\s*TOP\s*\()/i',"\\1SELECT\\2 TOP $limit", $sql);
		else if($limit > 0 && $offset > 0)
			$sql = $this->rewriteLimitOffsetSql($sql, $limit,$offset);
		return $sql;
	}

	/**
	 * Rewrite sql to apply $limit > and $offset > 0 for ODBC database.
	 * See http://troels.arvin.dk/db/rdbms/#select-limit-offset
	 * @param string $sql sql query
	 * @param integer $limit $limit > 0
	 * @param integer $offset $offset > 0
	 * @return sql modified sql query applied with limit and offset.
	 *
	 * @author Wei Zhuo <weizhuo[at]gmail[dot]com>
	 */
	protected function rewriteLimitOffsetSql($sql, $limit, $offset)
	{
		$fetch = $limit+$offset;
		$sql = preg_replace('/^([\s(])*SELECT( DISTINCT)?(?!\s*TOP\s*\()/i',"\\1SELECT\\2 TOP $fetch", $sql);
		$ordering = $this->findOrdering($sql);
		$orginalOrdering = $this->joinOrdering($ordering, '[__outer__]');
		$reverseOrdering = $this->joinOrdering($this->reverseDirection($ordering), '[__inner__]');
		$sql = "SELECT * FROM (SELECT TOP {$limit} * FROM ($sql) as [__inner__] {$reverseOrdering}) as [__outer__] {$orginalOrdering}";
		return $sql;
	}

	/**
	 * Base on simplified syntax http://msdn2.microsoft.com/en-us/library/aa259187(SQL.80).aspx
	 *
	 * @param string $sql $sql
	 * @return array ordering expression as key and ordering direction as value
	 *
	 * @author Wei Zhuo <weizhuo[at]gmail[dot]com>
	 */
	protected function findOrdering($sql)
	{
		if(!preg_match('/ORDER BY/i', $sql))
			return array();
		$matches=array();
		$ordering=array();
		preg_match_all('/(ORDER BY)[\s"\[](.*)(ASC|DESC)?(?:[\s"\[]|$|COMPUTE|FOR)/i', $sql, $matches);
		if(count($matches)>1 && count($matches[2]) > 0)
		{
			$parts = explode(',', $matches[2][0]);
			foreach($parts as $part)
			{
				$subs=array();
				if(preg_match_all('/(.*)[\s"\]](ASC|DESC)$/i', trim($part), $subs))
				{
					if(count($subs) > 1 && count($subs[2]) > 0)
					{
						$name='';
						foreach(explode('.', $subs[1][0]) as $p)
						{
							if($name!=='')
								$name.='.';
							$name.='[' . trim($p, '[]') . ']';
						}
						$ordering[$name] = $subs[2][0];
					}
					//else what?
				}
				else
					$ordering[trim($part)] = 'ASC';
			}
		}

		// replacing column names with their alias names
		foreach($ordering as $name => $direction)
		{
			$matches = array();
			$pattern = '/\s+'.str_replace(array('[',']'), array('\[','\]'), $name).'\s+AS\s+(\[[^\]]+\])/i';
			preg_match($pattern, $sql, $matches);
			if(isset($matches[1]))
			{
				$ordering[$matches[1]] = $ordering[$name];
				unset($ordering[$name]);
			}
		}

		return $ordering;
	}

	/**
	 * @param array $orders ordering obtained from findOrdering()
	 * @param string $newPrefix new table prefix to the ordering columns
	 * @return string concat the orderings
	 *
	 * @author Wei Zhuo <weizhuo[at]gmail[dot]com>
	 */
	protected function joinOrdering($orders, $newPrefix)
	{
		if(count($orders)>0)
		{
			$str=array();
			foreach($orders as $column => $direction)
				$str[] = $column.' '.$direction;
			$orderBy = 'ORDER BY '.implode(', ', $str);
			return preg_replace('/\s+\[[^\]]+\]\.(\[[^\]]+\])/i', ' '.$newPrefix.'.\1', $orderBy);
		}
	}

	/**
	 * @param array $orders original ordering
	 * @return array ordering with reversed direction.
	 *
	 * @author Wei Zhuo <weizhuo[at]gmail[dot]com>
	 */
	protected function reverseDirection($orders)
	{
		foreach($orders as $column => $direction)
			$orders[$column] = strtolower(trim($direction))==='desc' ? 'ASC' : 'DESC';
		return $orders;
	}


	/**
	 * Checks if the criteria has an order by clause when using offset/limit.
	 * Override parent implementation to check if an orderby clause if specified when querying with an offset
	 * If not, order it by pk.
     * Also, generate cast statements against PHP bug #44643
	 * @param COdbcTableSchema $table table schema
	 * @param CDbCriteria $criteria criteria
	 * @return CDbCrireria the modified criteria
	 */
	protected function checkCriteria($table, $criteria)
	{
		if ($criteria->offset > 0 && $criteria->order==='')
		{
			$criteria->order=is_array($table->primaryKey)?implode(',',$table->primaryKey):$table->primaryKey;
		}

		// PHP bug #44643 fix start
		foreach($criteria->params as $k=>$p) {
			$k_casted = $k;
			$p_casted = $p;
			$this->castBindParameter($p_casted, $k_casted);
			$criteria->params[$k] = $p_casted;
			$criteria->condition = str_replace($k, $k_casted, $criteria->condition);
		}
		// PHP bug #44643 fix end

		return $criteria;
	}

	/**
	 * Generates the expression for selecting rows with specified composite key values.
	 * @param CDbTableSchema $table the table schema
	 * @param array $values list of primary key values to be selected within
	 * @param string $prefix column prefix (ended with dot)
	 * @return string the expression for selection
	 * @since 1.0.4
	 */
	protected function createCompositeInCondition($table,$values,$prefix)
	{
		$vs=array();
		foreach($values as $value)
		{
			$c=array();
			foreach($value as $k=>$v)
				$c[]=$prefix.$table->columns[$k]->rawName.'='.$v;
			$vs[]='('.implode(' AND ',$c).')';
		}
		return '('.implode(' OR ',$vs).')';
	}

	/**
	 * Returns a filtered version of SQL statement with params casted to avoid
	 * PHP bug #44643
	 *
	 * @param string $sql: The SQL query
	 * @param array  $params: Associative array of params
	 * @since 1.1.7
	 */
	public function applySqlBugFilters(&$sql, &$params) {
		foreach( $params as $k=>$v ) {
			$old_k = $k;
			$this->castBindParameter($v, $k);
			$params[$old_k] = $v;
			$sql = str_replace($old_k, $k, $sql);
		}
	}

	/**
	 * Generates the casts to avoid PHP bug #44643
	 * (all parameters are binded as TEXT by PDO)
	 * @param mixed  $value: The value for the column
	 * @param string $param: The param name code
	 * @return void: Values are passed byRef and modified realtime
	 * @since 1.1.7
	 */
	protected function castBindParameter(&$value, &$param) {
		if( is_object($value) ) 
		{
			$param = $param;
		}else{
			switch( gettype($value) ) {
			case 'integer':
				$param = "CAST(CAST($param AS VARCHAR) AS INTEGER)";
				break;
			case 'float':
			case 'double':
				$param = "CAST(CAST($param AS VARCHAR) AS FLOAT)";
				break;
			case 'string':
				//If the value is a special SQLtxt object, leave it as-is
				if( is_object( $value ) && is_a($value, 'SQLtxt') )
				{
					$param = $param;
				} else {

					if( strlen($value) <= 255 )
						$param = "CAST($param AS VARCHAR)";
				}
				break;
			case 'NULL':
				// Varchar is safer
				$param = "CAST(CAST($param AS VARCHAR) AS INTEGER)";
				break;
			default:
				throw new Exception('Unknown data type: ' . gettype($value));
			}
		}
	}

	/**
	 * Fixes the CDBCriteria for bug above
	 */
	protected function fixCriteria(& $criteria) {
		if( $criteria->params ) {
			$replaces = array();
			foreach( $criteria->params as $name=>$value ) {
				$oldname = $name;
				$this->castBindParameter($value, $name);
				$replaces[$oldname] = $name;
			}
			foreach( $replaces as $o=>$n )
				$criteria->condition = str_replace($o,$n,$criteria->condition);
		}
	}

}
