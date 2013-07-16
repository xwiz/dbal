<?php namespace  Dbal;

class Select {

    const JOIN_INNER = 'INNER JOIN';
    const JOIN_OUTER = 'OUTER JOIN';
    const JOIN_LEFT = 'LEFT JOIN';
    const JOIN_RIGHT = 'RIGHT JOIN';
    const SQL_STAR = '*';

    /**
     * @var bool
     */
    protected $_prefixColumnsWithTable = true;

    /**
     * @var string
     */
    protected $_table;

    /**
     * @var array
     */
    protected $_columns = array(self::SQL_STAR);

    /**
     * @var array
     */
    protected $_joins = array();

    /**
     * @var Where
     */
    protected $_where = array();

    /**
     * @var null|string
     */
    protected $_order = array();

    /**
     * @var null|array
     */
    protected $_group = array();

    /**
     * @var null|string|array
     */
    protected $_having = array();

    /**
     * @var int|null
     */
    protected $_limit;

    /**
     * @var int|null
     */
    protected $_offset;

    /**
     * @var type 
     */
    protected $_parameters = array();

    /**
     * A function taking two parameters $sql and $bind to run the built query
     * @var Callable 
     */
    protected $_runner;
    
    /**
     *  array('"','"');	//SQL92/Sqllite
     *  array('[',']');	//SqlServer
     */
    protected $_quoteSymbols = array('`', '`'); //MySQL    
            

    /**
     * Constructor
     * 
     * @param  null|string $table
     * @return void
     */
    public function __construct($table = null, array $columns = null, $runner = null) {
        $table and $this->from($table);
        $columns and $this->columns($columns);
        $this->_runner = $runner;
    }

    /**
     * Create from clause
     * 
     * @param  string $table
     * @return Select
     */
    public function from($table, array $columns = null) {

        if (!is_string($table)) {
            throw new Exception('$table must be a string or an instance of TableIdentifier');
        }

        $this->_table = $table;

        is_null($columns) or $this->columns($columns);

        return $this;
    }

    /**
     *
     * @param array $columns
     * @param type $prefixColumnsWithTable
     * @return \Select 
     */
    public function columns(array $columns, $prefixColumnsWithTable = true) {
        $columns and $this->_columns = $columns;
        $this->_prefixColumnsWithTable = (bool) $prefixColumnsWithTable;
        return $this;
    }

    public function addColumn($column) {
        $this->_columns[] = $column;
        return $this;
    }

    /**
     * Create join clause
     * 
     * @param  string $name 
     * @param  string $on 
     * @param  string|array $columns 
     * @param  string $type one of the JOIN_* constants
     * @return Select
     */
    public function join($name, $on, $columns = self::SQL_STAR, $type = self::JOIN_INNER) {

        is_array($columns) or $columns = array($columns);

        $this->_joins[] = array(
            'name' => $name,
            'on' => $on,
            'columns' => $columns,
            'type' => $type
        );
        return $this;
    }

    /**
     * Create join clause
     * 
     * @param  string $name 
     * @param  string $on 
     * @param  string|array $columns 
     * @return Select
     */
    public function joinLeft($name, $on, $columns = self::SQL_STAR) {
        $this->join($name, $on, $columns, self::JOIN_LEFT);
        return $this;
    }

    /**
     * Create join clause
     * 
     * @param  string $name 
     * @param  string $on 
     * @param  string|array $columns 
     * @return Select
     */
    public function joinOuter($name, $on, $columns = self::SQL_STAR) {
        $this->join($name, $on, $columns, self::JOIN_OUTER);
        return $this;
    }

    /**
     * Create join clause
     * 
     * @param  string $name 
     * @param  string $on 
     * @param  string|array $columns 
     * @return Select
     */
    public function joinRight($name, $on, $columns = self::SQL_STAR) {
        $this->join($name, $on, $columns, self::JOIN_RIGHT);
        return $this;
    }
    
    public function orWhere($where,$bind = array()){
        $this->_where[] = array('OR',array($where,$bind));
        return $this;
    }

    /**
     * Create where clause
     * 
     * @param  string|array $where 
     * @return Select
     */
    public function where($where,$bind = array()) {
        $this->_where[] = array('AND',array($where,$bind));
        return $this;
    }

    public function group($group) {
        is_array($group) or $group = array($group);

        foreach ($group as $o) {
            $this->_group[] = $o;
        }
        return $this;
    }

    /**
     * Create having clause
     *
     * @param  string|array $having
     * @return Select
     */
    public function having($having,$bind = array()) {
        $this->_having[] = array('AND',array($having,$bind));
        return $this;
    }
    
    public function orHaving($having) {
        $this->_having[] = array('OR',array($having,$bind));
        return $this;
    }
    
    public function clearOrder(){
	$this->_order = array();
	return $this;
    }
    public function clearLimit(){
	$this->_limit = null;
	return $this;
    }
    public function clearOffset(){
	$this->_offset = null;
	return $this;
    }
    public function clearGroup(){
	$this->_group = array();
	return $this;
    }
    public function clearWhere(){
	$this->_where = array();
	return $this;
    }
    public function clearJoins(){
	$this->_joins = array();
	return $this;
    }
    public function clearHaving(){
	$this->_having = array();
	return $this;
    }

    /**
     * @param string|array $order
     * @return Select
     */
    public function order($order) {
        
        if(!$order) return $this;
        
        if (is_string($order)) {
            if (strpos($order, ',') !== false) {
                $order = preg_split('#,\s+#', $order);
            } else {
                $order = (array) $order;
            }
        }
        foreach ($order as $v) {
            $this->_order[] = $v;
        }
        return $this;
    }

    public function limit($limit) {
        $this->_limit = $limit;
        return $this;
    }

    public function offset($offset) {
        $this->_offset = $offset;
        return $this;
    }

    /**
     * Get SQL string for statement
     *
     * @return string
     */
    public function getSql() {
        $this->_parameters = array();
	
        return 'SELECT ' . $this->_buildColumns()
                . 'FROM ' . $this->quoteIdentifier($this->_table) . "\n"
                . $this->_buildJoin()
                . $this->_buildWhere()
                . $this->_buildGroup()
                . $this->_buildHaving()
                . $this->_buildOrder()
                . $this->_buildPagination();
    }

    public function getParameters() {
        return $this->_parameters;
    }

    public function get() {
        return array($this->getSql(), $this->getParameters());
    }
    
    public function __call($method,$params){
        return $this->_runner->$method($this);
    }
    
    public function setRunner($runner){
        $this->_runner = $runner;
        return $this;
    }
    
    public static function make($table = null,$columns = null){
        return new static($table,$columns);
    }

    protected function _buildColumns() {

        // process table columns
        $columns = array();

        $tables = $this->_joins;

        array_unshift($tables, array('name' => $this->_table, 'columns' => $this->_columns));

        foreach ($tables as $table) {
	    
	    $name = explode(' as ',  strtolower($table['name']));
	    
            $tableName = $this->quoteIdentifier(array_pop($name));

            $quotedTable = $this->_prefixColumnsWithTable ? $tableName . '.' : '';

            foreach ($table['columns'] as $columnIndexOrAs => $column) {

		// If its a star, we MUST prefix the column name with the table to avoid SQL error
                if ($column === self::SQL_STAR) {
                    $columns[] = $tableName .'.'. self::SQL_STAR;
                    continue;
                }

                $columnName = ($column instanceof Expr) ? $column : $quotedTable . $this->quoteIdentifierInFragment($column);

                is_string($columnIndexOrAs) and $columnName .= ' AS ' . $this->quoteIdentifier($columnIndexOrAs);

                $columns[] = $columnName;
            }
        }

        return implode(',', $columns) . "\n";
    }

    protected function _buildJoin() {
        if (!count($this->_joins)) {
            return null;
        }

        $spec = '';

        foreach ($this->_joins as $join) {
            $spec .= ' ' . $join['type'] . ' ' . $this->quoteIdentifierInFragment($join['name']) . ' ON ' . $join['on'];
        }

        return $spec . "\n";
    }

    protected function _buildWhere() {
        if (!count($this->_where)) {
            return null;
        }

        return 'WHERE ' . $this->_buildExpression($this->_where) . "\n";
    }

    protected function _buildGroup() {
        if (!count($this->_group)) {
            return null;
        }

        $groups = array();

        foreach ($this->_group as $column) {
            $groups[] = $column instanceof Expr ? $column : $this->quoteIdentifierInFragment($column);
        }

        return 'GROUP BY ' . implode(',', $groups) . "\n";
    }

    protected function _buildHaving() {
        if (!count($this->_having)) {
            return null;
        }
        return 'HAVING ' .  $this->_buildExpression($this->_having) . "\n";
    }

    protected function _buildOrder() {
        if (!count($this->_order)) {
            return null;
        }

        $orders = array();

        foreach ($this->_order as $col) {

            strpos($col, ' ') !== false and list($col, $dir) = preg_split('# #', $col, 2);

            $dir = (isset($dir) && strtoupper($dir) == 'DESC' ? 'DESC' : 'ASC');

            $orders[] = $this->quoteIdentifierInFragment($col) . ' ' . $dir;
        }

        return 'ORDER BY ' . implode(',', $orders) . "\n";
    }

    protected function _buildPagination() {

        if ($this->_limit === null) {
            return null;
        }

        return 'LIMIT ' . ($this->_offset === null ? null : (intval($this->_offset) . ',')) . intval($this->_limit) . "\n";
    }

    protected function _buildExpression($expression) {

        $sql = '';

        foreach ($expression as $part) {
            $ANDorOR = $sql ? $part[0] : '';
            $part = $part[1];

            is_array($part) or $part = array($part);

            isset($part[0]) and $sql .= ' '. $ANDorOR . ' (' . $part[0] . ')';

            if (isset($part[1])) {

                $bind = is_array($part[1]) ? $part[1] : array($part[1]);
                foreach ($bind as $b) {
                    $this->_parameters[] = $b;
                }
            }
        }

        return $sql;
    }

    /**
     *
     * @param type $identifier
     * @return type 
     */
    public function quoteIdentifierInFragment($identifier) {

        $parts = preg_split('#([\.\s])#', $identifier, -1, PREG_SPLIT_DELIM_CAPTURE);
        
        $safe = array('as',' ','.', '*', 'and', 'or', '(', ')', '=', '<', '>', '!=', '<>', '>=', '<=');

        foreach ($parts as $i => $part) {
            in_array(strtolower($part), $safe) or $parts[$i] = $this->quoteIdentifier($part);
        }

        return implode('', $parts);
    }

    public function quoteIdentifier($identifer) {
        list($q1, $q2) = $this->_getQuoteSymbols();
        return $q1 . str_replace(array($q1, $q2), array('\\' . $q1, '\\' . $q2), $identifer) . $q2;
    }

    protected function _getQuoteSymbols() {
        return $this->_quoteSymbols;
    }
    
    protected function _setQuoteSymbols($quote) {
        $this->_quoteSymbols = $quote;
    }

}
