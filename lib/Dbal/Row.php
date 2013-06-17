<?php

namespace Dbal;

class RowExecption extends \Exception {
    
}

abstract class Row extends \ArrayObject {
    
    /**
     * The Database Connection
     * @var Db 
     */
    private $_db;
    
    /**
     * The name of the auto increment id column
     * @var string|array 
     */
    public $idColumnName = 'id';
    
    public function __construct(Db $db, array $data = null) {
        
        $this->_db = $db;
        
        if(!is_null($data)){
            $this->replace($data);
        }

    }
    
    abstract public function getTable();
    
    /**
     * 
     * @param array $data
     * @return \Dbal\Row
     */
    public function replace(array $data){
        
        foreach($data as $key => $value){
            $this->offsetSet($key, $value);
        }

        return $this;
    }
    
    /**
     * Get the primary key id for this row
     * @return mixed
     */
    public function getId(){
        return parent::offsetExists($this->idColumnName) ? $this->offsetGet($this->idColumnName) : null;
    }
    
    /**
     * Get the db connection
     * @return \Dbal\Db $db 
     */
    public function getDb(){
        return $this->_db;
    }
    
    /**
     * 
     * @param type $param
     * @return type
     * @throws RowExecption
     */
    public function offsetGet($param){
        if(!parent::offsetExists($param)){
            throw new RowExecption('Column "' . $param . '" does not exist in the rowset in object "'.  get_class($this).'".');
        }

        return parent::offsetGet($param);
    }
    
    /**
     * 
     * @param type $param
     * @param type $value
     * @return \Dbal\Row
     */
    public function offsetSet($param,$value){
        
        $method = 'set'.ucfirst($param);
        
        method_exists($this, $method) ? $this->$method($value) : parent::offsetSet($param, $value);
        
        return $this;
    }
    
    /**
     * 
     * @param type $param
     * @param type $value
     */
    public function __set($param,$value){
       $this->offsetSet($param, $value);
    }
    
    /**
     * 
     * @param type $param
     * @return type
     */
    public function __get($param){
       return $this->offsetGet($param);
    }
    
    /**
     * 
     * @param type $param
     * @param type $value
     */
    public function setRaw($param,$value){
        parent::offsetSet($param,$value);
        return $this;
    }
    
    /**
     * 
     * @param type $assertUpdate
     * @return type
     * @throws SaveableRowException
     */
    public function save($assertUpdate = false){

        if(!$this->getId()){
            
            if($this->getDb()->insert($this->getTable(),$this->toArray())->rowCount() !== 1){
                throw new SaveableRowException;
            }
            
            $this->{$this->idColumnName} = $this->getDb()->lastInsertId();
            
        }else {
            
            if(($this->getDb()->update($this->getTable(),$this->toArray(),$this->idColumnName . ' = ?',$this->getId())->rowCount() !== 1) && $assertUpdate){
                throw new SaveableRowException;
            }
            
        }
        
        return $this->getId();
    }
    
    /**
     * 
     * @return type
     */
    public function load(){
        $data = $this->getDb()->fetchRow('SELECT * FROM '.$this->getTable().' WHERE '.$this->idColumnName . ' = ?',$this->getId());
        
        return $this->replace($data);
    }
    
    /**
     * 
     * @return type
     */
    public function toArray(){
        return $this->getArrayCopy();
    }
    
}
