<?php namespace Jg\Dbal;
class Expr {
    
    public $expr;
    
    public function __construct($expr) {
        $this->expr = $expr; 
    }
    
    public function __toString() {
        return $this->expr;
    }
}