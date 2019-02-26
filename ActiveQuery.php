<?php

namespace rezaid\geopoint;

use yii\db\ActiveQuery as YiiActiveQuery;
use yii\db\Expression;
use yii\db\Query;
use yii\db\Exception;

class ActiveQuery extends YiiActiveQuery
{

    protected $_skipPrep = false;

    /**
     * Finds nearest models by provided current location point and radius.
     *
     * @param $from
     * @param $attribute
     * @param int $radius
     * @param string $unit
     * @return $this
     */
    public function nearest($from, $attribute, $radius = 100, $unit = 'km')
    {
        $lenPerDegree = 111.045;    // km per degree latitude; for miles, use 69.0
        if ($unit == 'mil') {
            $lenPerDegree = 69.0;
        }

        $from = explode(',', $from);
        if (!is_array($from)) {
            return $this;
        }

        $lat = trim($from[0]);
        $lng = trim($from[1]);

        /** @var \yii\db\ActiveRecord $modelCls */
        $modelCls = $this->modelClass;

        if ($modelCls::getDb()->driverName === 'mysql') {
            $subQuery = $this->create($this)
                ->addSelect([
                    '_d' => "($lenPerDegree * ST_Distance($attribute, ST_PointFromText(:point)))"
                ])
                ->params([':point' => "POINT($lat $lng)"]);
        } else {
            if ($modelCls::getDb()->driverName === 'pgsql') {
                $subQuery = $this->create($this)
                    ->addSelect([
                        '_d' => "($lenPerDegree * ($attribute <-> POINT(:lt,:lg)))"
                    ])
                    ->params([':lg' => $lng, ':lt' => $lat]);
            } else {
                throw new Exception('Only MqSQL and PostgreSQL are supported by ' . self::className());
            }
        }

        $this->select(['*'])
            ->from(['distance' => $subQuery])
            ->where(['<', '_d', $radius])
            ->orderBy([
                '_d' => SORT_ASC
            ]);

        $this->limit = null;
        $this->offset = null;
        $this->distinct = null;
        $this->groupBy = null;
        $this->join = null;
        $this->union = null;

        return $this;
    }

    public function prepare($builder)
    {
        /** @var ActiveRecord $modelClass */
        $modelClass = $this->modelClass;
        if ($modelClass::getDb()->driverName === 'pgsql') {
            return parent::prepare($builder);
        }

        if (!$this->_skipPrep) {   // skip in case of queryScalar; it's not needed, and we get an SQL error (duplicate column names)
            if (empty($this->select)) {
                list(, $alias) = $this->getTableNameAndAlias();
                $this->select = ["$alias.*"];
                $this->allColumns();
            } else {
                $schema = $modelClass::getTableSchema();
                foreach ($this->select as $field) {
                    if (preg_match('/\*/', $field)) {
                        $this->allColumns();
                    } elseif (
                        $field instanceof Query == false &&
                        $field instanceof Expression == false
                    ) {
                        $column = $schema->getColumn($field);
                        if (ActiveRecord::isPoint($column)) {
                            $this->addSelect(["ST_AsText($field) AS $field"]);
                        }
                    }
                }
            }
        }

        return parent::prepare($builder);
    }

    protected function allColumns()
    {
        /** @var ActiveRecord $modelClass */
        $modelClass = $this->modelClass;
        $schema = $modelClass::getTableSchema();

        foreach ($schema->columns as $column) {
            if (ActiveRecord::isPoint($column)) {
                $field = $column->name;
                $this->addSelect(["ST_AsText($field) AS $field"]);
            }
        }
    }

    protected function queryScalar($selectExpression, $db)
    {
        $this->_skipPrep = true;
        $r = parent::queryScalar($selectExpression, $db);
        $this->_skipPrep = false;

        return $r;
    }

    /**
     * Returns the table name and the table alias for [[modelClass]]. This method is duplicate of parent method with the
     * same name.
     *
     * @return array the table name and the table alias.
     */
    private function getTableNameAndAlias()
    {
        if (empty($this->from)) {
            $tableName = $this->getPrimaryTableName();
        } else {
            $tableName = '';
            foreach ($this->from as $alias => $tableName) {
                if (is_string($alias)) {
                    return [$tableName, $alias];
                }
                break;
            }
        }

        if (preg_match('/^(.*?)\s+({{\w+}}|\w+)$/', $tableName, $matches)) {
            $alias = $matches[2];
        } else {
            $alias = $tableName;
        }

        return [$tableName, $alias];
    }
}
