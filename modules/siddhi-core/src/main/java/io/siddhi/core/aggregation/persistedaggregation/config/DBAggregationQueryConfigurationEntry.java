/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.aggregation.persistedaggregation.config;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class contains all the Siddhi RDBMS Event Table SQL query configuration mappings.
 */
@XmlRootElement(name = "database")
public class DBAggregationQueryConfigurationEntry {

    private String databaseName;
    private String category;
    private double minVersion;
    private double maxVersion;
    private String stringSize;
    private int fieldSizeLimit;
    private RDBMSTypeMapping rdbmsTypeMapping;
    private DBAggregationSelectQueryTemplate dbAggregationSelectQueryTemplate;
    private DBAggregationSelectFunctionTemplate dbAggregationSelectFunctionTemplate;
    private DBAggregationTimeConversionDurationMapping dbAggregationTimeConversionDurationMapping;
    private int batchSize;
    private boolean batchEnable = false;
    private String collation;
    private boolean transactionSupported = true;

    @XmlAttribute(name = "name", required = true)
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @XmlAttribute(name = "minVersion")
    public double getMinVersion() {
        return minVersion;
    }

    public void setMinVersion(double minVersion) {
        this.minVersion = minVersion;
    }

    @XmlAttribute(name = "maxVersion")
    public double getMaxVersion() {
        return maxVersion;
    }

    public void setMaxVersion(double maxVersion) {
        this.maxVersion = maxVersion;
    }

    @XmlAttribute(name = "category")
    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @XmlElement(name = "batchEnable")
    public boolean getBatchEnable() {
        return batchEnable;
    }

    public void setBatchEnable(boolean batchEnable) {
        this.batchEnable = batchEnable;
    }

    @XmlElement(name = "collation")
    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public String getStringSize() {
        return stringSize;
    }

    public void setStringSize(String stringSize) {
        this.stringSize = stringSize;
    }

    public boolean isTransactionSupported() {
        return transactionSupported;
    }

    public void setTransactionSupported(boolean transactionSupported) {
        this.transactionSupported = transactionSupported;
    }

    @XmlElement(name = "typeMapping", required = true)
    public RDBMSTypeMapping getRdbmsTypeMapping() {
        return rdbmsTypeMapping;
    }

    public void setRdbmsTypeMapping(RDBMSTypeMapping rdbmsTypeMapping) {
        this.rdbmsTypeMapping = rdbmsTypeMapping;
    }

    @XmlElement(name = "selectQueryTemplate")
    public DBAggregationSelectQueryTemplate getRdbmsSelectQueryTemplate() {
        return dbAggregationSelectQueryTemplate;
    }

    public void setRdbmsSelectQueryTemplate(DBAggregationSelectQueryTemplate dbAggregationSelectQueryTemplate) {
        this.dbAggregationSelectQueryTemplate = dbAggregationSelectQueryTemplate;
    }

    @XmlElement(name = "batchSize", required = true)
    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @XmlElement(name = "fieldSizeLimit", required = false)
    public int getFieldSizeLimit() {
        return fieldSizeLimit;
    }

    public void setFieldSizeLimit(int fieldSizeLimit) {
        this.fieldSizeLimit = fieldSizeLimit;
    }

    @XmlElement(name = "selectQueryFunctions")
    public DBAggregationSelectFunctionTemplate getRdbmsSelectFunctionTemplate() {
        return dbAggregationSelectFunctionTemplate;
    }

    public void setRdbmsSelectFunctionTemplate(DBAggregationSelectFunctionTemplate
                                                       dbAggregationSelectFunctionTemplate) {
        this.dbAggregationSelectFunctionTemplate = dbAggregationSelectFunctionTemplate;
    }

    @XmlElement(name = "timeConversionDurationMapping")
    public DBAggregationTimeConversionDurationMapping getDbAggregationTimeConversionDurationMapping() {
        return dbAggregationTimeConversionDurationMapping;
    }

    public void setDbAggregationTimeConversionDurationMapping(
            DBAggregationTimeConversionDurationMapping dbAggregationTimeConversionDurationMapping) {
        this.dbAggregationTimeConversionDurationMapping = dbAggregationTimeConversionDurationMapping;
    }
}

