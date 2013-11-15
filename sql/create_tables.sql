--
-- Copyright 2013 AMALTHEA REU; Dillon Rose; Michel Rouly
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
--  Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--


-- CREATE DATABASE SCHEMA (TABLES)
--  This step is required to create all required tables or update the 
--    existing schema 
--  Our schema name is `db_presentation`

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

CREATE SCHEMA IF NOT EXISTS `db_presentation` DEFAULT CHARACTER SET latin1 ;
USE `db_presentation` ;


-- ---------------------------------------------------------------------
--  Create table `db_presentation`.`tblWordLibrary` with columns:
--    idWord: globally unique identifier for every word    # primary key
--    dataset: identifier for dataset this word comes from
--    wordValue: textual value of this word (the word itself)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_presentation`.`tblWordLibrary` ( 
  `idWord` INT(10) UNSIGNED NOT NULL COMMENT 
    "globally unique identifier for every document (primary key)" , 
  `dataset` INT(10) UNSIGNED NOT NULL COMMENT 
    "identifier for dataset this word comes from" , 
  `wordValue` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL COMMENT 
    "textual value of this word (the word itself)" , 
  PRIMARY KEY (`idWord`,`dataset`) 
)
COMMENT "Columns: (`idWord`, `wordValue`)"
ENGINE = InnoDB
AUTO_INCREMENT = 10
DEFAULT CHARACTER SET = latin1;


-- ---------------------------------------------------------------------
--  Create table `db_presentation`.`tblDocumentLibrary` with columns:
--    uuidDocument:  unique identifier for every document  # primary key
--    idDocument: per-dataset unique identifier for each document
--    docDataset: identifier for dataset this doc comes from
--    docSource: textual pointer to document source
--    docDate: date of document retrieval
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_presentation`.`tblDocumentLibrary` ( 
  `uuidDocument` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 
    "unique identifier for every document (primary key)" , 
  `idDocument` INT(10) UNSIGNED NOT NULL COMMENT 
    "per-dataset unique identifier for each document" , 
  `docDataset` INT(10) UNSIGNED NOT NULL COMMENT 
    "identifier for dataset this doc comes from" , 
  `docSource` VARCHAR(100) CHARACTER SET 'utf8' NOT NULL COMMENT 
    "textual pointer to document source" , 
  `docDate` VARCHAR(100) CHARACTER SET 'utf8' COMMENT 
    "date of document retrieval" , 
  PRIMARY KEY (`uuidDocument`) 
)
COMMENT "Columns: (`uuidDocument`, `idDocument`, `docDataset`, 
`docSource`, `docDate`)"
ENGINE = InnoDB
AUTO_INCREMENT = 10
DEFAULT CHARACTER SET = latin1;


-- ---------------------------------------------------------------------
--  Create table `db_presentation`.`tblWordFrequencies` with columns:
--    uuidDocument: unique identifier for every document   # foreign key
--    idWord: globally unique identifier for every word    # foreign key
--    wordFreq: frequency that word appears in this doc
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_presentation`.`tblWordFrequencies` ( 
  `uuidDocument` INT(10) UNSIGNED NOT NULL COMMENT 
    "unique identifier for every document (foreign key)" , 
  `idWord` INT(10) UNSIGNED NOT NULL COMMENT 
    "globally unique identifier for every word (foreign key)" , 
  `wordFreq` INT(10) UNSIGNED NOT NULL COMMENT 
    "frequency that word appears in this doc" , 
  CONSTRAINT `fk_WordFreqDocID` FOREIGN KEY (`uuidDocument`) 
    REFERENCES `db_presentation`.`tblDocumentLibrary`(`uuidDocument`) 
    ON DELETE CASCADE ON UPDATE CASCADE , 
  CONSTRAINT `fk_WordFreqWordID` FOREIGN KEY (`idWord`) 
    REFERENCES `db_presentation`.`tblWordLibrary`(`idWord`) 
    ON DELETE CASCADE ON UPDATE CASCADE 
)
COMMENT "Columns: (`uuidDocument`, `idWord`, `wordFreq`)"
ENGINE = InnoDB
AUTO_INCREMENT = 10
DEFAULT CHARACTER SET = latin1;


-- ---------------------------------------------------------------------
--  Create table `db_presentation`.`tblClusterLibrary` with columns:
--    APLevel: hierarchical level of this document         # primary key
--    idCluster: reference to exemplar (a document)        # foreign key
--    uuidDocument: unique identifier for every document   # foreign key
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_presentation`.`tblClusterLibrary` ( 
  `APLevel` INT(10) UNSIGNED NOT NULL COMMENT 
    "hierarchical level of this document (primary key)" , 
  `idCluster` INT(10) UNSIGNED NOT NULL COMMENT 
    "reference to exemplar (a document) (foreign key)" , 
  `uuidDocument` INT(10) UNSIGNED NOT NULL COMMENT 
    "unique identifier for every document (foreign key)" , 
--  PRIMARY KEY (`APLevel`) , 
  CONSTRAINT `fk_ClusterLibClusterID` FOREIGN KEY (`idCluster`) 
    REFERENCES `db_presentation`.`tblDocumentLibrary`(`uuidDocument`)
    ON DELETE CASCADE ON UPDATE CASCADE , 
  CONSTRAINT `fk_ClusterLibDocumentID` FOREIGN KEY (`uuidDocument`) 
    REFERENCES `db_presentation`.`tblDocumentLibrary`(`uuidDocument`)
    ON DELETE CASCADE ON UPDATE CASCADE 
)
COMMENT "Columns: (`APLevel`, `idCluster`, `uuidDocument`)"
ENGINE = InnoDB
AUTO_INCREMENT = 10
DEFAULT CHARACTER SET = latin1;


-- ---------------------------------------------------------------------
--  Create table `db_presentation`.`tblClusterSize` with columns:
--    idCluster: reference to exemplar (a document)        # foreign key
--    APLevel: hierarchical level of this document         # foreign key
--    sizeCluster: count elements represented by exemplar   (incl. self)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_presentation`.`tblClusterSize` ( 
  `idCluster` INT(10) UNSIGNED NOT NULL COMMENT 
    "reference to exemplar (a document) (foreign key)" , 
  `APLevel` INT(10) UNSIGNED NOT NULL COMMENT 
    "hierarchical level of this document (foreign key)" , 
  `sizeCluster` INT(10) UNSIGNED NOT NULL COMMENT 
    "count elements represented by exemplar" , 
  CONSTRAINT `fk_ClusterSizeClusterID` FOREIGN KEY (`idCluster`) 
    REFERENCES `db_presentation`.`tblDocumentLibrary`(`uuidDocument`) 
    ON DELETE CASCADE ON UPDATE CASCADE 
--  CONSTRAINT `fk_ClusterSizeLevelID` FOREIGN KEY (`APLevel`) 
--    REFERENCES `db_presentation`.`tblClusterLibrary`(`APLevel`)
--    ON DELETE CASCADE ON UPDATE CASCADE 
)
COMMENT "Columns: (`idCluster`, `APLevel`, `sizeCluster`)"
ENGINE = InnoDB
AUTO_INCREMENT = 10
DEFAULT CHARACTER SET = latin1;

