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



--  CREATE PROCEDURES 
--  Our schema name is `db_presentation`

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL';

CREATE SCHEMA IF NOT EXISTS `db_presentation` DEFAULT CHARACTER SET latin1 ;
USE `db_presentation` ;


-- ---------------------------------------------------------------------
--  Procedure `ins_word`
--    This procedure inserts a new Word entry in the `tblWordLibrary`
--      table.
-- 
--  INput:
--    _idWord     INT(10)
--      globally unique identifier for every word
--    _dataset    INT(10)
--      identifier for dataset this word comes from
--    _wordValue  VARCHAR(100)
--      textual value of this word (the word itself)
--
--  OUTput:
--    _rowsUpdated     INT(10)
--      number of rows updated by this procedure
-- ---------------------------------------------------------------------
DROP procedure IF EXISTS `db_presentation`.`ins_word`;
DELIMITER $$

CREATE DEFINER=`amalthea`@`%` PROCEDURE `ins_word`(
  -- define all inputs
  IN _idWord INT(10),
  IN _dataset INT(10),
  IN _wordValue VARCHAR(100),
  -- define all outputs
  OUT _rowsUpdated INT(10)
)
BEGIN
  INSERT INTO `db_presentation`.`tblWordLibrary` ( 
    `idWord`,
    `dataset`,
    `wordValue`
  ) VALUES ( 
    _idWord,
    _dataset,
    _wordValue
  );
  
  SET _rowsUpdated = row_count();
END$$

DELIMITER ;


-- ---------------------------------------------------------------------
--  Procedure `ins_document`
--    This procedure inserts a new Document entry in the
--      `tblDocumentLibrary` table.
-- 
--  INput:
--    _idDocument   INT(10)
--      per-dataset unique identifier for each document
--    _docDataset   INT(10)
--      identifier for dataset this doc comes from
--    _docSource    VARCHAR(100)
--      textual pointer to document source
--    _docDate      VARCHAR(100)
--      date of document retrieval
--
--  OUTput:
--    _rowsUpdated     INT(10)
--      number of rows updated by this procedure
-- ---------------------------------------------------------------------
DROP procedure IF EXISTS `db_presentation`.`ins_document`;
DELIMITER $$

CREATE DEFINER=`amalthea`@`%` PROCEDURE `ins_document`(
  -- define all inputs
  IN _idDocument INT(10),
  IN _docDataset INT(10),
  IN _docSource VARCHAR(100),
  IN _docDate VARCHAR(100),
  -- define all outputs
  OUT _rowsUpdated INT(10)
)
BEGIN
  INSERT INTO `db_presentation`.`tblDocumentLibrary` ( 
    `uuidDocument`,
    `idDocument`,
    `docDataset`,
    `docSource`,
    `docDate`
  ) VALUES ( 
    NULL,
    _idDocument,
    _docDataset,
    _docSource,
    _docDate
  );
  
  SET _rowsUpdated = row_count();
END$$

DELIMITER ;


-- ---------------------------------------------------------------------
--  Procedure `ins_wordFreq`
--    This procedure inserts a new Word Frequency entry into the 
--      `tblWordFrequencies` table.
-- 
--  INput:
--    _uuidDocument   INT(10)
--      unique identifier for every document
--    _idWord         INT(10)
--      globally unique identifier for every word
--    _wordFreq       INT(10)
--      frequency that word appears in this doc
--
--  OUTput:
--    _rowsUpdated     INT(10)
--      number of rows updated by this procedure
-- ---------------------------------------------------------------------
DROP procedure IF EXISTS `db_presentation`.`ins_wordFreq`;
DELIMITER $$

CREATE DEFINER=`amalthea`@`%` PROCEDURE `ins_wordFreq`(
  -- define all inputs
  IN _uuidDocument INT(10),
  IN _idWord INT(10),
  IN _wordFreq INT(10),
  -- define all outputs
  OUT _rowsUpdated INT(10)
)
BEGIN
  INSERT INTO `db_presentation`.`tblWordFrequencies` ( 
    `uuidDocument`,
    `idWord`,
    `wordFreq`
  ) VALUES ( 
    _uuidDocument,
    _idWord,
    _wordFreq
  );
  
  SET _rowsUpdated = row_count();
END$$

DELIMITER ;


-- ---------------------------------------------------------------------
--  Procedure `ins_cluster`
--    This procedure inserts a new Cluster entry into the 
--      `tblClusterLibrary` table.
-- 
--  INput:
--    _APLevel        INT(10)
--      hierarchical level of this document
--    _idCluster      INT(10)
--      reference to exemplar (a document)
--    _uuidDocument   INT(10)
--      unique identifier for every document
--
--  OUTput:
--    _rowsUpdated     INT(10)
--      number of rows updated by this procedure
-- ---------------------------------------------------------------------
DROP procedure IF EXISTS `db_presentation`.`ins_cluster`;
DELIMITER $$

CREATE DEFINER=`amalthea`@`%` PROCEDURE `ins_cluster`(
  -- define all inputs
  IN _APLevel INT(10),
  IN _idCluster INT(10),
  IN _uuidDocument INT(10),
  -- define all outputs
  OUT _rowsUpdated INT(10)
)
BEGIN
  INSERT INTO `db_presentation`.`tblClusterLibrary` ( 
    `APLevel`,
    `idCluster`,
    `uuidDocument`
  ) VALUES ( 
    _APLevel,
    _idCluster,
    _uuidDocument
  );
  
  SET _rowsUpdated = row_count();
END$$

DELIMITER ;


-- ---------------------------------------------------------------------
--  Procedure `ins_clusterSize`
--    This procedure inserts a new Cluster Size entry into the 
--      `tblClusterSize` table.
-- 
--  INput:
--    _idCluster      INT(10)
--      reference to exemplar (a document)
--    _APLevel        INT(10)
--      hierarchical level of this document
--    _sizeCluster    INT(10)
--      count elements represented by exemplar
--
--  OUTput:
--    _rowsUpdated     INT(10)
--      number of rows updated by this procedure
-- ---------------------------------------------------------------------
DROP procedure IF EXISTS `db_presentation`.`ins_clusterSize`;
DELIMITER $$

CREATE DEFINER=`amalthea`@`%` PROCEDURE `ins_clusterSize`(
  -- define all inputs
  IN _idCluster INT(10),
  IN _APLevel INT(10),
  IN _sizeCluster INT(10),
  -- define all outputs
  OUT _rowsUpdated INT(10)
)
BEGIN
  INSERT INTO `db_presentation`.`tblClusterSize` ( 
    `idCluster`,
    `APLevel`,
    `sizeCluster`
  ) VALUES ( 
    _idCluster,
    _APLevel,
    _sizeCluster
  );
  
  SET _rowsUpdated = row_count();
END$$

DELIMITER ;

