# Stored-Procedure
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRL_CREATE_OUT_VIEWS
-- =============================================
-- Description: This procedure will create custom field enhancement view
-- Change log
--      [2016 04 07]: Initial version 
-- =============================================
-- Stored Procedure Parameters

(
IN isrc_tblnm VARCHAR(40),
IN iExec_Mode CHAR(1),
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000),
OUT oSQL_Text VARCHAR(50000)
) 
 MAIN:
BEGIN
-- Declare variables
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE vSQL_Text VARCHAR(10000);
DECLARE vSQL_ONText VARCHAR(10000);
DECLARE EOLStr VARCHAR(2) ;
DECLARE oSubReturn_Code INTEGER;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vLoopCount INTEGER;
DECLARE vComma CHAR(1);
DECLARE vSRC_DB_NM VARCHAR(40);
DECLARE vTRG_DB_NM VARCHAR(40);
DECLARE vSRC_DB VARCHAR(40);
DECLARE VSRC_TBNM VARCHAR(40);
DECLARE VTGT_TBNM VARCHAR(40);

--DECLARE return_code_create SMALLINT;

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    SET oReturn_Code = 2;
    SET oReturn_Message = ' Call to SP failed. SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 0, 'Started - isrc_tblnm '||isrc_tblnm, 0, oSubReturn_Code, oSubReturn_Message);

SET EOLStr    = '
';

SET vSQL_Text = '';
SET vSQL_ONText = '';
SET vLoopCount = 0;

SET vSRC_TBNM = UPPER(isrc_tblnm);
SET vTGT_TBNM = 'OUT_'||vSRC_TBNM;


-- Fetch Source DB Name
SELECT PRMTR_VAL INTO :vSRC_DB_NM FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S  WHERE PRMTR_NM = 'SRC_CNSUM';

-- Fetch Target DB Name
SELECT TRIM(PRMTR_VAL) INTO :vTRG_DB_NM FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S WHERE PRMTR_NM = 'GEN_DB_NM' ;



CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 100, 'Source DB Name =' || vSRC_DB_NM||'Target DB Name =  '|| vTRG_DB_NM||' Source Table Name = '|| vSRC_TBNM||' Target Table Name = ', 2, oSubReturn_Code, oSubReturn_Message);

SET vSQL_Text = 'REPLACE VIEW '||vTRG_DB_NM||'.'||vTGT_TBNM||' AS LOCKING ROW FOR ACCESS'||EOLStr||'SELECT';

L1: 
FOR    CSR1 AS
---Cursor to select the columns.

SELECT TRIM(COLUMNNAME) AS COLNM
FROM DBC.COLUMNS 
WHERE Databasename = vSRC_DB_NM 
AND TRIM(TABLENAME) = vSRC_TBNM
AND COLNM NOT LIKE '%CSTM%FLD%'
ORDER BY 1



DO
----------

SET vLoopCount = vLoopCount + 1;

IF vLoopCount = 1 THEN
    SET vComma = '';
ELSE 
    SET vComma = ',';
END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 200, 'In looop L1 CSR1.COLUMNNAME '|| CSR1.COLNM,3, oSubReturn_Code, oSubReturn_Message);

--SET vSQL_Text = vSQL_Text || EOLStr||CSR1.COLNM||vComma;
SET vSQL_Text = vSQL_Text || EOLStr||vComma||CSR1.COLNM;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 300, 'vSQL_Text '|| vSQL_Text, 3, oSubReturn_Code, oSubReturn_Message);    

----------
END FOR    L1;

L2: 
FOR    CSR2 AS
-- CURSOR TO SELECT THE CASE STATEMENTS

SELECT
GEN_COL
FROM
(SELECT
Col_Map_Tgt_Col,
OREPLACE (Col_Map_Tgt_Col,'_MDID',' ') AS Col_Map_Tgt_Col_1,
OTRANSLATE (Col_Map_Fmt,'!@#$/%^&*().-',' ') X, -- to remove the junk characters 
OREPLACE (X,' ','_') COL_MAP_VAL,
    'CASE  WHEN ' AS A,
    Col_Map_Tgt_Col AS B,
    '= ' AS C,
    Col_Map_Fmt AS D,
    ' THEN ' AS E,
    Col_Map_Tgt_Col_1 AS F,
    ' ELSE  NULL  END' AS G,
    ' AS ' AS H,
    COL_MAP_VAL AS I,
    '('||A||B||C||''''||D||''''||E||F||G||')'||H||I AS GEN_COL
	FROM
(SELECT
	DISTINCT Col_Map_Fmt,
    Col_Map_Tgt_Col  
    FROM QSIT_APRA2_BRL_RRP_VW.CLMD_COLUMN_MAPPING 
    WHERE COL_MAP_TYPE = 'STATICVAL'
    AND Col_Map_Tgt_Col LIKE '%CSTM%FLD%MDID%'
	AND Col_Map_Fmt <> ' ' 
	AND Col_Map_Tgt_Tbl = isrc_tblnm
	) Tab) Tab1
ORDER BY COL_MAP_VAL
DO
----------

SET vLoopCount = vLoopCount + 1;

IF vLoopCount = 1 THEN
    SET vComma = '';
ELSE 
    SET vComma = ',';
END IF;

--CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 200, 'In looop L2 CSR2.COLUMNNAME '|| CSR2.GEN_COL,3, oSubReturn_Code, oSubReturn_Message);

--SET vSQL_Text = vSQL_Text || EOLStr||CSR2.GEN_COL||vComma;
SET vSQL_Text = vSQL_Text || EOLStr||vComma||CSR2.GEN_COL;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 400, 'vSQL_Text '|| vSQL_Text, 3, oSubReturn_Code, oSubReturn_Message);    

----------
END FOR    L2;

SET vSQL_Text = vSQL_Text||EOLStr||'FROM '||vSRC_DB_NM||'.'||vSRC_TBNM||EOLStr||';';
SET oSQL_Text = vSQL_Text;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 500, 'vSQL_Text '|| vSQL_Text, 3, oSubReturn_Code, oSubReturn_Message);    

SET oReturn_Code = 0;
SET oReturn_Message = 'Successfully Completed ';

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BRL_CREATE_OUT_VIEWS', 600, 'Ended SP - isrc_tblnm '||vSRC_TBNM || 'oSQL_Text:'||oSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

IF (oSubReturn_Code =0)
THEN

---To run the SQL generated.	
	IF iExec_Mode = 'Y' THEN 
			EXECUTE IMMEDIATE vSQL_Text;
			SET oReturn_Code = 0;
			SET oReturn_Message='Success - Executed the generated SQL. Please verify that View has been created.';
			LEAVE MAIN;
	END IF;
	SET oReturn_Code = 0;
	SET oReturn_Message='Success - Did not execute the generated SQL';
	ELSE
	SET oReturn_Code = 5;
	SET oReturn_Message= 'SELECT Portion: ' || oSubReturn_Message;
    END IF; 

END MAIN;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP
-- =============================================
-- Description: This procedure populate the Cascading Prompts Mapping tables
-- Change log
--      [2016 06 23]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iExecution_Flag            CHAR(1),
--
-- Possible Values
--   Y  Execute Transform View SQL. Capture SQL in V_SQL_Log.
--   N  Do not Execute Transform View SQL. Capture SQL in V_SQL_Log.
--
--  Business Date is retrieved from vBRLVWDBName.T_APRA_RPT_PRD
--
 OUT oReturn_Code              SMALLINT,             /* 0: Successful; 1: Error */
 OUT oReturn_Message           VARCHAR(1000)
--
-- Run Time Format:
--
-- Example CALL PROCEDURE syntax
--      Example 1. Execution Flag = Y
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP ('Y', oReturn, oReturn_Message);
--      Example 2. Execution Flag = N
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP ('N', oReturn, oReturn_Message);
--
)
MAIN:
BEGIN
--
---------------------------------------------------------------------------------------------
-- Variables declaration
---------------------------------------------------------------------------------------------       
--
-- Declare Constants
--
DECLARE vBRLStoredProcDBName        VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_PGM';
DECLARE vBRLWorkDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_WK';
DECLARE vBRLVWDBName                VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_VW';
DECLARE vBRLGENVWDBName             VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_GEN_VW';
DECLARE vBRLTDBName                 VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_T';
DECLARE vCNSUMVWDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_VW';
DECLARE vCNSUMTDBName               VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_T';
DECLARE vStoredProcName             VARCHAR(128) DEFAULT 'BRL_CSCDNG_DFLTS_MAP';
DECLARE cLF                         CHAR(2) DEFAULT '0A'XC;
--
--
-- Declare variables
--
DECLARE vStepNbr                    SMALLINT DEFAULT 0;
DECLARE vSQLStep                    INTEGER DEFAULT 1;
DECLARE vExecute_Flag               CHAR(1) DEFAULT 'N';
DECLARE vBssns_Date		            VARCHAR(10);
DECLARE vSQL_Text                   VARCHAR(16384) DEFAULT '';
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);
--
-- Declare Error Handler variables
--
DECLARE vSQL_Code                   INTEGER;
DECLARE vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE vError_Text                 VARCHAR(256);
--
-- Error Handler
--
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
--
-- Preserve Diagnostic Codes from errored Statement
--
		SET vSQL_Code  = SQLCODE;
		SET vSQL_State = SQLSTATE;
		GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
		SET oReturn_Code = 1;
		SET oReturn_Message = 'Stored Procedure = '||vStoredProcName||', Step Nbr = '||TRIM(vStepNbr)||cLF||'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	END
	;
--
-- Validate Input Parameters
--
    IF iExecution_Flag NOT IN ('Y', 'N') THEN
        SET vExecute_Flag = 'N';
    ELSE
        SET vExecute_Flag = iExecution_Flag;
    END IF;
--
-- Get Business Date
--  Get latest END_TS just in case
--
    SET vStepNbr = 1;
    BEGIN
        DECLARE C1 CURSOR FOR S1;
        SET vSQL_Text = 'SELECT CAST((END_TS (FORMAT ''YYYY-MM-DD'')) AS CHAR(10)) FROM ' || vBRLVWDBName || '.T_APRA_RPT_PRD QUALIFY ROW_NUMBER() OVER (ORDER BY END_TS DESC) = 1';
        PREPARE S1 FROM vSQL_Text;
        OPEN C1;
        FETCH C1 INTO vBssns_Date;
        CLOSE C1;
    END
    ;
--
-- Truncate the V_SQL_Log tables in BRL for the Business Date
--
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.V_SQL_Log WHERE SP_Name = ''' || vStoredProcName || ''' AND Bssns_Date = DATE ''' || vBssns_Date || ''';';
    EXECUTE IMMEDIATE vSQL_Text;
--
-- Populate the L0624 Entity tree BRL work table
--
    SET vStepNbr = 10;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_L0624_ENTITY;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_ENTITY', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_L0624_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_LEAF_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ROOT_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_IMM_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ENTITY_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || '  AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LABEL AS HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_IMM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DESCRIPTIONS AS HFM_ENTITY_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CASE WHEN HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,0 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vCNSUMVWDBName ||'.SR_ENTITY_TREE'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE DEFAULT_PARENT = ''L0624'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.HFM_ROOT_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DESCRIPTIONS'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM.HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ENTITIES A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN ' || vCNSUMVWDBName ||'.SR_ENTITY_TREE HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.HFM_LEAF_ENTITY = HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25 AND A.IS_LEAF_FLAG = ''N'''||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LEAF_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ROOT_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_IMM_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ENTITY_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '    IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '    DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_ENTITY', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the L0624 Entity leaf nodes BRL work table
--
    SET vStepNbr = 11;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_L0624_LEAFS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_L0624_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || '( LCD_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_LEGAL_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM_LEAF_ENTITY AS LCD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_IMM_PARENT_ENTITY AS NODE_LEGAL_ENT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_ENTITY_DESC AS LCD_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,IS_LEAF_FLAG AS IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,1 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM '|| vBRLWorkDBName || '.SR_L0624_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     A.LCD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.HFM_IMM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.LCD_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ENTITIES A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN '|| vBRLWorkDBName || '.SR_L0624_ENTITY HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.NODE_LEGAL_ENT_CD = HFM.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT '||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_CD, '||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_LEGAL_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Balance Sheet Account tree BRL work table
--
    SET vStepNbr = 12;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_ACCNT', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_LEAF_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ROOT_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_IMM_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ACCOUNT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     LABEL AS HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_IMM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DESCRIPTIONS AS HFM_ACCOUNT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,0 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE DEFAULT_PARENT = ''BALSHEET'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.HFM_ROOT_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DESCRIPTIONS'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM.HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ACCOUNTS A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.HFM_LEAF_ACCOUNT = HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE A.DEPTH <= 25 AND A.IS_LEAF_FLAG = ''N'''||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LEAF_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ROOT_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_IMM_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ACCOUNT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '    IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '    DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_ACCNT', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Balance Sheet Account leaf nodes BRL work table
-- Includes a row for each leaf mapped to itself with at depth of 0
--
    SET vStepNbr = 13;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM_LEAF_ACCOUNT AS LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_IMM_PARENT_ACCOUNT AS NODE_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_ACCOUNT_DESC AS LEAF_ACCT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,IS_LEAF_FLAG AS IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,1 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     A.LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.HFM_IMM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.LEAF_ACCT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ACCOUNTS A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.NODE_ACCT_CD = HFM.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;
  
    SET vStepNbr = 14;
    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  0'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE DEPTH = 1'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR GL Balance table in BRL layer
--
    SET vStepNbr = 20;
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.INFRC_SR_GL_BAL;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.INFRC_SR_GL_BAL'||cLF;
    SET vSQL_Text = vSQL_Text || '( ENT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BSBCC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ''L0624'' AS ENT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BUSINESS_UNIT AS PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.ACCOUNT_1 AS PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.PRODUCT AS PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.AFFILIATE AS PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.DEPTID AS PSGL_BSBCC'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.PROJECT_ID AS PSGL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.CURRENCY_CD AS TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_TRAN_TOTAL) AS TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BASE_CURRENCY AS LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_BASE_TOTAL) AS LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BASE_CURRENCY AS RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_BASE_TOTAL) AS RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.SR_GL_BAL gl'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_L0624_ENTITY ent'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON gl.HFM_LCD = ent.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_BALSHEET_ACCNT bsh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON gl.HFM_ACCT_CD = bsh.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE ent.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || '  AND bsh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14'||cLF;
    SET vSQL_Text = vSQL_Text || 'HAVING RPT_AMT <> 0'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR GL Account Association table in BRL layer
--
    SET vStepNbr = 21;
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_ACCT_ASSOC', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' ||vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || '   ENT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '   ''L0624'''||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM'||cLF;
    SET vSQL_Text = vSQL_Text || '  ('||cLF;
    SET vSQL_Text = vSQL_Text || '   SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     sbh.SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.USR_FLD_1 AS BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN sbh.VALTN_TY IS NULL OR sbh.VALTN_TY = '''' THEN ''Gross Balance'' ELSE sbh.VALTN_TY END AS VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,COALESCE(amap.SR_ACCT, ''Unknown HFM Account'') AS SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.DEPT_ID AS BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,COALESCE(lmap.SR_LCD, ''Unknown HFM LCD'') AS SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.GL_AC AS GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.GL_PRD AS GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.AFFIL AS GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.PROJ_ID AS GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.BSNS_UNT AS GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.CUR_CD AS LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || '   FROM ' || vCNSUMVWDBName || '.SRC_ACC_BAL_HIST sbh'||cLF;
    SET vSQL_Text = vSQL_Text || '   LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP amap'||cLF;
    SET vSQL_Text = vSQL_Text || '   ON sbh.GL_AC = amap.GL_ACCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_DEPT_LCD_MAP lmap'||cLF;
    SET vSQL_Text = vSQL_Text || '   ON sbh.BSNS_UNT = lmap.GL_BSSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || '   AND sbh.DEPT_ID = lmap.GL_DEPTID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ) DT'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_L0624_ENTITY ent'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DT.SR_LCD = ent.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_BALSHEET_ACCNT bsh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DT.SR_ACCT_CD = bsh.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE ent.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || '  AND bsh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_ACCT_ASSOC', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR Balance table in BRL layer
--
    SET vStepNbr = 22;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.INFRC_SR_BAL;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLVWDBName || '.INFRC_SR_BAL'||cLF;
    SET vSQL_Text = vSQL_Text || '(  ENT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_YEAR_1'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_MONTH_1'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     ''L0624''                        -- Business Rules to be determined'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.Year_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.Month_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.ACCOUNT_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,NULL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,NULL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     = MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END) = ''AUD'''||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     ELSE MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,SUM((CASE WHEN UPPER(act.ACCOUNTTYPE) IN (''ASSET'', ''EXPENSE'') THEN 1000 ELSE -1000 END) * CAST(tcl.Amount AS DECIMAL(36,8)))'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     = MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END) = ''AUD'''||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     ELSE MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,SUM((CASE WHEN tcl.Currency IN (''AUD'', ''AUD Adjs'') THEN (CASE WHEN act.ACCOUNTTYPE IN (''ASSET'', ''EXPENSE'') THEN 1000 ELSE -1000 END) ELSE 0 END) * CAST(tcl.Amount AS DECIMAL(36,8)))'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.HFMZZ_EA_TCL tcl'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE act'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ACCOUNT_1 = act.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_BALSHEET_ACCNT acth'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ACCOUNT_1 = acth.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_L0624_ENTITY lcdh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ENTITY = lcdh.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_CUSTOM1_HIER C1'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.Custom1 = C1.LEAF_CUSTOM1'||cLF;
    SET vSQL_Text = vSQL_Text || 'OR tcl.Custom1 = ''STAT_ADJ'''||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_CUSTOM3_HIER C3'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.Custom3 = C3.LEAF_CUSTOM3'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE tcl.Scenario = ''ACTUAL'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom2 = ''EOP'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom4 = ''[None]'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Periodicity = ''YTD'''||cLF;
    SET vSQL_Text = vSQL_Text || '-- AND tcl.Currency IN (''AUD'', ''AUD Adjs'')'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND acth.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND lcdh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C1.NODE_CUSTOM1 = ''MGMT'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom1 <> ''MGMT_ADJ'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C3.NODE_CUSTOM3 = ''TB_TOT'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Year_1 = (SELECT CASE WHEN month_of_year >= 10 THEN year_of_calendar+1 ELSE year_of_calendar END FROM Sys_Calendar.CALENDAR WHERE calendar_date = DATE ''' || vBssns_Date || ''')'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Month_1 = (SELECT b.SR_MTH FROM Sys_Calendar.CALENDAR a INNER JOIN ' || vCNSUMVWDBName || '.SR_MTH b ON a.month_of_year = b.MTH WHERE calendar_date = DATE ''' || vBssns_Date || ''')'||cLF;
    SET vSQL_Text = vSQL_Text || 'GROUP BY 1,2,3,4,5'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the HFM PSGL EXPLO Ledger work table in BRL Layer
--
    SET vStepNbr = 23;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_EXPLO_SUBLEDGER', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUSINESS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE)'||cLF;
-- PSGL Entries from INFRC_SR_GL_BAL
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_GL_BAL'||cLF;
-- HFM Entries from INFRC_SR_BAL
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_BAL'||cLF;
-- EXPLO Entries from INFRC_SR_GL_ACCT_ASSOC
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_EXPLO_SUBLEDGER', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Start of the Cascading Defaults process
--
-- Populate the HFM Leaf Defaults work table in BRL Layer
--
    SET vStepNbr = 24;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_LEAF_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DF_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION '||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ( SELECT ROW_NUMBER() OVER (PARTITION BY B.LEAF_ACCT_CD ORDER BY CASE WHEN B.LEAF_ACCT_CD = A.HFM_ACCOUNT_CODE THEN 1 ELSE 9 END, B.DEPTH) AS ROW_NR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              B.LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BAL_CLASS,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_DF_PRODUCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.REGULATORY_PRODUCT_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BSS_ITEM_CDE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BSS_ITEM_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_DC_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SECURITISATION,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_RESECURITISATION,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_ASSET_BACKED,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.PRD_REPO_PRODUCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SETT_CREDIT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_CONVERTIBLE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_TANGIBLE_ASSETS,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_BS_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_NEAR_FAR_INDIC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_ENTRY_TYPE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_PURPOSE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CTY_RESIDENCE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_REVOLVING_FAC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_CPTY_TYPE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CDE_CONSOLIDATION_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_COLLATERAL,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_PORTFOLIO,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_INTEREST_RATE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CCY_ISO_CURRENCY,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_REMAINING_BKT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_ORIGINAL_BKT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CTY_ISSUE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_LISTED,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SUBORDINATION'||cLF;
    SET vSQL_Text = vSQL_Text || '      FROM ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS A'||cLF;
    SET vSQL_Text = vSQL_Text || '      INNER JOIN '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS B'||cLF;
    SET vSQL_Text = vSQL_Text || '      ON A.HFM_ACCOUNT_CODE = B.NODE_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '      WHERE A.PSGL_ACCOUNT_CODE = '''''||cLF;
    SET vSQL_Text = vSQL_Text || '     ) C'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE C.ROW_NR = 1;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the PSGL Leaf Defaults work table in BRL Layer
--
    SET vStepNbr = 25;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT A.SR_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       A.GL_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       CAST(NULL AS VARCHAR(50)),'||cLF;
    SET vSQL_Text = vSQL_Text || '       CAST(NULL AS VARCHAR(50)),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BAL_CLASS, B.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DF_PRODUCT, B.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.REGULATORY_PRODUCT_CODE, B.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_CDE, B.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_DESC, B.BSS_ITEM_DESC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DC_INDICATOR, B.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SECURITISATION, B.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_RESECURITISATION, B.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ASSET_BACKED, B.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.PRD_REPO_PRODUCT, B.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SETT_CREDIT, B.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CONVERTIBLE, B.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_TANGIBLE_ASSETS, B.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_BS_INDICATOR, B.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_INDICATOR, B.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_NEAR_FAR_INDIC, B.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ENTRY_TYPE, B.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PURPOSE, B.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_RESIDENCE, B.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_REVOLVING_FAC, B.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CPTY_TYPE, B.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CDE_CONSOLIDATION_CODE, B.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_COLLATERAL, B.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PORTFOLIO, B.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_INTEREST_RATE, B.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CCY_ISO_CURRENCY, B.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_REMAINING_BKT, B.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_ORIGINAL_BKT, B.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_ISSUE, B.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_LISTED, B.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SUBORDINATION, B.TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP A'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.SR_ACCT = B.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.GL_ACCT = C.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C.PSGL_PRODUCT_CODE = '''''||cLF;
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT A.SR_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       A.GL_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       C.PSGL_PRODUCT_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       C.PSGL_AFFILIATE_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BAL_CLASS, B.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DF_PRODUCT, B.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.REGULATORY_PRODUCT_CODE, B.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_CDE, B.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_DESC, B.BSS_ITEM_DESC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DC_INDICATOR, B.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SECURITISATION, B.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_RESECURITISATION, B.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ASSET_BACKED, B.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.PRD_REPO_PRODUCT, B.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SETT_CREDIT, B.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CONVERTIBLE, B.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_TANGIBLE_ASSETS, B.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_BS_INDICATOR, B.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_INDICATOR, B.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_NEAR_FAR_INDIC, B.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ENTRY_TYPE, B.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PURPOSE, B.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_RESIDENCE, B.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_REVOLVING_FAC, B.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CPTY_TYPE, B.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CDE_CONSOLIDATION_CODE, B.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_COLLATERAL, B.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PORTFOLIO, B.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_INTEREST_RATE, B.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CCY_ISO_CURRENCY, B.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_REMAINING_BKT, B.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_ORIGINAL_BKT, B.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_ISSUE, B.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_LISTED, B.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SUBORDINATION, B.TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP A'||cLF;
    SET vSQL_Text = vSQL_Text || 'JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.SR_ACCT = B.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.GL_ACCT = C.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE C.PSGL_PRODUCT_CODE <> '''';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface HFM PSGL Map Defaults table in BRL layer
--   Truncate records for current Business Date
--
    SET vStepNbr = 26;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE BSNS_DT = DATE ''' || vBssns_Date || ''';';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( BSNS_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRD_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_DC_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SCRTZN'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RESCRTIZ'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ASSET_BKCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_REPO_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SETL_CR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CONV'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_TNGBL_ASSET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_SHEET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_MTRTY_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_NEAR_FAR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ENTRY_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRPS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RES_CNRTY_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RVLV_FCLT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CNTRPRT_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CNSLDT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_COLLAT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PORTF'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INT_RTE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CUR_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_RMN_BCKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORGNL_BCKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,ISS_CNTRY_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_LSTD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INRFC_SUBB'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EFFV_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EXPR_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,USR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DATE ''' || vBssns_Date || ''','||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.HFM_ACCOUNT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_ACCOUNT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_PRODUCT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_AFFILIATE_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_BUSINESS_UNIT, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.AMOUNT_FIELD, PSGL.AMOUNT_FIELD, HFM.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.PRODUCT_TYPE, PSGL.PRODUCT_TYPE, HFM.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.REGULATORY_PRODUCT_CODE, PSGL.REGULATORY_PRODUCT_CODE, HFM.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_DC_INDICATOR, PSGL.TYP_DC_INDICATOR, HFM.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SECURITISATION, PSGL.TYP_SECURITISATION, HFM.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_RESECURITISATION, PSGL.TYP_RESECURITISATION, HFM.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_ASSET_BACKED, PSGL.TYP_ASSET_BACKED, HFM.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.PRD_REPO_PRODUCT, PSGL.PRD_REPO_PRODUCT, HFM.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SETT_CREDIT, PSGL.TYP_SETT_CREDIT, HFM.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_CONVERTIBLE, PSGL.TYP_CONVERTIBLE, HFM.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_TANGIBLE_ASSETS, PSGL.TYP_TANGIBLE_ASSETS, HFM.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_BS_INDICATOR, PSGL.TYP_BS_INDICATOR, HFM.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_INDICATOR, PSGL.MAT_INDICATOR, HFM.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_NEAR_FAR_INDIC, PSGL.TYP_NEAR_FAR_INDIC, HFM.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_ENTRY_TYPE, PSGL.TYP_ENTRY_TYPE, HFM.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_PURPOSE, PSGL.TYP_PURPOSE, HFM.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.CTY_RESIDENCE, PRD.CTY_RESIDENCE, PSGL.CTY_RESIDENCE, HFM.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_REVOLVING_FAC, PSGL.TYP_REVOLVING_FAC, HFM.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.TYP_CPTY_TYPE, PRD.TYP_CPTY_TYPE, PSGL.TYP_CPTY_TYPE, HFM.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.CDE_CONSOLIDATION_CODE, PRD.CDE_CONSOLIDATION_CODE, PSGL.CDE_CONSOLIDATION_CODE, HFM.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_COLLATERAL, PSGL.TYP_COLLATERAL, HFM.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_PORTFOLIO, PSGL.TYP_PORTFOLIO, HFM.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_INTEREST_RATE, PSGL.TYP_INTEREST_RATE, HFM.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.CCY_ISO_CURRENCY, PSGL.CCY_ISO_CURRENCY, HFM.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_REMAINING_BKT, PSGL.MAT_REMAINING_BKT, HFM.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_ORIGINAL_BKT, PSGL.MAT_ORIGINAL_BKT, HFM.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.CTY_ISSUE, PSGL.CTY_ISSUE, HFM.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_LISTED, PSGL.TYP_LISTED, HFM.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SUBORDINATION, PSGL.TYP_SUBORDINATION, HFM.TYP_SUBORDINATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       CURRENT_DATE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       DATE ''9999-12-31'','||cLF;
--    SET vSQL_Text = vSQL_Text || '       ''verweyt'','||cLF;
    SET vSQL_Text = vSQL_Text || '       CASE WHEN AFF.AFFILIATE_CODE IS NOT NULL THEN'||cLF;
    SET vSQL_Text = vSQL_Text || '           (CASE WHEN PRD.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL PROD AND AFFIL OVRD MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '                 WHEN PSGL.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL ACCT AND AFFIL OVRD MAP'' ELSE ''HFM MAP AND AFFIL OVRD MAP'' END)'||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN PRD.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL PROD MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN PSGL.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL ACCT MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN HFM.HFM_ACCOUNT_CODE IS NOT NULL THEN ''HFM MAP'' ELSE ''NOT MAPPED'' END,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.BSS_ITEM_CDE, PSGL.BSS_ITEM_CDE, HFM.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.BSS_ITEM_DESC, PSGL.BSS_ITEM_DESC, HFM.BSS_ITEM_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER DB'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS HFM'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = HFM.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = PRD.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_ACCOUNT_CODE = PRD.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_PRODUCT_CODE = PRD.PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_PRODUCT_CODE IS NOT NULL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS PSGL'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = PSGL.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_ACCOUNT_CODE = PSGL.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PSGL.PSGL_PRODUCT_CODE IS NULL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_PSGL_AFF_DEFAULTS AFF'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.PSGL_AFFILIATE_CODE = AFF.AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the HFM PSGL Deal Type Deal Sub Type combination work table in BRL layer
--
    SET vStepNbr = 27;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_PRD;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.SR_GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUSINESS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
--    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(SMAP.SR_ACCT, ''Unknown HFM Account''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_AC,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_PRD,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.AFFIL,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.BSNS_UNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       CASE AR.FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_loan'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_interest_bearing_accounts'' THEN ''DP'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_bond'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_equity'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_lease'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_money_market'' THEN ''CA'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_repo_style'' THEN ''RE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_future'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_option'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_credit_derivatives'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_forex'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_fra'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_futures'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_fx'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_ip'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_facility'' THEN ''LI'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_fixed_asset'' THEN ''AT'''||cLF;
    SET vSQL_Text = vSQL_Text || '          ELSE '''''||cLF;
    SET vSQL_Text = vSQL_Text || '       END AS Product_Type,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_SUB_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_47, '''') AS CHEQUE_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.INFRC_VALTN_TY AS VALTN_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.DEBIT_CREDIT_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_50, '''') AS AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLGENVWDBName || '.EFFV_OUT_AR_TO_GL_LNK LNK'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_ACCT_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ') AR'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = AR.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = AR.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.BAL_TY = AR.BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT OUTER JOIN ' || vBRLVWDBName || '.PIVOT_BRO_AR BRO'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = BRO.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = BRO.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP SMAP'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.GL_AC = SMAP.GL_ACCT;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Product Map Defaults work table in BRL Layer
--   First where the Debit Credit Indicator is not blank
--
    SET vStepNbr = 28;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND <> '''';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
--   Next where the Debit Credit Indicator is blank explode for 'C' and 'D' values
--
    SET vStepNbr = 29;
    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'CROSS JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT DEBIT_CREDIT_IND FROM (SELECT ''C'' AS DEBIT_CREDIT_IND) cd1 UNION SELECT DEBIT_CREDIT_IND FROM (SELECT ''D'' AS DEBIT_CREDIT_IND) cd2'||cLF;
    SET vSQL_Text = vSQL_Text || ') cjn_dc'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND = '''''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND NOT EXISTS'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  xx.DEAL_TYPE, xx.DEAL_SUBTYPE, xx.VALUATION_TYPE, xx.DEBIT_CREDIT_IND, xx.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS xx'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE xx.DEAL_TYPE = dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEAL_SUBTYPE = dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.VALUATION_TYPE = dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEBIT_CREDIT_IND = cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.CHEQUE_IND = dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.AT_CALL_IND = dflt.AT_CALL_IND);'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface HFM PSGL Map Defaults table in BRL layer
--   Truncate records for current Business Date
--
    SET vStepNbr = 30;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE BSNS_DT = DATE ''' || vBssns_Date || ''';';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( BSNS_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CHQ_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DR_CR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRD_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_DC_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SCRTZN'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RESCRTIZ'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ASSET_BKCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_REPO_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SETL_CR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CONV'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_TNGBL_ASSET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_SHEET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_MTRTY_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_NEAR_FAR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ENTRY_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EFFV_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EXPR_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,USR_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '    DATE ''' || vBssns_Date || ''''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.HFM_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_PRODUCT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_AFFILIATE_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_BUSINESS_UNIT, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_SUBTYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.CHEQUE_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEBIT_CREDIT_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.VALUATION_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.AT_CALL_INDICATOR, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.AMOUNT_FIELD, B.BAL_TY, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PRODUCT_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.REGULATORY_PRODUCT_CODE, B.REG_PRD_CD, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_CDE, B.REG_BSS_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_DESC, B.REG_BSS_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_DC_INDICATOR, B.INFRC_DC_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SECURITISATION, B.INFRC_SCRTZN)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_RESECURITISATION, B.INFRC_RESCRTIZ)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ASSET_BACKED, B.INFRC_ASSET_BKCD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.PRD_REPO_PRODUCT, B.INFRC_REPO_PRD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SETT_CREDIT, B.INFRC_SETL_CR)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_CONVERTIBLE, B.INFRC_CONV)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_TANGIBLE_ASSETS, B.INFRC_TNGBL_ASSET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_BS_INDICATOR, B.INFRC_BAL_SHEET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.MAT_INDICATOR, B.INFRC_MTRTY_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_NEAR_FAR_INDIC, B.INFRC_NEAR_FAR_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ENTRY_TYPE, B.INFRC_ENTRY_TY)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CURRENT_DATE'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,DATE ''9999-12-31'''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CASE WHEN C.DEAL_TYPE IS NOT NULL THEN ''DEAL TYPE MAP OVERRIDE'''||cLF;
    SET vSQL_Text = vSQL_Text || '         WHEN B.SR_AC IS NOT NULL THEN ''HFM PSGL MAP'' ELSE ''NOT MAPPED'' END'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLWorkDBName || '.SR_GL_PRD PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.HFM_ACCOUNT_CODE = B.SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_ACCOUNT_CODE = B.GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_BUSINESS_UNIT = B.BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_PRODUCT_CODE = B.GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_AFFILIATE_CODE = B.AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLWorkDBName || '.PRD_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.DEAL_TYPE = C.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEAL_SUBTYPE = C.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.VALUATION_TYPE = C.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.CHEQUE_IND = C.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEBIT_CREDIT_IND = C.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.AT_CALL_INDICATOR = C.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C.OVERRIDE_IND = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    IF vExecute_Flag = 'Y' THEN
        SET oReturn_Message = 'Create and Load Cascading Defaults for '||vBssns_Date||'. Completed Successfully';
    ELSE
        SET oReturn_Message = 'Create SQL only for '||vBssns_Date||' in T_SQL_Log BRL table. Completed Successfully';
    END IF;

    SET oReturn_Code = 0;

END MAIN;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP_STEP1
-- =============================================
-- Description: This procedure populate the Cascading Prompts Mapping tables
-- Change log
--      [2016 06 23]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iExecution_Flag            CHAR(1),
--
-- Possible Values
--   Y  Execute Transform View SQL. Capture SQL in V_SQL_Log.
--   N  Do not Execute Transform View SQL. Capture SQL in V_SQL_Log.
--
--  Business Date is retrieved from vBRLVWDBName.T_APRA_RPT_PRD
--
 OUT oReturn_Code              SMALLINT,             /* 0: Successful; 1: Error */
 OUT oReturn_Message           VARCHAR(1000)
--
-- Run Time Format:
--
-- Example CALL PROCEDURE syntax
--      Example 1. Execution Flag = Y
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP ('Y', oReturn, oReturn_Message);
--      Example 2. Execution Flag = N
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP ('N', oReturn, oReturn_Message);
--
)
MAIN:
BEGIN
--
---------------------------------------------------------------------------------------------
-- Variables declaration
---------------------------------------------------------------------------------------------       
--
-- Declare Constants
--
DECLARE vBRLStoredProcDBName        VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_PGM';
DECLARE vBRLWorkDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_WK';
DECLARE vBRLVWDBName                VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_VW';
DECLARE vBRLGENVWDBName             VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_GEN_VW';
DECLARE vBRLTDBName                 VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_T';
DECLARE vCNSUMVWDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_VW';
DECLARE vCNSUMTDBName               VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_T';
DECLARE vStoredProcName             VARCHAR(128) DEFAULT 'BRL_CSCDNG_DFLTS_MAP';
DECLARE cLF                         CHAR(2) DEFAULT '0A'XC;
--
--
-- Declare variables
--
DECLARE vStepNbr                    SMALLINT DEFAULT 0;
DECLARE vSQLStep                    INTEGER DEFAULT 1;
DECLARE vExecute_Flag               CHAR(1) DEFAULT 'N';
DECLARE vBssns_Date		            VARCHAR(10);
DECLARE vSQL_Text                   VARCHAR(16384) DEFAULT '';
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);
--
-- Declare Error Handler variables
--
DECLARE vSQL_Code                   INTEGER;
DECLARE vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE vError_Text                 VARCHAR(256);
--
-- Error Handler
--
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
--
-- Preserve Diagnostic Codes from errored Statement
--
		SET vSQL_Code  = SQLCODE;
		SET vSQL_State = SQLSTATE;
		GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
		SET oReturn_Code = 1;
		SET oReturn_Message = 'Stored Procedure = '||vStoredProcName||', Step Nbr = '||TRIM(vStepNbr)||cLF||'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	END
	;
--
-- Validate Input Parameters
--
    IF iExecution_Flag NOT IN ('Y', 'N') THEN
        SET vExecute_Flag = 'N';
    ELSE
        SET vExecute_Flag = iExecution_Flag;
    END IF;
--
-- Get Business Date
--  Get latest END_TS just in case
--
    SET vStepNbr = 1;
    BEGIN
        DECLARE C1 CURSOR FOR S1;
        SET vSQL_Text = 'SELECT CAST((END_TS (FORMAT ''YYYY-MM-DD'')) AS CHAR(10)) FROM ' || vBRLVWDBName || '.T_APRA_RPT_PRD QUALIFY ROW_NUMBER() OVER (ORDER BY END_TS DESC) = 1';
        PREPARE S1 FROM vSQL_Text;
        OPEN C1;
        FETCH C1 INTO vBssns_Date;
        CLOSE C1;
    END
    ;
--
-- Truncate the V_SQL_Log tables in BRL for the Business Date
--
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.V_SQL_Log WHERE SP_Name = ''' || vStoredProcName || ''' AND Bssns_Date = DATE ''' || vBssns_Date || ''';';
    EXECUTE IMMEDIATE vSQL_Text;
--
-- Populate the L0624 Entity tree BRL work table
--
    SET vStepNbr = 10;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_L0624_ENTITY;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_ENTITY', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_L0624_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_LEAF_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ROOT_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_IMM_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ENTITY_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || '  AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LABEL AS HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_IMM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DESCRIPTIONS AS HFM_ENTITY_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CASE WHEN HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,0 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vCNSUMVWDBName ||'.SR_ENTITY_TREE'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE DEFAULT_PARENT = ''L0624'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.HFM_ROOT_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DESCRIPTIONS'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM.HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ENTITIES A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN ' || vCNSUMVWDBName ||'.SR_ENTITY_TREE HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.HFM_LEAF_ENTITY = HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25 AND A.IS_LEAF_FLAG = ''N'''||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LEAF_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ROOT_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_IMM_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ENTITY_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '    IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '    DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_ENTITY', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the L0624 Entity leaf nodes BRL work table
--
    SET vStepNbr = 11;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_L0624_LEAFS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_L0624_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || '( LCD_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_LEGAL_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM_LEAF_ENTITY AS LCD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_IMM_PARENT_ENTITY AS NODE_LEGAL_ENT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_ENTITY_DESC AS LCD_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,IS_LEAF_FLAG AS IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,1 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM '|| vBRLWorkDBName || '.SR_L0624_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     A.LCD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.HFM_IMM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.LCD_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ENTITIES A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN '|| vBRLWorkDBName || '.SR_L0624_ENTITY HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.NODE_LEGAL_ENT_CD = HFM.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT '||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_CD, '||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_LEGAL_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_L0624_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Balance Sheet Account tree BRL work table
--
    SET vStepNbr = 12;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_ACCNT', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_LEAF_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ROOT_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_IMM_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ACCOUNT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     LABEL AS HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_IMM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DESCRIPTIONS AS HFM_ACCOUNT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,0 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE DEFAULT_PARENT = ''BALSHEET'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.HFM_ROOT_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DESCRIPTIONS'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM.HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ACCOUNTS A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.HFM_LEAF_ACCOUNT = HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE A.DEPTH <= 25 AND A.IS_LEAF_FLAG = ''N'''||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LEAF_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ROOT_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_IMM_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ACCOUNT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '    IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '    DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_ACCNT', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Balance Sheet Account leaf nodes BRL work table
-- Includes a row for each leaf mapped to itself with at depth of 0
--
    SET vStepNbr = 13;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM_LEAF_ACCOUNT AS LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_IMM_PARENT_ACCOUNT AS NODE_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_ACCOUNT_DESC AS LEAF_ACCT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,IS_LEAF_FLAG AS IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,1 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     A.LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.HFM_IMM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.LEAF_ACCT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.IS_LEAF'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ACCOUNTS A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN '|| vBRLWorkDBName || '.SR_BALSHEET_ACCNT HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.NODE_ACCT_CD = HFM.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;
  
    SET vStepNbr = 14;
    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF,'||cLF;
    SET vSQL_Text = vSQL_Text || '  0'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE DEPTH = 1'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_BALSHEET_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR GL Balance table in BRL layer
--
    SET vStepNbr = 20;
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.INFRC_SR_GL_BAL;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.INFRC_SR_GL_BAL'||cLF;
    SET vSQL_Text = vSQL_Text || '( ENT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BSBCC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ''L0624'' AS ENT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BUSINESS_UNIT AS PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.ACCOUNT_1 AS PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.PRODUCT AS PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.AFFILIATE AS PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.DEPTID AS PSGL_BSBCC'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.PROJECT_ID AS PSGL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.CURRENCY_CD AS TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_TRAN_TOTAL) AS TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BASE_CURRENCY AS LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_BASE_TOTAL) AS LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BASE_CURRENCY AS RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_BASE_TOTAL) AS RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.SR_GL_BAL gl'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_L0624_ENTITY ent'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON gl.HFM_LCD = ent.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_BALSHEET_ACCNT bsh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON gl.HFM_ACCT_CD = bsh.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE ent.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || '  AND bsh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14'||cLF;
    SET vSQL_Text = vSQL_Text || 'HAVING RPT_AMT <> 0'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR GL Account Association table in BRL layer
--
    SET vStepNbr = 21;
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_ACCT_ASSOC', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' ||vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || '   ENT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '   ''L0624'''||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM'||cLF;
    SET vSQL_Text = vSQL_Text || '  ('||cLF;
    SET vSQL_Text = vSQL_Text || '   SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     sbh.SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.USR_FLD_1 AS BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN sbh.VALTN_TY IS NULL OR sbh.VALTN_TY = '''' THEN ''Gross Balance'' ELSE sbh.VALTN_TY END AS VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,COALESCE(amap.SR_ACCT, ''Unknown HFM Account'') AS SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.DEPT_ID AS BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,COALESCE(lmap.SR_LCD, ''Unknown HFM LCD'') AS SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.GL_AC AS GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.GL_PRD AS GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.AFFIL AS GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.PROJ_ID AS GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.BSNS_UNT AS GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.CUR_CD AS LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || '   FROM ' || vCNSUMVWDBName || '.SRC_ACC_BAL_HIST sbh'||cLF;
    SET vSQL_Text = vSQL_Text || '   LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP amap'||cLF;
    SET vSQL_Text = vSQL_Text || '   ON sbh.GL_AC = amap.GL_ACCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_DEPT_LCD_MAP lmap'||cLF;
    SET vSQL_Text = vSQL_Text || '   ON sbh.BSNS_UNT = lmap.GL_BSSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || '   AND sbh.DEPT_ID = lmap.GL_DEPTID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ) DT'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_L0624_ENTITY ent'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DT.SR_LCD = ent.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_BALSHEET_ACCNT bsh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DT.SR_ACCT_CD = bsh.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE ent.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || '  AND bsh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_ACCT_ASSOC', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR Balance table in BRL layer
--
    SET vStepNbr = 22;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.INFRC_SR_BAL;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLVWDBName || '.INFRC_SR_BAL'||cLF;
    SET vSQL_Text = vSQL_Text || '(  ENT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_YEAR_1'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_MONTH_1'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     ''L0624''                        -- Business Rules to be determined'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.Year_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.Month_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.ACCOUNT_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,NULL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,NULL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     = MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END) = ''AUD'''||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     ELSE MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,SUM((CASE WHEN UPPER(act.ACCOUNTTYPE) IN (''ASSET'', ''EXPENSE'') THEN 1000 ELSE -1000 END) * CAST(tcl.Amount AS DECIMAL(36,8)))'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     = MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END) = ''AUD'''||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     ELSE MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,SUM((CASE WHEN tcl.Currency IN (''AUD'', ''AUD Adjs'') THEN (CASE WHEN act.ACCOUNTTYPE IN (''ASSET'', ''EXPENSE'') THEN 1000 ELSE -1000 END) ELSE 0 END) * CAST(tcl.Amount AS DECIMAL(36,8)))'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.HFMZZ_EA_TCL tcl'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE act'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ACCOUNT_1 = act.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_BALSHEET_ACCNT acth'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ACCOUNT_1 = acth.HFM_LEAF_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_L0624_ENTITY lcdh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ENTITY = lcdh.HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_CUSTOM1_HIER C1'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.Custom1 = C1.LEAF_CUSTOM1'||cLF;
    SET vSQL_Text = vSQL_Text || 'OR tcl.Custom1 = ''STAT_ADJ'''||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_CUSTOM3_HIER C3'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.Custom3 = C3.LEAF_CUSTOM3'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE tcl.Scenario = ''ACTUAL'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom2 = ''EOP'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom4 = ''[None]'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Periodicity = ''YTD'''||cLF;
    SET vSQL_Text = vSQL_Text || '-- AND tcl.Currency IN (''AUD'', ''AUD Adjs'')'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND acth.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND lcdh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C1.NODE_CUSTOM1 = ''MGMT'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom1 <> ''MGMT_ADJ'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C3.NODE_CUSTOM3 = ''TB_TOT'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Year_1 = (SELECT CASE WHEN month_of_year >= 10 THEN year_of_calendar+1 ELSE year_of_calendar END FROM Sys_Calendar.CALENDAR WHERE calendar_date = DATE ''' || vBssns_Date || ''')'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Month_1 = (SELECT b.SR_MTH FROM Sys_Calendar.CALENDAR a INNER JOIN ' || vCNSUMVWDBName || '.SR_MTH b ON a.month_of_year = b.MTH WHERE calendar_date = DATE ''' || vBssns_Date || ''')'||cLF;
    SET vSQL_Text = vSQL_Text || 'GROUP BY 1,2,3,4,5'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the HFM PSGL EXPLO Ledger work table in BRL Layer
--
    SET vStepNbr = 23;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_EXPLO_SUBLEDGER', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUSINESS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE)'||cLF;
-- PSGL Entries from INFRC_SR_GL_BAL
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_GL_BAL'||cLF;
-- HFM Entries from INFRC_SR_BAL
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_BAL'||cLF;
-- EXPLO Entries from INFRC_SR_GL_ACCT_ASSOC
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_EXPLO_SUBLEDGER', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Start of the Cascading Defaults process
--
-- Populate the HFM Leaf Defaults work table in BRL Layer
--
    SET vStepNbr = 24;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_LEAF_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DF_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION '||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ( SELECT ROW_NUMBER() OVER (PARTITION BY B.LEAF_ACCT_CD ORDER BY CASE WHEN B.LEAF_ACCT_CD = A.HFM_ACCOUNT_CODE THEN 1 ELSE 9 END, B.DEPTH) AS ROW_NR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              B.LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BAL_CLASS,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_DF_PRODUCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.REGULATORY_PRODUCT_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BSS_ITEM_CDE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BSS_ITEM_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_DC_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SECURITISATION,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_RESECURITISATION,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_ASSET_BACKED,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.PRD_REPO_PRODUCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SETT_CREDIT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_CONVERTIBLE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_TANGIBLE_ASSETS,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_BS_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_NEAR_FAR_INDIC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_ENTRY_TYPE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_PURPOSE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CTY_RESIDENCE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_REVOLVING_FAC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_CPTY_TYPE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CDE_CONSOLIDATION_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_COLLATERAL,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_PORTFOLIO,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_INTEREST_RATE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CCY_ISO_CURRENCY,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_REMAINING_BKT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_ORIGINAL_BKT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CTY_ISSUE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_LISTED,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SUBORDINATION'||cLF;
    SET vSQL_Text = vSQL_Text || '      FROM ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS A'||cLF;
    SET vSQL_Text = vSQL_Text || '      INNER JOIN '|| vBRLWorkDBName || '.SR_BALSHEET_LEAFS B'||cLF;
    SET vSQL_Text = vSQL_Text || '      ON A.HFM_ACCOUNT_CODE = B.NODE_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '      WHERE A.PSGL_ACCOUNT_CODE = '''''||cLF;
    SET vSQL_Text = vSQL_Text || '     ) C'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE C.ROW_NR = 1;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the PSGL Leaf Defaults work table in BRL Layer
--
    SET vStepNbr = 25;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT A.SR_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       A.GL_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       CAST(NULL AS VARCHAR(50)),'||cLF;
    SET vSQL_Text = vSQL_Text || '       CAST(NULL AS VARCHAR(50)),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BAL_CLASS, B.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DF_PRODUCT, B.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.REGULATORY_PRODUCT_CODE, B.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_CDE, B.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_DESC, B.BSS_ITEM_DESC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DC_INDICATOR, B.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SECURITISATION, B.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_RESECURITISATION, B.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ASSET_BACKED, B.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.PRD_REPO_PRODUCT, B.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SETT_CREDIT, B.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CONVERTIBLE, B.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_TANGIBLE_ASSETS, B.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_BS_INDICATOR, B.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_INDICATOR, B.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_NEAR_FAR_INDIC, B.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ENTRY_TYPE, B.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PURPOSE, B.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_RESIDENCE, B.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_REVOLVING_FAC, B.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CPTY_TYPE, B.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CDE_CONSOLIDATION_CODE, B.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_COLLATERAL, B.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PORTFOLIO, B.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_INTEREST_RATE, B.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CCY_ISO_CURRENCY, B.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_REMAINING_BKT, B.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_ORIGINAL_BKT, B.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_ISSUE, B.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_LISTED, B.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SUBORDINATION, B.TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP A'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.SR_ACCT = B.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.GL_ACCT = C.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C.PSGL_PRODUCT_CODE = '''''||cLF;
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT A.SR_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       A.GL_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       C.PSGL_PRODUCT_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       C.PSGL_AFFILIATE_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BAL_CLASS, B.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DF_PRODUCT, B.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.REGULATORY_PRODUCT_CODE, B.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_CDE, B.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_DESC, B.BSS_ITEM_DESC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DC_INDICATOR, B.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SECURITISATION, B.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_RESECURITISATION, B.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ASSET_BACKED, B.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.PRD_REPO_PRODUCT, B.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SETT_CREDIT, B.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CONVERTIBLE, B.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_TANGIBLE_ASSETS, B.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_BS_INDICATOR, B.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_INDICATOR, B.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_NEAR_FAR_INDIC, B.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ENTRY_TYPE, B.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PURPOSE, B.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_RESIDENCE, B.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_REVOLVING_FAC, B.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CPTY_TYPE, B.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CDE_CONSOLIDATION_CODE, B.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_COLLATERAL, B.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PORTFOLIO, B.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_INTEREST_RATE, B.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CCY_ISO_CURRENCY, B.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_REMAINING_BKT, B.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_ORIGINAL_BKT, B.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_ISSUE, B.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_LISTED, B.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SUBORDINATION, B.TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP A'||cLF;
    SET vSQL_Text = vSQL_Text || 'JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.SR_ACCT = B.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.GL_ACCT = C.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE C.PSGL_PRODUCT_CODE <> '''';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface HFM PSGL Map Defaults table in BRL layer
--   Truncate records for current Business Date
--
    SET vStepNbr = 26;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE BSNS_DT = DATE ''' || vBssns_Date || ''';';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( BSNS_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRD_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_DC_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SCRTZN'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RESCRTIZ'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ASSET_BKCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_REPO_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SETL_CR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CONV'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_TNGBL_ASSET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_SHEET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_MTRTY_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_NEAR_FAR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ENTRY_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRPS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RES_CNRTY_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RVLV_FCLT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CNTRPRT_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CNSLDT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_COLLAT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PORTF'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INT_RTE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CUR_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_RMN_BCKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORGNL_BCKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,ISS_CNTRY_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_LSTD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INRFC_SUBB'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EFFV_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EXPR_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,USR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DATE ''' || vBssns_Date || ''','||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.HFM_ACCOUNT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_ACCOUNT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_PRODUCT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_AFFILIATE_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_BUSINESS_UNIT, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.AMOUNT_FIELD, PSGL.AMOUNT_FIELD, HFM.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.PRODUCT_TYPE, PSGL.PRODUCT_TYPE, HFM.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.REGULATORY_PRODUCT_CODE, PSGL.REGULATORY_PRODUCT_CODE, HFM.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_DC_INDICATOR, PSGL.TYP_DC_INDICATOR, HFM.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SECURITISATION, PSGL.TYP_SECURITISATION, HFM.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_RESECURITISATION, PSGL.TYP_RESECURITISATION, HFM.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_ASSET_BACKED, PSGL.TYP_ASSET_BACKED, HFM.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.PRD_REPO_PRODUCT, PSGL.PRD_REPO_PRODUCT, HFM.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SETT_CREDIT, PSGL.TYP_SETT_CREDIT, HFM.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_CONVERTIBLE, PSGL.TYP_CONVERTIBLE, HFM.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_TANGIBLE_ASSETS, PSGL.TYP_TANGIBLE_ASSETS, HFM.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_BS_INDICATOR, PSGL.TYP_BS_INDICATOR, HFM.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_INDICATOR, PSGL.MAT_INDICATOR, HFM.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_NEAR_FAR_INDIC, PSGL.TYP_NEAR_FAR_INDIC, HFM.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_ENTRY_TYPE, PSGL.TYP_ENTRY_TYPE, HFM.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_PURPOSE, PSGL.TYP_PURPOSE, HFM.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.CTY_RESIDENCE, PRD.CTY_RESIDENCE, PSGL.CTY_RESIDENCE, HFM.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_REVOLVING_FAC, PSGL.TYP_REVOLVING_FAC, HFM.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.TYP_CPTY_TYPE, PRD.TYP_CPTY_TYPE, PSGL.TYP_CPTY_TYPE, HFM.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.CDE_CONSOLIDATION_CODE, PRD.CDE_CONSOLIDATION_CODE, PSGL.CDE_CONSOLIDATION_CODE, HFM.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_COLLATERAL, PSGL.TYP_COLLATERAL, HFM.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_PORTFOLIO, PSGL.TYP_PORTFOLIO, HFM.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_INTEREST_RATE, PSGL.TYP_INTEREST_RATE, HFM.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.CCY_ISO_CURRENCY, PSGL.CCY_ISO_CURRENCY, HFM.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_REMAINING_BKT, PSGL.MAT_REMAINING_BKT, HFM.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_ORIGINAL_BKT, PSGL.MAT_ORIGINAL_BKT, HFM.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.CTY_ISSUE, PSGL.CTY_ISSUE, HFM.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_LISTED, PSGL.TYP_LISTED, HFM.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SUBORDINATION, PSGL.TYP_SUBORDINATION, HFM.TYP_SUBORDINATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       CURRENT_DATE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       DATE ''9999-12-31'','||cLF;
--    SET vSQL_Text = vSQL_Text || '       ''verweyt'','||cLF;
    SET vSQL_Text = vSQL_Text || '       CASE WHEN AFF.AFFILIATE_CODE IS NOT NULL THEN'||cLF;
    SET vSQL_Text = vSQL_Text || '           (CASE WHEN PRD.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL PROD AND AFFIL OVRD MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '                 WHEN PSGL.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL ACCT AND AFFIL OVRD MAP'' ELSE ''HFM MAP AND AFFIL OVRD MAP'' END)'||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN PRD.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL PROD MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN PSGL.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL ACCT MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN HFM.HFM_ACCOUNT_CODE IS NOT NULL THEN ''HFM MAP'' ELSE ''NOT MAPPED'' END,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.BSS_ITEM_CDE, PSGL.BSS_ITEM_CDE, HFM.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.BSS_ITEM_DESC, PSGL.BSS_ITEM_DESC, HFM.BSS_ITEM_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER DB'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS HFM'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = HFM.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = PRD.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_ACCOUNT_CODE = PRD.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_PRODUCT_CODE = PRD.PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_PRODUCT_CODE IS NOT NULL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS PSGL'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = PSGL.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_ACCOUNT_CODE = PSGL.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PSGL.PSGL_PRODUCT_CODE IS NULL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_PSGL_AFF_DEFAULTS AFF'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.PSGL_AFFILIATE_CODE = AFF.AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

/*
--
-- Populate the HFM PSGL Deal Type Deal Sub Type combination work table in BRL layer
--
    SET vStepNbr = 27;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_PRD;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.SR_GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUSINESS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
--    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(SMAP.SR_ACCT, ''Unknown HFM Account''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_AC,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_PRD,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.AFFIL,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.BSNS_UNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       CASE AR.FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_loan'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_interest_bearing_accounts'' THEN ''DP'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_bond'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_equity'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_lease'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_money_market'' THEN ''CA'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_repo_style'' THEN ''RE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_future'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_option'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_credit_derivatives'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_forex'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_fra'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_futures'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_fx'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_ip'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_facility'' THEN ''LI'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_fixed_asset'' THEN ''AT'''||cLF;
    SET vSQL_Text = vSQL_Text || '          ELSE '''''||cLF;
    SET vSQL_Text = vSQL_Text || '       END AS Product_Type,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_SUB_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_47, '''') AS CHEQUE_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.INFRC_VALTN_TY AS VALTN_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.DEBIT_CREDIT_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_50, '''') AS AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLGENVWDBName || '.EFFV_OUT_AR_TO_GL_LNK LNK'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_ACCT_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ') AR'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = AR.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = AR.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.BAL_TY = AR.BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT OUTER JOIN ' || vBRLVWDBName || '.PIVOT_BRO_AR BRO'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = BRO.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = BRO.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP SMAP'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.GL_AC = SMAP.GL_ACCT;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Product Map Defaults work table in BRL Layer
--   First where the Debit Credit Indicator is not blank
--
    SET vStepNbr = 28;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND <> '''';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
--   Next where the Debit Credit Indicator is blank explode for 'C' and 'D' values
--
    SET vStepNbr = 29;
    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'CROSS JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT DEBIT_CREDIT_IND FROM (SELECT ''C'' AS DEBIT_CREDIT_IND) cd1 UNION SELECT DEBIT_CREDIT_IND FROM (SELECT ''D'' AS DEBIT_CREDIT_IND) cd2'||cLF;
    SET vSQL_Text = vSQL_Text || ') cjn_dc'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND = '''''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND NOT EXISTS'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  xx.DEAL_TYPE, xx.DEAL_SUBTYPE, xx.VALUATION_TYPE, xx.DEBIT_CREDIT_IND, xx.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS xx'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE xx.DEAL_TYPE = dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEAL_SUBTYPE = dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.VALUATION_TYPE = dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEBIT_CREDIT_IND = cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.CHEQUE_IND = dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.AT_CALL_IND = dflt.AT_CALL_IND);'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface HFM PSGL Map Defaults table in BRL layer
--   Truncate records for current Business Date
--
    SET vStepNbr = 30;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE BSNS_DT = DATE ''' || vBssns_Date || ''';';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( BSNS_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CHQ_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DR_CR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRD_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_DC_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SCRTZN'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RESCRTIZ'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ASSET_BKCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_REPO_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SETL_CR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CONV'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_TNGBL_ASSET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_SHEET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_MTRTY_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_NEAR_FAR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ENTRY_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EFFV_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EXPR_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,USR_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '    DATE ''' || vBssns_Date || ''''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.HFM_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_PRODUCT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_AFFILIATE_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_BUSINESS_UNIT, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_SUBTYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.CHEQUE_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEBIT_CREDIT_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.VALUATION_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.AT_CALL_INDICATOR, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.AMOUNT_FIELD, B.BAL_TY, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PRODUCT_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.REGULATORY_PRODUCT_CODE, B.REG_PRD_CD, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_CDE, B.REG_BSS_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_DESC, B.REG_BSS_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_DC_INDICATOR, B.INFRC_DC_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SECURITISATION, B.INFRC_SCRTZN)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_RESECURITISATION, B.INFRC_RESCRTIZ)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ASSET_BACKED, B.INFRC_ASSET_BKCD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.PRD_REPO_PRODUCT, B.INFRC_REPO_PRD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SETT_CREDIT, B.INFRC_SETL_CR)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_CONVERTIBLE, B.INFRC_CONV)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_TANGIBLE_ASSETS, B.INFRC_TNGBL_ASSET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_BS_INDICATOR, B.INFRC_BAL_SHEET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.MAT_INDICATOR, B.INFRC_MTRTY_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_NEAR_FAR_INDIC, B.INFRC_NEAR_FAR_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ENTRY_TYPE, B.INFRC_ENTRY_TY)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CURRENT_DATE'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,DATE ''9999-12-31'''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CASE WHEN C.DEAL_TYPE IS NOT NULL THEN ''DEAL TYPE MAP OVERRIDE'''||cLF;
    SET vSQL_Text = vSQL_Text || '         WHEN B.SR_AC IS NOT NULL THEN ''HFM PSGL MAP'' ELSE ''NOT MAPPED'' END'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLWorkDBName || '.SR_GL_PRD PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.HFM_ACCOUNT_CODE = B.SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_ACCOUNT_CODE = B.GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_BUSINESS_UNIT = B.BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_PRODUCT_CODE = B.GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_AFFILIATE_CODE = B.AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLWorkDBName || '.PRD_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.DEAL_TYPE = C.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEAL_SUBTYPE = C.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.VALUATION_TYPE = C.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.CHEQUE_IND = C.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEBIT_CREDIT_IND = C.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.AT_CALL_INDICATOR = C.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C.OVERRIDE_IND = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;
*/
    IF vExecute_Flag = 'Y' THEN
        SET oReturn_Message = 'Create and Load Cascading Defaults for '||vBssns_Date||'. Completed Successfully';
    ELSE
        SET oReturn_Message = 'Create SQL only for '||vBssns_Date||' in T_SQL_Log BRL table. Completed Successfully';
    END IF;

    SET oReturn_Code = 0;

END MAIN;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP_STEP2
-- =============================================
-- Description: This procedure populate the Cascading Prompts Mapping tables
-- Change log
--      [2016 06 23]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iExecution_Flag            CHAR(1),
--
-- Possible Values
--   Y  Execute Transform View SQL. Capture SQL in V_SQL_Log.
--   N  Do not Execute Transform View SQL. Capture SQL in V_SQL_Log.
--
--  Business Date is retrieved from vBRLVWDBName.T_APRA_RPT_PRD
--
 OUT oReturn_Code              SMALLINT,             /* 0: Successful; 1: Error */
 OUT oReturn_Message           VARCHAR(1000)
--
-- Run Time Format:
--
-- Example CALL PROCEDURE syntax
--      Example 1. Execution Flag = Y
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP ('Y', oReturn, oReturn_Message);
--      Example 2. Execution Flag = N
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP ('N', oReturn, oReturn_Message);
--
)
MAIN:
BEGIN
--
---------------------------------------------------------------------------------------------
-- Variables declaration
---------------------------------------------------------------------------------------------       
--
-- Declare Constants
--
DECLARE vBRLStoredProcDBName        VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_PGM';
DECLARE vBRLWorkDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_WK';
DECLARE vBRLVWDBName                VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_VW';
DECLARE vBRLGENVWDBName             VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_GEN_VW';
DECLARE vBRLTDBName                 VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_T';
DECLARE vCNSUMVWDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_VW';
DECLARE vCNSUMTDBName               VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_T';
DECLARE vStoredProcName             VARCHAR(128) DEFAULT 'BRL_CSCDNG_DFLTS_MAP';
DECLARE cLF                         CHAR(2) DEFAULT '0A'XC;
--
--
-- Declare variables
--
DECLARE vStepNbr                    SMALLINT DEFAULT 0;
DECLARE vSQLStep                    INTEGER DEFAULT 1;
DECLARE vExecute_Flag               CHAR(1) DEFAULT 'N';
DECLARE vBssns_Date		            VARCHAR(10);
DECLARE vSQL_Text                   VARCHAR(16384) DEFAULT '';
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);
--
-- Declare Error Handler variables
--
DECLARE vSQL_Code                   INTEGER;
DECLARE vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE vError_Text                 VARCHAR(256);
--
-- Error Handler
--
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
--
-- Preserve Diagnostic Codes from errored Statement
--
		SET vSQL_Code  = SQLCODE;
		SET vSQL_State = SQLSTATE;
		GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
		SET oReturn_Code = 1;
		SET oReturn_Message = 'Stored Procedure = '||vStoredProcName||', Step Nbr = '||TRIM(vStepNbr)||cLF||'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	END
	;
--
-- Validate Input Parameters
--
    IF iExecution_Flag NOT IN ('Y', 'N') THEN
        SET vExecute_Flag = 'N';
    ELSE
        SET vExecute_Flag = iExecution_Flag;
    END IF;
--
-- Get Business Date
--  Get latest END_TS just in case
--
    SET vStepNbr = 1;
    BEGIN
        DECLARE C1 CURSOR FOR S1;
        SET vSQL_Text = 'SELECT CAST((END_TS (FORMAT ''YYYY-MM-DD'')) AS CHAR(10)) FROM ' || vBRLVWDBName || '.T_APRA_RPT_PRD QUALIFY ROW_NUMBER() OVER (ORDER BY END_TS DESC) = 1';
        PREPARE S1 FROM vSQL_Text;
        OPEN C1;
        FETCH C1 INTO vBssns_Date;
        CLOSE C1;
    END
    ;
--
--
-- Populate the HFM PSGL Deal Type Deal Sub Type combination work table in BRL layer
--
    SET vStepNbr = 2;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_PRD;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.SR_GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUSINESS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
--    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(SMAP.SR_ACCT, ''Unknown HFM Account''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_AC,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_PRD,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.AFFIL,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.BSNS_UNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       CASE AR.FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_loan'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_interest_bearing_accounts'' THEN ''DP'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_bond'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_equity'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_lease'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_money_market'' THEN ''CA'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_repo_style'' THEN ''RE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_future'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_option'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_credit_derivatives'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_forex'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_fra'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_futures'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_fx'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_ip'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_facility'' THEN ''LI'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_fixed_asset'' THEN ''AT'''||cLF;
    SET vSQL_Text = vSQL_Text || '          ELSE '''''||cLF;
    SET vSQL_Text = vSQL_Text || '       END AS Product_Type,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_SUB_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_47, '''') AS CHEQUE_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.INFRC_VALTN_TY AS VALTN_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.DEBIT_CREDIT_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_50, '''') AS AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLGENVWDBName || '.EFFV_OUT_AR_TO_GL_LNK LNK'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_ACCT_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ') AR'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = AR.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = AR.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.BAL_TY = AR.BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT OUTER JOIN ' || vBRLVWDBName || '.PIVOT_BRO_AR BRO'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = BRO.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = BRO.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP SMAP'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.GL_AC = SMAP.GL_ACCT;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Product Map Defaults work table in BRL Layer
--   First where the Debit Credit Indicator is not blank
--
    SET vStepNbr = 3;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND <> '''';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
--   Next where the Debit Credit Indicator is blank explode for 'C' and 'D' values
--
    SET vStepNbr = 4;
    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'CROSS JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT DEBIT_CREDIT_IND FROM (SELECT ''C'' AS DEBIT_CREDIT_IND) cd1 UNION SELECT DEBIT_CREDIT_IND FROM (SELECT ''D'' AS DEBIT_CREDIT_IND) cd2'||cLF;
    SET vSQL_Text = vSQL_Text || ') cjn_dc'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND = '''''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND NOT EXISTS'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  xx.DEAL_TYPE, xx.DEAL_SUBTYPE, xx.VALUATION_TYPE, xx.DEBIT_CREDIT_IND, xx.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS xx'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE xx.DEAL_TYPE = dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEAL_SUBTYPE = dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.VALUATION_TYPE = dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEBIT_CREDIT_IND = cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.CHEQUE_IND = dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.AT_CALL_IND = dflt.AT_CALL_IND);'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface HFM PSGL Map Defaults table in BRL layer
--   Truncate records for current Business Date
--
    SET vStepNbr = 5;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE BSNS_DT = DATE ''' || vBssns_Date || ''';';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( BSNS_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CHQ_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DR_CR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRD_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_DC_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SCRTZN'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RESCRTIZ'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ASSET_BKCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_REPO_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SETL_CR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CONV'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_TNGBL_ASSET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_SHEET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_MTRTY_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_NEAR_FAR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ENTRY_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EFFV_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EXPR_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,USR_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '    DATE ''' || vBssns_Date || ''''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.HFM_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_PRODUCT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_AFFILIATE_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_BUSINESS_UNIT, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_SUBTYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.CHEQUE_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEBIT_CREDIT_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.VALUATION_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.AT_CALL_INDICATOR, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.AMOUNT_FIELD, B.BAL_TY, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PRODUCT_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.REGULATORY_PRODUCT_CODE, B.REG_PRD_CD, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_CDE, B.REG_BSS_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_DESC, B.REG_BSS_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_DC_INDICATOR, B.INFRC_DC_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SECURITISATION, B.INFRC_SCRTZN)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_RESECURITISATION, B.INFRC_RESCRTIZ)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ASSET_BACKED, B.INFRC_ASSET_BKCD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.PRD_REPO_PRODUCT, B.INFRC_REPO_PRD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SETT_CREDIT, B.INFRC_SETL_CR)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_CONVERTIBLE, B.INFRC_CONV)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_TANGIBLE_ASSETS, B.INFRC_TNGBL_ASSET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_BS_INDICATOR, B.INFRC_BAL_SHEET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.MAT_INDICATOR, B.INFRC_MTRTY_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_NEAR_FAR_INDIC, B.INFRC_NEAR_FAR_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ENTRY_TYPE, B.INFRC_ENTRY_TY)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CURRENT_DATE'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,DATE ''9999-12-31'''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CASE WHEN C.DEAL_TYPE IS NOT NULL THEN ''DEAL TYPE MAP OVERRIDE'''||cLF;
    SET vSQL_Text = vSQL_Text || '         WHEN B.SR_AC IS NOT NULL THEN ''HFM PSGL MAP'' ELSE ''NOT MAPPED'' END'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLWorkDBName || '.SR_GL_PRD PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.HFM_ACCOUNT_CODE = B.SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_ACCOUNT_CODE = B.GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_BUSINESS_UNIT = B.BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_PRODUCT_CODE = B.GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_AFFILIATE_CODE = B.AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLWorkDBName || '.PRD_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.DEAL_TYPE = C.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEAL_SUBTYPE = C.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.VALUATION_TYPE = C.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.CHEQUE_IND = C.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEBIT_CREDIT_IND = C.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.AT_CALL_INDICATOR = C.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C.OVERRIDE_IND = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    IF vExecute_Flag = 'Y' THEN
        SET oReturn_Message = 'Create and Load Cascading Defaults for '||vBssns_Date||'. Completed Successfully';
    ELSE
        SET oReturn_Message = 'Create SQL only for '||vBssns_Date||' in T_SQL_Log BRL table. Completed Successfully';
    END IF;

    SET oReturn_Code = 0;

END MAIN;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP_V2
-- =============================================
-- Description: This procedure populate the Cascading Prompts Mapping tables
-- Change log
--      [2016 06 23]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iSR_LCD                     VARCHAR(35),
 IN iSR_Acct                    VARCHAR(35),
 IN iExecution_Flag             CHAR(1),
--
-- Possible Values
--   Y  Execute Transform View SQL. Capture SQL in V_SQL_Log.
--   N  Do not Execute Transform View SQL. Capture SQL in V_SQL_Log.
--
--  Business Date is retrieved from vBRLVWDBName.T_APRA_RPT_PRD
--
 OUT oReturn_Code               SMALLINT,             /* 0: Successful; 1: Error */
 OUT oReturn_Message            VARCHAR(1000)
--
-- Run Time Format:
--
-- Example CALL PROCEDURE syntax
--      Example 1. Execution Flag = Y, L0624 (TB Aust (Incl. MIDANZ)) Entity hierarchy and BALSHEET Account hierarchy
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP_V2 ('L0624', 'BALSHEET', 'Y', oReturn, oReturn_Message);
--      Example 2. Execution Flag = N, L0624 (TB Aust (Incl. MIDANZ)) Entity hierarchy and BALSHEET Account hierarchy
-- CALL QSIT_APRA2_BRL_RRP_PGM.BRL_CSCDNG_DFLTS_MAP_V2 ('L0624', 'BALSHEET', 'N', oReturn, oReturn_Message);
--
)
MAIN:
BEGIN
--
---------------------------------------------------------------------------------------------
-- Variables declaration
---------------------------------------------------------------------------------------------       
--
-- Declare Constants
--
DECLARE vBRLStoredProcDBName        VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_PGM';
DECLARE vBRLWorkDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_WK';
DECLARE vBRLVWDBName                VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_VW';
DECLARE vBRLGENVWDBName             VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_GEN_VW';
DECLARE vBRLTDBName                 VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_T';
DECLARE vCNSUMVWDBName              VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_VW';
DECLARE vCNSUMTDBName               VARCHAR(128) DEFAULT 'QSIT_APRA2_CNSUM_RRP_T';
DECLARE vStoredProcName             VARCHAR(128) DEFAULT 'BRL_CSCDNG_DFLTS_MAP_V2';
DECLARE cLF                         CHAR(2) DEFAULT '0A'XC;
--
--
-- Declare variables
--
DECLARE vStepNbr                    SMALLINT DEFAULT 0;
DECLARE vSQLStep                    INTEGER DEFAULT 1;
DECLARE vExecute_Flag               CHAR(1) DEFAULT 'N';
DECLARE vBssns_Date		            VARCHAR(10);
DECLARE vSR_Acct_Lvl                SMALLINT;
DECLARE vSR_Acct                    VARCHAR(35);
DECLARE vSR_LCD_Lvl                 SMALLINT;
DECLARE vSR_LCD                     VARCHAR(35);
DECLARE vSQL_Text                   VARCHAR(16384) DEFAULT '';
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);
--
-- Declare Error Handler variables
--
DECLARE vSQL_Code                   INTEGER;
DECLARE vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE vError_Text                 VARCHAR(256);
--
-- Error Handler
--
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
--
-- Preserve Diagnostic Codes from errored Statement
--
		SET vSQL_Code  = SQLCODE;
		SET vSQL_State = SQLSTATE;
		GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
		SET oReturn_Code = 1;
		SET oReturn_Message = 'Stored Procedure = '||vStoredProcName||', Step Nbr = '||TRIM(vStepNbr)||cLF||'. SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	END
	;
--
-- Validate Input Parameters
--
    SET vStepNbr = 1;
    IF iExecution_Flag NOT IN ('Y', 'N') THEN
        SET vExecute_Flag = 'N';
    ELSE
        SET vExecute_Flag = iExecution_Flag;
    END IF;
--
-- Get Business Date
--  Get latest END_TS just in case
--
    BEGIN
        DECLARE C1 CURSOR FOR S1;
        SET vSQL_Text = 'SELECT CAST((END_TS (FORMAT ''YYYY-MM-DD'')) AS CHAR(10)) FROM ' || vBRLVWDBName || '.T_APRA_RPT_PRD QUALIFY ROW_NUMBER() OVER (ORDER BY END_TS DESC) = 1';
        PREPARE S1 FROM vSQL_Text;
        OPEN C1;
        FETCH C1 INTO vBssns_Date;
        CLOSE C1;
    END
    ;
--
-- Truncate the V_SQL_Log tables in BRL for the Business Date
--
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.V_SQL_Log WHERE SP_Name = ''' || vStoredProcName || ''' AND Bssns_Date = DATE ''' || vBssns_Date || ''';';
    EXECUTE IMMEDIATE vSQL_Text;
--
-- Populate the Entity Hierarchy tree BRL work table
--  Always truncate and populate Work table - data needed to validate input parameter
--
    SET vStepNbr = 10;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_ENTITY_HIER;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ENTITY_HIER', 'Y', oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_ENTITY_HIER'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_LEAF_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL1_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL2_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL3_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL4_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL5_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_IMM_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ENTITY_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || '  AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LABEL AS HFM_LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_LVL1_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_LVL2_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_LVL3_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_LVL4_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_LVL5_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEFAULT_PARENT AS HFM_IMM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DESCRIPTIONS AS HFM_ENTITY_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CASE WHEN HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,0 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vCNSUMVWDBName ||'.SR_ENTITY_TREE'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE DEFAULT_PARENT = '''''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 0 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL1_PARENT_ENTITY END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 1 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL2_PARENT_ENTITY END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 2 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL3_PARENT_ENTITY END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 3 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL4_PARENT_ENTITY END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 4 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL5_PARENT_ENTITY END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DESCRIPTIONS'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM.HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM HFM_ENTITIES A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN ' || vCNSUMVWDBName ||'.SR_ENTITY_TREE HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.HFM_LEAF_ENTITY = HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25 AND A.IS_LEAF_FLAG = ''N'''||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LEAF_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL1_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL2_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL3_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL4_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL5_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_IMM_PARENT_ENTITY,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ENTITY_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '    IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '    DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ENTITY_HIER', 'Y', oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;
--
-- Validate iSR_LCD input parameter and derive Hierarchy Level
--
    BEGIN
        DECLARE C1 CURSOR FOR S1;
        SET vSQL_Text = 'SELECT HFM_ENTITY, MAX(DEPTH) FROM ' || vBRLWorkDBName ||'.SR_ENTITY_HIER WHERE HFM_ENTITY = ? GROUP BY 1';
        PREPARE S1 FROM vSQL_Text;
        OPEN C1 USING iSR_LCD;
        FETCH C1 INTO vSR_LCD, vSR_LCD_Lvl;
        CLOSE C1;
    END
    ;
    IF (iSR_LCD <> vSR_LCD OR vSR_LCD IS NULL OR vSR_LCD_Lvl IS NULL OR vSR_LCD_Lvl < 0 OR vSR_LCD_Lvl > 4) THEN
        SET oReturn_Code = 1;
        SET vSR_LCD = COALESCE(vSR_LCD, 'NULL');
        SET oReturn_Message = 'Stored Procedure = '||vStoredProcName||', Step Nbr = '||TRIM(vStepNbr)||cLF||'Invalid input parameter iSR_LCD = '||iSR_LCD||'; Returned HFM Entity = '||vSR_LCD;
        LEAVE MAIN;
    END IF;
    SET vSR_LCD_Lvl = vSR_LCD_Lvl+1;
--
-- Populate the iSR_LCD_LVL (for example, L0624) Entity leaf nodes BRL work table
--
    SET vStepNbr = 11;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_ENTITY_LEAFS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ENTITY_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_ENTITY_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || '( LCD_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  ROOT_PARENT_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_LEGAL_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM_ENTITY AS LCD_CD'||cLF;
    CASE
        WHEN vSR_LCD_Lvl = 1 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL1_PARENT_ENTITY AS ROOT_PARENT_ENT_CD'||cLF;
        WHEN vSR_LCD_Lvl = 2 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL2_PARENT_ENTITY AS ROOT_PARENT_ENT_CD'||cLF;
        WHEN vSR_LCD_Lvl = 3 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL3_PARENT_ENTITY AS ROOT_PARENT_ENT_CD'||cLF;
        WHEN vSR_LCD_Lvl = 4 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL4_PARENT_ENTITY AS ROOT_PARENT_ENT_CD'||cLF;
        WHEN vSR_LCD_Lvl = 5 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL5_PARENT_ENTITY AS ROOT_PARENT_ENT_CD'||cLF;
    END CASE;
    SET vSQL_Text = vSQL_Text || '    ,HFM_IMM_PARENT_ENTITY AS NODE_LEGAL_ENT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_ENTITY_DESC AS LCD_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,IS_LEAF_FLAG AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,1 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM '|| vBRLWorkDBName || '.SR_ENTITY_HIER'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE IS_LEAF_FLAG = ''Y'''||cLF;
    CASE
        WHEN vSR_LCD_Lvl = 1 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL1_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 2 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL2_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 3 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL3_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 4 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL4_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 5 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL5_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
    END CASE;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     A.LCD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.ROOT_PARENT_ENT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.HFM_IMM_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.LCD_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ENTITIES A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN '|| vBRLWorkDBName || '.SR_ENTITY_HIER HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.NODE_LEGAL_ENT_CD = HFM.HFM_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25'||cLF;
    CASE
        WHEN vSR_LCD_Lvl = 1 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL1_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 2 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL2_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 3 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL3_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 4 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL4_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
        WHEN vSR_LCD_Lvl = 5 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL5_PARENT_ENTITY = '''||vSR_LCD||''''||cLF;
    END CASE;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT '||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_CD, '||cLF;
    SET vSQL_Text = vSQL_Text || '  ROOT_PARENT_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_LEGAL_ENT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LCD_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ENTITIES'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ENTITY_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Account Hierarchy tree BRL work table
--  Always truncate and populate Work table - data needed to validate input parameter
--
    SET vStepNbr = 12;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_ACCNT_HIER;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ACCNT_HIER', 'Y', oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_ACCNT_HIER'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL1_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL2_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL3_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL4_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_LVL5_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_IMM_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '  HFM_ACCOUNT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     LABEL AS HFM_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_LVL1_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_LVL2_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_LVL3_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_LVL4_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_LVL5_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DEFAULT_PARENT AS HFM_IMM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,DESCRIPTIONS AS HFM_ACCOUNT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,0 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE DEFAULT_PARENT = '''''||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 0 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL1_PARENT_ACCOUNT END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 1 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL2_PARENT_ACCOUNT END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 2 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL3_PARENT_ACCOUNT END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 3 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL4_PARENT_ACCOUNT END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN A.DEPTH = 4 THEN HFM.DEFAULT_PARENT ELSE A.HFM_LVL5_PARENT_ACCOUNT END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.DESCRIPTIONS'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN HFM.HFM_ISBASE = ''True'' THEN ''Y'' ELSE ''N'' END AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM HFM_ACCOUNTS A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.HFM_ACCOUNT = HFM.DEFAULT_PARENT'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE A.DEPTH <= 25 AND A.IS_LEAF_FLAG = ''N'''||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL1_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL2_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL3_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL4_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_LVL5_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_IMM_PARENT_ACCOUNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '    HFM_ACCOUNT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '    IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '    DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ACCNT_HIER', 'Y', oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;
--
-- Validate iSR_Acct input parameter and derive Hierarchy Level
--
    BEGIN
        DECLARE C1 CURSOR FOR S1;
        SET vSQL_Text = 'SELECT HFM_ACCOUNT, MAX(DEPTH) FROM ' || vBRLWorkDBName ||'.SR_ACCNT_HIER WHERE HFM_ACCOUNT = ? GROUP BY 1';
        PREPARE S1 FROM vSQL_Text;
        OPEN C1 USING iSR_Acct;
        FETCH C1 INTO vSR_Acct, vSR_Acct_Lvl;
        CLOSE C1;
    END
    ;
    IF (iSR_Acct <> vSR_Acct OR vSR_Acct IS NULL OR vSR_Acct_Lvl IS NULL OR vSR_Acct_Lvl < 0 OR vSR_Acct_Lvl > 4) THEN
        SET oReturn_Code = 1;
        SET vSR_Acct = COALESCE(vSR_Acct, 'NULL');
        SET oReturn_Message = 'Stored Procedure = '||vStoredProcName||', Step Nbr = '||TRIM(vStepNbr)||cLF||'Invalid input parameter iSR_Acct = '||iSR_Acct||'; Returned HFM Account = '||vSR_Acct;
        LEAVE MAIN;
    END IF;
    SET vSR_Acct_Lvl = vSR_Acct_Lvl+1;
--
-- Populate the Account hierarchy leaf nodes BRL work table
-- Includes a row for each leaf mapped to itself with at depth of 0
--
    SET vStepNbr = 13;
    SET vSQL_Text = 'DELETE FROM '|| vBRLWorkDBName || '.SR_ACCNT_LEAFS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ACCNT_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_ACCNT_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WITH RECURSIVE LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  ROOT_PARENT_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH)'||cLF;
    SET vSQL_Text = vSQL_Text || 'AS'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     HFM_ACCOUNT AS LEAF_ACCT_CD'||cLF;
    CASE
        WHEN vSR_Acct_Lvl = 1 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL1_PARENT_ACCOUNT'||cLF;
        WHEN vSR_Acct_Lvl = 2 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL2_PARENT_ACCOUNT'||cLF;
        WHEN vSR_Acct_Lvl = 3 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL3_PARENT_ACCOUNT'||cLF;
        WHEN vSR_Acct_Lvl = 4 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL4_PARENT_ACCOUNT'||cLF;
        WHEN vSR_Acct_Lvl = 5 THEN
            SET vSQL_Text = vSQL_Text || '    ,HFM_LVL5_PARENT_ACCOUNT'||cLF;
    END CASE;
    SET vSQL_Text = vSQL_Text || '    ,HFM_IMM_PARENT_ACCOUNT AS NODE_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM_ACCOUNT_DESC AS LEAF_ACCT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,IS_LEAF_FLAG AS IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,1 AS DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM '|| vBRLWorkDBName || '.SR_ACCNT_HIER'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE IS_LEAF_FLAG = ''Y'''||cLF;
    CASE
        WHEN vSR_Acct_Lvl = 1 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL1_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 2 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL2_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 3 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL3_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 4 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL4_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 5 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM_LVL5_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
    END CASE;
    SET vSQL_Text = vSQL_Text || ' UNION ALL'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     A.LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.ROOT_PARENT_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,HFM.HFM_IMM_PARENT_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.LEAF_ACCT_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.IS_LEAF_FLAG'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,A.DEPTH + 1'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM LEAF_HFM_ACCOUNTS A'||cLF;
    SET vSQL_Text = vSQL_Text || ' INNER JOIN '|| vBRLWorkDBName || '.SR_ACCNT_HIER HFM'||cLF;
    SET vSQL_Text = vSQL_Text || ' ON A.NODE_ACCT_CD = HFM.HFM_ACCOUNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE A.DEPTH <= 25'||cLF;
    CASE
        WHEN vSR_Acct_Lvl = 1 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL1_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 2 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL2_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 3 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL3_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 4 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL4_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
        WHEN vSR_Acct_Lvl = 5 THEN
            SET vSQL_Text = vSQL_Text || ' AND HFM.HFM_LVL5_PARENT_ACCOUNT = '''||vSR_Acct||''''||cLF;
    END CASE;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  ROOT_PARENT_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  NODE_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  DEPTH'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM LEAF_HFM_ACCOUNTS'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ACCNT_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;
--
-- Includes a row for each leaf mapped to itself with at depth of 0
--
    SET vStepNbr = 14;
    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_ACCNT_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  ROOT_PARENT_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '  IS_LEAF_FLAG,'||cLF;
    SET vSQL_Text = vSQL_Text || '  0'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM '|| vBRLWorkDBName || '.SR_ACCNT_LEAFS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE DEPTH = 1'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_ACCNT_LEAFS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR GL Balance table in BRL layer
--
    SET vStepNbr = 20;
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.INFRC_SR_GL_BAL;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.INFRC_SR_GL_BAL'||cLF;
    SET vSQL_Text = vSQL_Text || '( ENT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BSBCC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ent.ROOT_PARENT_ENTITY AS ENT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BUSINESS_UNIT AS PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.ACCOUNT_1 AS PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.PRODUCT AS PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.AFFILIATE AS PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.DEPTID AS PSGL_BSBCC'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.PROJECT_ID AS PSGL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.CURRENCY_CD AS TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_TRAN_TOTAL) AS TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BASE_CURRENCY AS LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_BASE_TOTAL) AS LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ', gl.BASE_CURRENCY AS RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || ', SUM(gl.POSTED_BASE_TOTAL) AS RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.SR_GL_BAL gl'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ENTITY_LEAFS ent'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON gl.HFM_LCD = ent.LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ACCNT_LEAFS bsh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON gl.HFM_ACCT_CD = bsh.LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE ent.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || '  AND bsh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14'||cLF;
    SET vSQL_Text = vSQL_Text || 'HAVING RPT_AMT <> 0'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR GL Account Association table in BRL layer
--
    SET vStepNbr = 21;
    SET vSQL_Text = 'DELETE FROM '|| vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_ACCT_ASSOC', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' ||vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC'||cLF;
    SET vSQL_Text = vSQL_Text || '('||cLF;
    SET vSQL_Text = vSQL_Text || '   ENT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '   ent.ROOT_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,DT.BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM'||cLF;
    SET vSQL_Text = vSQL_Text || '  ('||cLF;
    SET vSQL_Text = vSQL_Text || '   SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     sbh.SRC_SYS_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.AR_UNQ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.USR_FLD_1 AS BASE_TABLE'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN sbh.VALTN_TY IS NULL OR sbh.VALTN_TY = '''' THEN ''Gross Balance'' ELSE sbh.VALTN_TY END AS VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,COALESCE(amap.SR_ACCT, ''Unknown HFM Account'') AS SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.DEPT_ID AS BSB_CC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,COALESCE(lmap.SR_LCD, ''Unknown HFM LCD'') AS SR_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.GL_AC AS GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.GL_PRD AS GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.AFFIL AS GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.PROJ_ID AS GL_PRJ_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.BSNS_UNT AS GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.CUR_CD AS LCL_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,sbh.BAL_DT'||cLF;
    SET vSQL_Text = vSQL_Text || '   FROM ' || vCNSUMVWDBName || '.SRC_ACC_BAL_HIST sbh'||cLF;
    SET vSQL_Text = vSQL_Text || '   LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP amap'||cLF;
    SET vSQL_Text = vSQL_Text || '   ON sbh.GL_AC = amap.GL_ACCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   LEFT OUTER JOIN ' || vCNSUMVWDBName || '.SR_GL_DEPT_LCD_MAP lmap'||cLF;
    SET vSQL_Text = vSQL_Text || '   ON sbh.BSNS_UNT = lmap.GL_BSSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || '   AND sbh.DEPT_ID = lmap.GL_DEPTID'||cLF;
    SET vSQL_Text = vSQL_Text || '  ) DT'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ENTITY_LEAFS ent'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DT.SR_LCD = ent.LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ACCNT_LEAFS bsh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DT.SR_ACCT_CD = bsh.LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE ent.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || '  AND bsh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_GL_ACCT_ASSOC', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface SR Balance table in BRL layer
--
    SET vStepNbr = 22;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.INFRC_SR_BAL;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLVWDBName || '.INFRC_SR_BAL'||cLF;
    SET vSQL_Text = vSQL_Text || '(  ENT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_YEAR_1'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_MONTH_1'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,HFM_LCD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,TRN_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,TRN_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCY_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,LCY_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,RPT_CUR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,RPT_AMT'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '     lcdh.ROOT_PARENT_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.Year_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.Month_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.ACCOUNT_1'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,tcl.ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,NULL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,NULL'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     = MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END) = ''AUD'''||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     ELSE MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,SUM((CASE WHEN UPPER(act.ACCOUNTTYPE) IN (''ASSET'', ''EXPENSE'') THEN 1000 ELSE -1000 END) * CAST(tcl.Amount AS DECIMAL(36,8)))'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     = MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     WHEN MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END) = ''AUD'''||cLF;
    SET vSQL_Text = vSQL_Text || '     THEN MAX(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     ELSE MIN(CASE WHEN tcl.Currency LIKE ''% Adjs'' THEN SUBSTRING(tcl.Currency FROM 1 FOR POSITION('' Adjs'' IN tcl.Currency)-1) ELSE tcl.Currency END)'||cLF;
    SET vSQL_Text = vSQL_Text || '     END'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,SUM((CASE WHEN tcl.Currency IN (''AUD'', ''AUD Adjs'') THEN (CASE WHEN act.ACCOUNTTYPE IN (''ASSET'', ''EXPENSE'') THEN 1000 ELSE -1000 END) ELSE 0 END) * CAST(tcl.Amount AS DECIMAL(36,8)))'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.HFMZZ_EA_TCL tcl'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_ACCOUNT_TREE act'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ACCOUNT_1 = act.LABEL'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ACCNT_LEAFS acth'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ACCOUNT_1 = acth.LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ENTITY_LEAFS lcdh'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.ENTITY = lcdh.LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_CUSTOM1_HIER C1'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.Custom1 = C1.LEAF_CUSTOM1'||cLF;
    SET vSQL_Text = vSQL_Text || 'OR tcl.Custom1 = ''STAT_ADJ'''||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_CUSTOM3_HIER C3'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON tcl.Custom3 = C3.LEAF_CUSTOM3'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE tcl.Scenario = ''ACTUAL'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom2 = ''EOP'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom4 = ''[None]'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Periodicity = ''YTD'''||cLF;
    SET vSQL_Text = vSQL_Text || '-- AND tcl.Currency IN (''AUD'', ''AUD Adjs'')'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND acth.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND lcdh.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C1.NODE_CUSTOM1 = ''MGMT'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Custom1 <> ''MGMT_ADJ'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C3.NODE_CUSTOM3 = ''TB_TOT'''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Year_1 = (SELECT CASE WHEN month_of_year >= 10 THEN year_of_calendar+1 ELSE year_of_calendar END FROM Sys_Calendar.CALENDAR WHERE calendar_date = DATE ''' || vBssns_Date || ''')'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND tcl.Month_1 = (SELECT b.SR_MTH FROM Sys_Calendar.CALENDAR a INNER JOIN ' || vCNSUMVWDBName || '.SR_MTH b ON a.month_of_year = b.MTH WHERE calendar_date = DATE ''' || vBssns_Date || ''')'||cLF;
    SET vSQL_Text = vSQL_Text || 'GROUP BY 1,2,3,4,5'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'INFRC_SR_BAL', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the HFM PSGL EXPLO Ledger work table in BRL Layer
--
    SET vStepNbr = 23;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_EXPLO_SUBLEDGER', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUSINESS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE)'||cLF;
-- PSGL Entries from INFRC_SR_GL_BAL
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,PSGL_AFFILIATE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_GL_BAL'||cLF;
-- HFM Entries from INFRC_SR_BAL
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   HFM_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,CAST(NULL AS VARCHAR(10))'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_BAL'||cLF;
-- EXPLO Entries from INFRC_SR_GL_ACCT_ASSOC
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   SR_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_BUS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,GL_AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLVWDBName || '.INFRC_SR_GL_ACCT_ASSOC;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_EXPLO_SUBLEDGER', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Start of the Cascading Defaults process
--
-- Populate the HFM Leaf Defaults work table in BRL Layer
--
    SET vStepNbr = 24;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_LEAF_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DF_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION '||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ( SELECT ROW_NUMBER() OVER (PARTITION BY B.LEAF_ACCT_CD ORDER BY CASE WHEN B.LEAF_ACCT_CD = A.HFM_ACCOUNT_CODE THEN 1 ELSE 9 END, B.DEPTH) AS ROW_NR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              B.LEAF_ACCT_CD,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BAL_CLASS,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_DF_PRODUCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.REGULATORY_PRODUCT_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BSS_ITEM_CDE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.BSS_ITEM_DESC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_DC_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SECURITISATION,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_RESECURITISATION,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_ASSET_BACKED,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.PRD_REPO_PRODUCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SETT_CREDIT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_CONVERTIBLE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_TANGIBLE_ASSETS,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_BS_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_INDICATOR,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_NEAR_FAR_INDIC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_ENTRY_TYPE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_PURPOSE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CTY_RESIDENCE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_REVOLVING_FAC,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_CPTY_TYPE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CDE_CONSOLIDATION_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_COLLATERAL,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_PORTFOLIO,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_INTEREST_RATE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CCY_ISO_CURRENCY,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_REMAINING_BKT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.MAT_ORIGINAL_BKT,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.CTY_ISSUE,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_LISTED,'||cLF;
    SET vSQL_Text = vSQL_Text || '              A.TYP_SUBORDINATION'||cLF;
    SET vSQL_Text = vSQL_Text || '      FROM ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS A'||cLF;
    SET vSQL_Text = vSQL_Text || '      INNER JOIN '|| vBRLWorkDBName || '.SR_ACCNT_LEAFS B'||cLF;
    SET vSQL_Text = vSQL_Text || '      ON A.HFM_ACCOUNT_CODE = B.NODE_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || '      WHERE A.PSGL_ACCOUNT_CODE = '''''||cLF;
    SET vSQL_Text = vSQL_Text || '     ) C'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE C.ROW_NR = 1;'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the PSGL Leaf Defaults work table in BRL Layer
--
    SET vStepNbr = 25;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PURPOSE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_RESIDENCE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_REVOLVING_FAC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CPTY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CDE_CONSOLIDATION_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_COLLATERAL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_PORTFOLIO'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_INTEREST_RATE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CCY_ISO_CURRENCY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_REMAINING_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORIGINAL_BKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CTY_ISSUE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_LISTED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT A.SR_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       A.GL_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       CAST(NULL AS VARCHAR(50)),'||cLF;
    SET vSQL_Text = vSQL_Text || '       CAST(NULL AS VARCHAR(50)),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BAL_CLASS, B.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DF_PRODUCT, B.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.REGULATORY_PRODUCT_CODE, B.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_CDE, B.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_DESC, B.BSS_ITEM_DESC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DC_INDICATOR, B.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SECURITISATION, B.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_RESECURITISATION, B.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ASSET_BACKED, B.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.PRD_REPO_PRODUCT, B.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SETT_CREDIT, B.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CONVERTIBLE, B.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_TANGIBLE_ASSETS, B.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_BS_INDICATOR, B.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_INDICATOR, B.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_NEAR_FAR_INDIC, B.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ENTRY_TYPE, B.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PURPOSE, B.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_RESIDENCE, B.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_REVOLVING_FAC, B.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CPTY_TYPE, B.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CDE_CONSOLIDATION_CODE, B.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_COLLATERAL, B.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PORTFOLIO, B.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_INTEREST_RATE, B.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CCY_ISO_CURRENCY, B.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_REMAINING_BKT, B.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_ORIGINAL_BKT, B.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_ISSUE, B.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_LISTED, B.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SUBORDINATION, B.TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP A'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.SR_ACCT = B.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.GL_ACCT = C.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C.PSGL_PRODUCT_CODE = '''''||cLF;
    SET vSQL_Text = vSQL_Text || 'UNION'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT A.SR_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       A.GL_ACCT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       C.PSGL_PRODUCT_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       C.PSGL_AFFILIATE_CODE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BAL_CLASS, B.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DF_PRODUCT, B.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.REGULATORY_PRODUCT_CODE, B.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_CDE, B.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.BSS_ITEM_DESC, B.BSS_ITEM_DESC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_DC_INDICATOR, B.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SECURITISATION, B.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_RESECURITISATION, B.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ASSET_BACKED, B.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.PRD_REPO_PRODUCT, B.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SETT_CREDIT, B.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CONVERTIBLE, B.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_TANGIBLE_ASSETS, B.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_BS_INDICATOR, B.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_INDICATOR, B.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_NEAR_FAR_INDIC, B.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_ENTRY_TYPE, B.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PURPOSE, B.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_RESIDENCE, B.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_REVOLVING_FAC, B.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_CPTY_TYPE, B.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CDE_CONSOLIDATION_CODE, B.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_COLLATERAL, B.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_PORTFOLIO, B.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_INTEREST_RATE, B.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CCY_ISO_CURRENCY, B.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_REMAINING_BKT, B.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.MAT_ORIGINAL_BKT, B.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.CTY_ISSUE, B.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_LISTED, B.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(C.TYP_SUBORDINATION, B.TYP_SUBORDINATION)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP A'||cLF;
    SET vSQL_Text = vSQL_Text || 'JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.SR_ACCT = B.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_HFM_PSGL_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON A.GL_ACCT = C.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE C.PSGL_PRODUCT_CODE <> '''';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_LEAF_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface HFM PSGL Map Defaults table in BRL layer
--   Truncate records for current Business Date
--
    SET vStepNbr = 26;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE BSNS_DT = DATE ''' || vBssns_Date || ''';';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( BSNS_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRD_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_DC_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SCRTZN'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RESCRTIZ'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ASSET_BKCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_REPO_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SETL_CR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CONV'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_TNGBL_ASSET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_SHEET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_MTRTY_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_NEAR_FAR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ENTRY_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRPS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,RES_CNRTY_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RVLV_FCLT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CNTRPRT_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CNSLDT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_COLLAT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PORTF'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INT_RTE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CUR_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_RMN_BCKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_ORGNL_BCKT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,ISS_CNTRY_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_LSTD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INRFC_SUBB'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EFFV_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EXPR_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,USR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DATE ''' || vBssns_Date || ''','||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.HFM_ACCOUNT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_ACCOUNT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_PRODUCT_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_AFFILIATE_CODE, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(DB.PSGL_BUSINESS_UNIT, ''''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.AMOUNT_FIELD, PSGL.AMOUNT_FIELD, HFM.AMOUNT_FIELD),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.PRODUCT_TYPE, PSGL.PRODUCT_TYPE, HFM.PRODUCT_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.REGULATORY_PRODUCT_CODE, PSGL.REGULATORY_PRODUCT_CODE, HFM.REGULATORY_PRODUCT_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_DC_INDICATOR, PSGL.TYP_DC_INDICATOR, HFM.TYP_DC_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SECURITISATION, PSGL.TYP_SECURITISATION, HFM.TYP_SECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_RESECURITISATION, PSGL.TYP_RESECURITISATION, HFM.TYP_RESECURITISATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_ASSET_BACKED, PSGL.TYP_ASSET_BACKED, HFM.TYP_ASSET_BACKED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.PRD_REPO_PRODUCT, PSGL.PRD_REPO_PRODUCT, HFM.PRD_REPO_PRODUCT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SETT_CREDIT, PSGL.TYP_SETT_CREDIT, HFM.TYP_SETT_CREDIT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_CONVERTIBLE, PSGL.TYP_CONVERTIBLE, HFM.TYP_CONVERTIBLE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_TANGIBLE_ASSETS, PSGL.TYP_TANGIBLE_ASSETS, HFM.TYP_TANGIBLE_ASSETS),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_BS_INDICATOR, PSGL.TYP_BS_INDICATOR, HFM.TYP_BS_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_INDICATOR, PSGL.MAT_INDICATOR, HFM.MAT_INDICATOR),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_NEAR_FAR_INDIC, PSGL.TYP_NEAR_FAR_INDIC, HFM.TYP_NEAR_FAR_INDIC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_ENTRY_TYPE, PSGL.TYP_ENTRY_TYPE, HFM.TYP_ENTRY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_PURPOSE, PSGL.TYP_PURPOSE, HFM.TYP_PURPOSE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.CTY_RESIDENCE, PRD.CTY_RESIDENCE, PSGL.CTY_RESIDENCE, HFM.CTY_RESIDENCE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_REVOLVING_FAC, PSGL.TYP_REVOLVING_FAC, HFM.TYP_REVOLVING_FAC),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.TYP_CPTY_TYPE, PRD.TYP_CPTY_TYPE, PSGL.TYP_CPTY_TYPE, HFM.TYP_CPTY_TYPE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(AFF.CDE_CONSOLIDATION_CODE, PRD.CDE_CONSOLIDATION_CODE, PSGL.CDE_CONSOLIDATION_CODE, HFM.CDE_CONSOLIDATION_CODE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_COLLATERAL, PSGL.TYP_COLLATERAL, HFM.TYP_COLLATERAL),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_PORTFOLIO, PSGL.TYP_PORTFOLIO, HFM.TYP_PORTFOLIO),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_INTEREST_RATE, PSGL.TYP_INTEREST_RATE, HFM.TYP_INTEREST_RATE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.CCY_ISO_CURRENCY, PSGL.CCY_ISO_CURRENCY, HFM.CCY_ISO_CURRENCY),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_REMAINING_BKT, PSGL.MAT_REMAINING_BKT, HFM.MAT_REMAINING_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.MAT_ORIGINAL_BKT, PSGL.MAT_ORIGINAL_BKT, HFM.MAT_ORIGINAL_BKT),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.CTY_ISSUE, PSGL.CTY_ISSUE, HFM.CTY_ISSUE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_LISTED, PSGL.TYP_LISTED, HFM.TYP_LISTED),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.TYP_SUBORDINATION, PSGL.TYP_SUBORDINATION, HFM.TYP_SUBORDINATION),'||cLF;
    SET vSQL_Text = vSQL_Text || '       CURRENT_DATE,'||cLF;
    SET vSQL_Text = vSQL_Text || '       DATE ''9999-12-31'','||cLF;
--    SET vSQL_Text = vSQL_Text || '       ''verweyt'','||cLF;
    SET vSQL_Text = vSQL_Text || '       CASE WHEN AFF.AFFILIATE_CODE IS NOT NULL THEN'||cLF;
    SET vSQL_Text = vSQL_Text || '           (CASE WHEN PRD.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL PROD AND AFFIL OVRD MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '                 WHEN PSGL.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL ACCT AND AFFIL OVRD MAP'' ELSE ''HFM MAP AND AFFIL OVRD MAP'' END)'||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN PRD.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL PROD MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN PSGL.PSGL_ACCOUNT_CODE IS NOT NULL THEN ''PSGL ACCT MAP'''||cLF;
    SET vSQL_Text = vSQL_Text || '            WHEN HFM.HFM_ACCOUNT_CODE IS NOT NULL THEN ''HFM MAP'' ELSE ''NOT MAPPED'' END,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.BSS_ITEM_CDE, PSGL.BSS_ITEM_CDE, HFM.BSS_ITEM_CDE),'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(PRD.BSS_ITEM_DESC, PSGL.BSS_ITEM_DESC, HFM.BSS_ITEM_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLWorkDBName || '.SR_GL_EXPLO_SUBLEDGER DB'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_LEAF_DEFAULTS HFM'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = HFM.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = PRD.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_ACCOUNT_CODE = PRD.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_PRODUCT_CODE = PRD.PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_PRODUCT_CODE IS NOT NULL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN '|| vBRLWorkDBName || '.SR_GL_LEAF_DEFAULTS PSGL'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.HFM_ACCOUNT_CODE = PSGL.HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND DB.PSGL_ACCOUNT_CODE = PSGL.PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PSGL.PSGL_PRODUCT_CODE IS NULL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vCNSUMVWDBName || '.MAPRAAU_PSGL_AFF_DEFAULTS AFF'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON DB.PSGL_AFFILIATE_CODE = AFF.AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the HFM PSGL Deal Type Deal Sub Type combination work table in BRL layer
--
    SET vStepNbr = 27;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.SR_GL_PRD;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.SR_GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || '( HFM_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_ACCOUNT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_AFFILIATE_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PSGL_BUSINESS_UNIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRODUCT_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
--    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ')'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(SMAP.SR_ACCT, ''Unknown HFM Account''),'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_AC,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.GL_PRD,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.AFFIL,'||cLF;
    SET vSQL_Text = vSQL_Text || '       LNK.BSNS_UNT,'||cLF;
    SET vSQL_Text = vSQL_Text || '       CASE AR.FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_loan'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_interest_bearing_accounts'' THEN ''DP'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_bond'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_equity'' THEN ''SE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_lease'' THEN ''LN'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_money_market'' THEN ''CA'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_repo_style'' THEN ''RE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_future'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_tfi_trn_option'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_credit_derivatives'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_forex'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_fra'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_futures'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_fx'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_trn_swap_ip'' THEN ''DE'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_facility'' THEN ''LI'''||cLF;
    SET vSQL_Text = vSQL_Text || '          WHEN ''t_fixed_asset'' THEN ''AT'''||cLF;
    SET vSQL_Text = vSQL_Text || '          ELSE '''''||cLF;
    SET vSQL_Text = vSQL_Text || '       END AS Product_Type,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.CNFRM_DEAL_SUB_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_47, '''') AS CHEQUE_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.INFRC_VALTN_TY AS VALTN_TY,'||cLF;
    SET vSQL_Text = vSQL_Text || '       AR.DEBIT_CREDIT_IND,'||cLF;
    SET vSQL_Text = vSQL_Text || '       COALESCE(BRO.CC_VAL_INFRC_NM_50, '''') AS AT_CALL_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLGENVWDBName || '.EFFV_OUT_AR_TO_GL_LNK LNK'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_GL_ACCT_MAP SMAP'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.GL_AC = SMAP.GL_ACCT'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vCNSUMVWDBName || '.SR_GL_DEPT_LCD_MAP LMAP'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.BSNS_UNT = LMAP.GL_BSSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.DEPT_ID = LMAP.GL_DEPTID'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ENTITY_LEAFS ENT'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LMAP.SR_LCD = ENT.LEAF_ENTITY'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN ' || vBRLWorkDBName || '.SR_ACCNT_LEAFS BSH'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON SMAP.SR_ACCT = BSH.LEAF_ACCT_CD'||cLF;
    SET vSQL_Text = vSQL_Text || 'INNER JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ' UNION'||cLF;
    SET vSQL_Text = vSQL_Text || ' SELECT AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,FDA_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CNFRM_DEAL_SUB_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,INFRC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || '    ,CASE WHEN VALTN_AMT < 0 THEN ''C'' ELSE ''D'' END AS DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLVWDBName || '.UNPIVOT_INP_AR_ACCT_VALTN'||cLF;
    SET vSQL_Text = vSQL_Text || ') AR'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = AR.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = AR.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.BAL_TY = AR.BLC_VALTN_TY'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT OUTER JOIN ' || vBRLVWDBName || '.PIVOT_BRO_AR BRO'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON LNK.AR_SRC = BRO.AR_SRC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND LNK.AR_ID = BRO.AR_ID'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE ENT.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || '  AND BSH.IS_LEAF_FLAG = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'SR_GL_PRD', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Product Map Defaults work table in BRL Layer
--   First where the Debit Credit Indicator is not blank
--
    SET vStepNbr = 28;
    SET vSQL_Text = 'DELETE FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS;';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND <> '''';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
--   Next where the Debit Credit Indicator is blank explode for 'C' and 'D' values
--
    SET vStepNbr = 29;
    SET vSQL_Text = 'INSERT INTO ' || vBRLWorkDBName || '.PRD_DEFAULTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REGULATORY_PRODUCT_CODE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_CDE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AMOUNT_FIELD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,TYP_ENTRY_TYPE)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '   dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.OVERRIDE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.REGULATORY_PRODUCT_CODE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_CDE'||cLF;
	SET vSQL_Text = vSQL_Text || '  ,dflt.BSS_ITEM_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.BAL_CLASS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_DC_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_RESECURITISATION'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ASSET_BACKED'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.PRD_REPO_PRODUCT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_SETT_CREDIT'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_CONVERTIBLE'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_TANGIBLE_ASSETS'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_BS_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.MAT_INDICATOR'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_NEAR_FAR_INDIC'||cLF;
    SET vSQL_Text = vSQL_Text || '  ,dflt.TYP_ENTRY_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vCNSUMVWDBName || '.MAPRAAU_PROD_DEFAULTS dflt'||cLF;
    SET vSQL_Text = vSQL_Text || 'CROSS JOIN'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT DEBIT_CREDIT_IND FROM (SELECT ''C'' AS DEBIT_CREDIT_IND) cd1 UNION SELECT DEBIT_CREDIT_IND FROM (SELECT ''D'' AS DEBIT_CREDIT_IND) cd2'||cLF;
    SET vSQL_Text = vSQL_Text || ') cjn_dc'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE dflt.DEBIT_CREDIT_IND = '''''||cLF;
    SET vSQL_Text = vSQL_Text || 'AND NOT EXISTS'||cLF;
    SET vSQL_Text = vSQL_Text || '(SELECT'||cLF;
    SET vSQL_Text = vSQL_Text || '  xx.DEAL_TYPE, xx.DEAL_SUBTYPE, xx.VALUATION_TYPE, xx.DEBIT_CREDIT_IND, xx.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' FROM ' || vBRLWorkDBName || '.PRD_DEFAULTS xx'||cLF;
    SET vSQL_Text = vSQL_Text || ' WHERE xx.DEAL_TYPE = dflt.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEAL_SUBTYPE = dflt.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.VALUATION_TYPE = dflt.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.DEBIT_CREDIT_IND = cjn_dc.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.CHEQUE_IND = dflt.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' AND   xx.AT_CALL_IND = dflt.AT_CALL_IND);'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLWorkDBName, 'PRD_DEFAULTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

--
-- Populate the Interface HFM PSGL Map Defaults table in BRL layer
--   Truncate records for current Business Date
--
    SET vStepNbr = 30;
    SET vSQL_Text = 'DELETE FROM ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || 'WHERE BSNS_DT = DATE ''' || vBssns_Date || ''';';
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    SET vSQL_Text = 'INSERT INTO ' || vBRLVWDBName || '.SR_GL_PRD_MAP_DFLTS'||cLF;
    SET vSQL_Text = vSQL_Text || '( BSNS_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_GRP'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CHQ_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,DR_CR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_PRD_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_PRD_CD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_ID'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,REG_BSS_DESC'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_DC_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SCRTZN'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_RESCRTIZ'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ASSET_BKCD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_REPO_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_SETL_CR'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_CONV'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_TNGBL_ASSET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_BAL_SHEET'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_MTRTY_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_NEAR_FAR_IND'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,INFRC_ENTRY_TY'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EFFV_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,EXPR_DT'||cLF;
    SET vSQL_Text = vSQL_Text || ' ,USR_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || 'SELECT DISTINCT'||cLF;
    SET vSQL_Text = vSQL_Text || '    DATE ''' || vBssns_Date || ''''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.HFM_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_ACCOUNT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_PRODUCT_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_AFFILIATE_CODE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PSGL_BUSINESS_UNIT, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEAL_SUBTYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.CHEQUE_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.DEBIT_CREDIT_IND, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.VALUATION_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.AT_CALL_INDICATOR, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.AMOUNT_FIELD, B.BAL_TY, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(PRD.PRODUCT_TYPE, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.REGULATORY_PRODUCT_CODE, B.REG_PRD_CD, '''')'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_CDE, B.REG_BSS_ID)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.BSS_ITEM_DESC, B.REG_BSS_DESC)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_DC_INDICATOR, B.INFRC_DC_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SECURITISATION, B.INFRC_SCRTZN)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_RESECURITISATION, B.INFRC_RESCRTIZ)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ASSET_BACKED, B.INFRC_ASSET_BKCD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.PRD_REPO_PRODUCT, B.INFRC_REPO_PRD)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_SETT_CREDIT, B.INFRC_SETL_CR)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_CONVERTIBLE, B.INFRC_CONV)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_TANGIBLE_ASSETS, B.INFRC_TNGBL_ASSET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_BS_INDICATOR, B.INFRC_BAL_SHEET)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.MAT_INDICATOR, B.INFRC_MTRTY_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_NEAR_FAR_INDIC, B.INFRC_NEAR_FAR_IND)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,COALESCE(C.TYP_ENTRY_TYPE, B.INFRC_ENTRY_TY)'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CURRENT_DATE'||cLF;
    SET vSQL_Text = vSQL_Text || '   ,DATE ''9999-12-31'''||cLF;
    SET vSQL_Text = vSQL_Text || '   ,CASE WHEN C.DEAL_TYPE IS NOT NULL THEN ''DEAL TYPE MAP OVERRIDE'''||cLF;
    SET vSQL_Text = vSQL_Text || '         WHEN B.SR_AC IS NOT NULL THEN ''HFM PSGL MAP'' ELSE ''NOT MAPPED'' END'||cLF;
    SET vSQL_Text = vSQL_Text || 'FROM ' || vBRLWorkDBName || '.SR_GL_PRD PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLVWDBName || '.SR_GL_MAP_DFLTS B'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.HFM_ACCOUNT_CODE = B.SR_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_ACCOUNT_CODE = B.GL_AC'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_BUSINESS_UNIT = B.BSNS_UNT'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_PRODUCT_CODE = B.GL_PRD'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.PSGL_AFFILIATE_CODE = B.AFFIL'||cLF;
    SET vSQL_Text = vSQL_Text || 'LEFT JOIN ' || vBRLWorkDBName || '.PRD_DEFAULTS C'||cLF;
    SET vSQL_Text = vSQL_Text || 'ON PRD.DEAL_TYPE = C.DEAL_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEAL_SUBTYPE = C.DEAL_SUBTYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.VALUATION_TYPE = C.VALUATION_TYPE'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.CHEQUE_IND = C.CHEQUE_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.DEBIT_CREDIT_IND = C.DEBIT_CREDIT_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND PRD.AT_CALL_INDICATOR = C.AT_CALL_IND'||cLF;
    SET vSQL_Text = vSQL_Text || 'AND C.OVERRIDE_IND = ''Y'''||cLF;
    SET vSQL_Text = vSQL_Text || ';'||cLF;
    CALL QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG(vSQL_Text, vStoredProcName, vSQLStep, vBssns_Date, 'Y', vBRLTDBName, 'SR_GL_PRD_MAP_DFLTS', vExecute_Flag, oSubReturn_Code, oSubReturn_Message);
    IF oSubReturn_Code <> 0 THEN
        SET oReturn_Code = oSubReturn_Code;
        SET oReturn_Message = oSubReturn_Message;
        LEAVE MAIN;
    END IF;
    SET vSQLStep = vSQLStep + 1;

    IF vExecute_Flag = 'Y' THEN
        SET oReturn_Message = 'Create and Load Cascading Defaults for '||vBssns_Date||'. Completed Successfully';
    ELSE
        SET oReturn_Message = 'Create SQL only for '||vBssns_Date||' in T_SQL_Log BRL table. Completed Successfully';
    END IF;

    SET oReturn_Code = 0;

END MAIN;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRL_EXEC_AND_LOG
-- =============================================
-- Description: This procedure executes and Logs Dynamic SQL. Also Collects Stats
-- Change log
--      [2016 06 23]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iSQL_Txt                   VARCHAR(16384),
 IN iStoredProcName            VARCHAR(128),
 INOUT ioSQLStep               INTEGER,
 IN iBssns_Date                CHAR(10),
 IN iClctSts_Flag              CHAR(1),
 IN iClctSts_DB_Nm             VARCHAR(128),
 IN iClctSts_Table_Nm          VARCHAR(128),
 IN iExecution_Flag            CHAR(1),
--
-- Possible Values
--   Y  Execute Transform View SQL. Capture SQL in V_SQL_Log.
--   N  Do not Execute Transform View SQL. Capture SQL in V_SQL_Log.
--
 OUT oReturn_Code              SMALLINT,             /* 0: Successful; 1: Error */
 OUT oReturn_Message           VARCHAR(1000)
)
MAIN:
BEGIN
--
---------------------------------------------------------------------------------------------
-- Variables declaration
---------------------------------------------------------------------------------------------       
--
-- Declare Constants
--
DECLARE vBRLVWDBName                VARCHAR(128) DEFAULT 'QSIT_APRA2_BRL_RRP_VW';
DECLARE vStoredProcName             VARCHAR(128) DEFAULT 'BRL_EXEC_AND_LOG';
DECLARE vORplChar                   INTEGER DEFAULT 7000;                           -- Block size for OREPLACE to process
DECLARE vNbrQtes                    INTEGER DEFAULT 500;                            -- Number of consecutive single quotes cannot exceed
DECLARE vNbrBlks                    INTEGER DEFAULT 3;                              -- Number of Blocks before exceeding 16K SQL_Txt size
DECLARE cLF                         CHAR(2) DEFAULT '0A'XC;
--
--
-- Declare variables
--
DECLARE vSQL_Log_StartTS            VARCHAR(26);
DECLARE vSQL_Log_EndTS              VARCHAR(26);
DECLARE vCtr1                       INTEGER;
DECLARE vCtr2                       INTEGER;
DECLARE vPos1                       INTEGER;
DECLARE vSQL_SubTxt                 VARCHAR(8000);
DECLARE vSQL_OTxt                   VARCHAR(16384) DEFAULT '';
DECLARE vSQL_Msg                    VARCHAR(16384) DEFAULT '';
--
-- Declare Error Handler variables
--
DECLARE vSQL_Code                   INTEGER;
DECLARE vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE vError_Text                 VARCHAR(256);
--
-- Error Handler
--
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
--
-- Preserve Diagnostic Codes from errored Statement
--
		SET vSQL_Code  = SQLCODE;
		SET vSQL_State = SQLSTATE;
		GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
		SET oReturn_Code = 1;
		SET oReturn_Message = 'Stored Procedure = '||vStoredProcName||', SQL Step Nbr = '||TRIM(ioSQLStep)||cLF||'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	END
	;
--
-- OREPLACE has 8000 limit for VARCHAR
--   Loop through blocks of 7000 characters
--   Ensure each block of characters does not end on a single quote
--      vPos1 is the position in iSQL_Txt during Outer Loop
--      vCtr1 is the Outer Loop iteration counter (OREPLACE Blocks)
--      vCtr2 is the Inner Loop iteration counter (consequetive single quotes)
--
    IF CHARS(iSQL_Txt) > vORplChar THEN
        SET vCtr1 = 1;
        SET vPos1 = 0;
        SET vSQL_OTxt = '';
        L1: LOOP
            SET vCtr2 = 0;
            L2: LOOP
                IF SUBSTRING(iSQL_Txt FROM vPos1 + vORplChar - vCtr2 FOR 1) = '''' THEN
                    SET vCtr2 = vCtr2 + 1;
                ELSE
                    SET vSQL_SubTxt = SUBSTRING(iSQL_Txt FROM vPos1 FOR vORplChar - vCtr2);
                    SET vPos1 = vPos1 + vORplChar - vCtr2 + 1;
                    LEAVE L2;
                END IF;
                IF vCtr2 > vNbrQtes THEN
                    SET oReturn_Code = 1;
                    SET oReturn_Message = 'SQL Step Nbr = '||TRIM(CAST(ioSQLStep AS VARCHAR(5)))||'. SQL Text has over 500 quotes!!!!';
                    LEAVE MAIN;
                END IF;
            END LOOP L2;
            SET vSQL_OTxt = vSQL_OTxt||OREPLACE(vSQL_SubTxt, '''', '''''');
            SET vCtr1 = vCtr1 + 1;
            IF SUBSTRING(iSQL_Txt FROM vPos1 FOR vORplChar) = '' THEN
                LEAVE L1;
            END IF;
            IF vCtr1 > vNbrBlks THEN
                SET oReturn_Code = 1;
                SET oReturn_Message = 'SQL Step Nbr = '||TRIM(CAST(ioSQLStep AS VARCHAR(5)))||'. SQL Text has over 16,384 Bytes!!!!';
                LEAVE MAIN;
            END IF;
        END LOOP L1;
    ELSE
        SET vSQL_OTxt = OREPLACE(iSQL_Txt, '''', '''''');
    END IF;
--
SET oReturn_Code = 0;
SET oReturn_Message = '';
--
-- Execute the SQL
--  and save the SQL in V_SQL_Log Table in BRL
--
    SET vSQL_Log_StartTS = CAST((CURRENT_TIMESTAMP(6) (FORMAT 'YYYY-MM-DDbHH:MI:SSDS(6)')) AS CHAR(26));
    SET vSQL_Msg = 'INSERT INTO '||vBRLVWDBName||'.V_SQL_Log VALUES(''' || iStoredProcName || ''', ' || ioSQLStep || ', ''' || vSQL_OTxt || ''', DATE ''' || iBssns_Date || ''', TIMESTAMP ''' || vSQL_Log_StartTS || ''', NULL)';
    EXECUTE IMMEDIATE vSQL_Msg;
    IF iExecution_Flag = 'Y' THEN
        EXECUTE IMMEDIATE iSQL_Txt;
    END IF;
    SET vSQL_Log_EndTS = CAST((CURRENT_TIMESTAMP(6) (FORMAT 'YYYY-MM-DDbHH:MI:SSDS(6)')) AS CHAR(26));
    SET vSQL_Msg = 'UPDATE '||vBRLVWDBName||'.V_SQL_Log SET SQL_Log_EndTimeStamp = TIMESTAMP '''||vSQL_Log_EndTS||''' WHERE SP_Name = '''||iStoredProcName||''' AND Bssns_Date = DATE ''' || iBssns_Date || ''' AND SQL_Step = ' || ioSQLStep;
    EXECUTE IMMEDIATE vSQL_Msg;
--
-- Check if Collecting Statistics
--  and save the SQL in SQL Log Table in BRL
--
    IF iClctSts_Flag = 'Y' THEN
        SET vSQL_OTxt = 'COLLECT STATISTICS ON ' || iClctSts_DB_Nm || '.' || iClctSts_Table_Nm || ';';
        SET ioSQLStep = ioSQLStep + 1;
        SET vSQL_Log_StartTS = CAST((CURRENT_TIMESTAMP(6) (FORMAT 'YYYY-MM-DDbHH:MI:SSDS(6)')) AS CHAR(26));
        SET vSQL_Msg = 'INSERT INTO '||vBRLVWDBName||'.V_SQL_Log VALUES(''' || iStoredProcName || ''', ' || ioSQLStep || ', ''' || vSQL_OTxt || ''', DATE ''' || iBssns_Date || ''', TIMESTAMP ''' || vSQL_Log_StartTS || ''', NULL)';
        EXECUTE IMMEDIATE vSQL_Msg;
        IF iExecution_Flag = 'Y' THEN
            EXECUTE IMMEDIATE vSQL_OTxt;
        END IF;
        SET vSQL_Log_EndTS = CAST((CURRENT_TIMESTAMP(6) (FORMAT 'YYYY-MM-DDbHH:MI:SSDS(6)')) AS CHAR(26));
        SET vSQL_Msg = 'UPDATE '||vBRLVWDBName||'.V_SQL_Log SET SQL_Log_EndTimeStamp = TIMESTAMP '''||vSQL_Log_EndTS||''' WHERE SP_Name = '''||iStoredProcName||''' AND Bssns_Date = DATE ''' || iBssns_Date || ''' AND SQL_Step = ' || ioSQLStep;
        EXECUTE IMMEDIATE vSQL_Msg;
    END IF;

END MAIN;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRL_GENERATE_METADATA 
( 
 IN I_V_SCHEMANAME VARCHAR(128)
,IN I_V_PATTERN VARCHAR(128)
,IN I_V_EFFV_DT DATE
,OUT O_I_RETURNSTATUS INTEGER
,OUT O_V_RETURNMESSAGE VARCHAR(2000)
)
-- =============================================
-- DESCRIPTION: THIS PROCEDURE WILL GENERATE THE BSNS RULES METADATA (ENTITY & ATTRIBUTE MASTER)
-- CHANGE LOG
--      [2015 01 27]: INITIAL VERSION 
--  	[2015 01 29] : BUG FIX - ADDED ENTITY ID JOIN IN ATTR MASTER CURSOR
-- =============================================
-- STORED PROCEDURE PARAMETERS
MAIN:
BEGIN	
DECLARE	V_PATTERN VARCHAR(33);
DECLARE	I_COUNT INTEGER;
DECLARE	I_INSERTCOUNT INTEGER;
DECLARE VEFFV_DT VARCHAR(10);
DECLARE	V_RETURNMESSAGE VARCHAR(2000);
DECLARE VCOUNT INTEGER;
DECLARE	VCNTR 							INTEGER DEFAULT 100;
DECLARE	VLOGMSG 						VARCHAR(1000);
DECLARE	VLOGMSGVARIABLE 				VARCHAR(40000);
DECLARE	VDEBUGLVL 						SMALLINT DEFAULT 5; -- 5 = VERBOSE
DECLARE	VLOGSPNAME 						VARCHAR(255) DEFAULT 'BRL_GENERATE_METADATA';
DECLARE	OSUBRETURN_CODE                	SMALLINT;
DECLARE OSUBRETURN_MESSAGE         		VARCHAR(1000);

SELECT COUNT(*)
INTO :VCOUNT
FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR
WHERE PRMTR_VAL = I_V_SCHEMANAME
AND PRMTR_NM LIKE '%DB_NM';

IF VCOUNT = 0 THEN
SET VCNTR = VCNTR + 1; -- INCREASE STEP NUMBER BY 1
SET	VLOGMSGVARIABLE = 'PROCESS - FAILED. INVALID SCHEMA NAME SUBMITTED = ' ||I_V_SCHEMANAME;
SET VLOGMSG = VLOGMSGVARIABLE;
SET	VDEBUGLVL = 1;
CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (VLOGSPNAME, VCNTR, VLOGMSG, VDEBUGLVL, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);
SET O_I_RETURNSTATUS = 2  ;
SET O_V_RETURNMESSAGE = VLOGMSG;		
LEAVE MAIN;
END IF;	


IF I_V_SCHEMANAME = 'QSIT_APRA2_CNSUM_RRP_GEN_VW' THEN
	SET V_PATTERN = 'OUT'||'%';
ELSE
	SET	V_PATTERN = UPPER(COALESCE(I_V_PATTERN,'') || '%');
END IF;

SET	I_COUNT = 100;
SET	I_INSERTCOUNT = 0;
SET VEFFV_DT = CAST(CAST (I_V_EFFV_DT AS DATE FORMAT 'YYYY-MM-DD') AS VARCHAR(30));
	
SELECT	COALESCE(MAX(ENT_ID), 0) INTO I_COUNT 
FROM	 QSIT_APRA2_BRL_RRP_VW.ENT_MSTR;

L1: 
FOR	CSR1 AS 

SEL	TABLENAME 
FROM	DBC.TABLESV D 
LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR
	ON	TABLENAME=ENT_NM
WHERE	DATABASENAME  = I_V_SCHEMANAME
	AND	UPPER(TABLENAME) LIKE V_PATTERN
	AND	TABLEKIND IN ('T','V')
	AND	ENT_NM IS NULL 
ORDER	BY 1

DO
----------

SET	I_COUNT = I_COUNT + 1;

INSERT	INTO QSIT_APRA2_BRL_RRP_VW.ENT_MSTR (ENT_ID, ENT_NM,SCHM_NM, EFFV_DT, EXPR_DT, USR_ID) 
VALUES	(I_COUNT, CSR1.TABLENAME,I_V_SCHEMANAME, VEFFV_DT (DATE), '9999-12-31' (DATE), USER);

SET	I_INSERTCOUNT = I_INSERTCOUNT + 1;
----------
END FOR	L1;


SET	V_RETURNMESSAGE = 'NO OF ROWS INSERTED INTO ENT_MSTR: '|| TRIM(I_INSERTCOUNT);
SET	I_INSERTCOUNT = 0;

SELECT COALESCE(MAX(ATTR_ID), 0) INTO I_COUNT 
FROM	 QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR;

L2: 
FOR	CSR2 AS 


SELECT	ENT.ENT_ID, COL.COLUMNNAME
FROM	DBC.COLUMNSV COL
INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR ENT
	ON	COL.TABLENAME = ENT.ENT_NM
LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR ATTRIB
	ON	ATTRIB.ATTR_NM = COL.COLUMNNAME
	AND	ATTRIB.ENT_ID = ENT.ENT_ID
WHERE	 1=1
	AND	COL.DATABASENAME = I_V_SCHEMANAME
	AND	ATTRIB.ATTR_ID IS NULL
	AND COL.COLUMNNAME NOT IN ('EFFV_DT','EXPR_DT','USR_ID')
	AND COL.COLUMNNAME NOT LIKE '%CSTM%'  -- A SEPARATE SECTION HAS BEEN ADDED BELOW TO HANDLE CUSTOM FIELD
ORDER	BY 1,2


DO
----------

SET	I_COUNT = I_COUNT + 1;

INSERT	INTO QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR (ATTR_ID, ENT_ID, ATTR_NM, EFFV_DT, EXPR_DT, USR_ID) 
VALUES	(I_COUNT, CSR2.ENT_ID, CSR2.COLUMNNAME, VEFFV_DT (DATE), '9999-12-31' (DATE), USER);

SET	I_INSERTCOUNT = I_INSERTCOUNT + 1;
----------
END FOR	L2;

SET	V_RETURNMESSAGE = V_RETURNMESSAGE || '; NO OF ROWS INSERTED INTO ATTR_MSTR: '|| TRIM(I_INSERTCOUNT);
SET	O_I_RETURNSTATUS = 0;

---------- DELETE ORPHAN RECORDS FROM ATTR_MSTR

DELETE FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR
WHERE (ATTR_ID, ENT_ID) IN (											
											SELECT	ATTRIB.ATTR_ID, ATTRIB.ENT_ID
											FROM	QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR ATTRIB
											LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR ENT
												ON ATTRIB.ENT_ID = ENT.ENT_ID
											LEFT OUTER JOIN DBC.COLUMNSV COL
												ON	ATTRIB.ATTR_NM = COL.COLUMNNAME
												AND	COL.TABLENAME = ENT.ENT_NM
												AND	COL.DATABASENAME = I_V_SCHEMANAME
											WHERE	 1=1
												AND ATTRIB.ATTR_NM  NOT IN ('EFFV_DT','EXPR_DT','USR_ID')
												AND COL.COLUMNNAME IS NULL )
AND ENT_ID IN ( SELECT ENT.ENT_ID FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR ENT WHERE ENT.SCHM_NM = I_V_SCHEMANAME);

--SET I_INSERTCOUNT = ACTIVITY_COUNT;
SET	V_RETURNMESSAGE = V_RETURNMESSAGE || '; NO OF ORPHAN ROWS DELETED FROM ATTR_MSTR: '||TRIM(COALESCE (ACTIVITY_COUNT,0));
SET	O_I_RETURNSTATUS = 0;

---------- DELETE ORPHAN RECORDS FROM ENT_MSTR

DELETE FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR 
WHERE ENT_ID IN (
									SELECT	ENT_ID
									FROM	 QSIT_APRA2_BRL_RRP_VW.ENT_MSTR
									LEFT OUTER JOIN DBC.TABLESV D 
										ON	TABLENAME=ENT_NM
									    AND 	DATABASENAME  = I_V_SCHEMANAME
									    AND	TABLEKIND IN ('T','V')
									WHERE D.TABLENAME IS NULL )
AND SCHM_NM = I_V_SCHEMANAME;

--SET I_INSERTCOUNT = ACTIVITY_COUNT;
SET	V_RETURNMESSAGE = V_RETURNMESSAGE || '; NO OF ORPHAN ROWS DELETED FROM ENT_MSTR: '||TRIM(COALESCE (ACTIVITY_COUNT,0));
SET	O_I_RETURNSTATUS = 0;

-- UPDATE BUSINESS ATTRIBUTE NAME 
UPDATE QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR SET BSNS_ATTR_NM = OREPLACE(ATTR_NM,'_',' ')
WHERE BSNS_ATTR_NM IS NULL AND ATTR_NM IS NOT NULL;

SET	V_RETURNMESSAGE = V_RETURNMESSAGE || '; NO OF BUSINESS ATTRIBUTE NAME UPDATED: '||TRIM(COALESCE (ACTIVITY_COUNT,0));
SET	O_I_RETURNSTATUS = 0;

-- UPDATE BUSINESS ENTITY NAME 
UPDATE QSIT_APRA2_BRL_RRP_VW.ENT_MSTR SET BSNS_ENT_NM = OREPLACE(ENT_NM,'_',' ')
WHERE BSNS_ENT_NM IS NULL AND ENT_NM IS NOT NULL;

SET	V_RETURNMESSAGE = V_RETURNMESSAGE || '; NO OF BUSINESS ENTITY NAME UPDATED: '||TRIM(COALESCE (ACTIVITY_COUNT,0));
SET	O_I_RETURNSTATUS = 0;

-- SECTION FOR CUSTOM FIELDS 

-- DELETE ORPHAN CSTM FIELDS 
DELETE FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR 
WHERE ATTR_NM LIKE '%CSTM%' 
AND (BSNS_ATTR_NM, ENT_ID) NOT IN (SELECT BSNS_ATTR_NM, ENT_ID 
																	FROM 
																	--- CSTM LOGIC STARTS
																	(SELECT AM_MAX.MAX_ID + ROW_NUMBER() OVER (ORDER BY ATTR_NM) AS ATTR_ID
																	,EM.ENT_ID AS ENT_ID
																	,OREPLACE (BRL.COL_MAP_TGT_COL, '_MDID', '') AS ATTR_NM
																	,CL.COL_MAP_FMT AS BSNS_ATTR_NM
																	,NULL AS DATA_TY
																	,NULL AS IS_PK
																	,VEFFV_DT (DATE) AS EFFV_DT
																	,'9999-12-31' (DATE) AS EXPR_DT
																	,USER AS USR_ID

																	FROM (
																	SELECT COL_MAP_SRC_TBL, COL_MAP_TGT_TBL, COL_MAP_TGT_COL, COL_MAP_SRC_COL, COL_MAP_FMT 
																	FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_COLUMN_MAPPING
																	GROUP BY 1, 2, 3, 4, 5
																	) BRL

																	INNER JOIN (
																	SELECT COL_MAP_SRC_TBL, COL_MAP_TGT_TBL, COL_MAP_TGT_COL, COL_MAP_SRC_COL, COL_MAP_FMT 
																	FROM QSIT_APRA2_BRL_RRP_VW.CLMD_COLUMN_MAPPING
																	WHERE COL_MAP_TGT_COL LIKE '%CSTM%MDID'
																	AND COL_MAP_TYPE = 'STATICVAL'
																	GROUP BY 1, 2, 3, 4, 5
																	) CL
																	ON BRL.COL_MAP_SRC_TBL = CL.COL_MAP_TGT_TBL
																	AND BRL.COL_MAP_SRC_COL = CL.COL_MAP_TGT_COL

																	CROSS JOIN (SELECT MAX(ATTR_ID) AS MAX_ID FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR) AM_MAX

																	INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR EM
																	ON EM.ENT_NM = BRL.COL_MAP_TGT_TBL ) A --- CSTM LOGIC ENDS
															GROUP BY 1, 2
);

SET	V_RETURNMESSAGE = V_RETURNMESSAGE || '; NO OF ORPHAN CSTM FIELD ROWS DELETED FROM ATTR_MSTR: '||TRIM(COALESCE (ACTIVITY_COUNT,0));
SET	O_I_RETURNSTATUS = 0;

-- INSERT NEW CSTM FIELDS THAT DOESN'T ALREADY EXIST 
INSERT INTO QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR (ATTR_ID, ENT_ID, ATTR_NM, BSNS_ATTR_NM, DATA_TY, IS_PK, EFFV_DT, EXPR_DT, USR_ID)
SELECT ATTR_ID, ENT_ID, ATTR_NM, BSNS_ATTR_NM, DATA_TY, IS_PK,EFFV_DT, EXPR_DT, USR_ID 
-- NOTE: CSTM LOGIC IN THIS SECTION IS EXACTLY SAME AS ABOVE
FROM --- CSTM LOGIC STARTS		
																(SELECT AM_MAX.MAX_ID + ROW_NUMBER() OVER (ORDER BY ATTR_NM) AS ATTR_ID
																,EM.ENT_ID AS ENT_ID
																,OREPLACE (BRL.COL_MAP_TGT_COL, '_MDID', '') AS ATTR_NM
																,CL.COL_MAP_FMT AS BSNS_ATTR_NM
																,NULL AS DATA_TY
																,NULL AS IS_PK
																,VEFFV_DT (DATE) AS EFFV_DT
																,'9999-12-31' (DATE) AS EXPR_DT
																,USER AS USR_ID

																FROM (
																SELECT COL_MAP_SRC_TBL, COL_MAP_TGT_TBL, COL_MAP_TGT_COL, COL_MAP_SRC_COL, COL_MAP_FMT 
																FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_COLUMN_MAPPING
																GROUP BY 1, 2, 3, 4, 5
																) BRL

																INNER JOIN (
																SELECT COL_MAP_SRC_TBL, COL_MAP_TGT_TBL, COL_MAP_TGT_COL, COL_MAP_SRC_COL, COL_MAP_FMT 
																FROM QSIT_APRA2_BRL_RRP_VW.CLMD_COLUMN_MAPPING
																WHERE COL_MAP_TGT_COL LIKE '%CSTM%MDID'
																AND COL_MAP_TYPE = 'STATICVAL'
																GROUP BY 1, 2, 3, 4, 5
																) CL
																ON BRL.COL_MAP_SRC_TBL = CL.COL_MAP_TGT_TBL
																AND BRL.COL_MAP_SRC_COL = CL.COL_MAP_TGT_COL

																CROSS JOIN (SELECT MAX(ATTR_ID) AS MAX_ID FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR) AM_MAX

																INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR EM
																ON EM.ENT_NM = BRL.COL_MAP_TGT_TBL ) A --- CSTM LOGIC ENDS

WHERE  (BSNS_ATTR_NM, ENT_ID) NOT IN (SELECT BSNS_ATTR_NM, ENT_ID FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR )
AND BSNS_ATTR_NM IS NOT NULL 
AND BSNS_ATTR_NM <> '';

SET	O_V_RETURNMESSAGE = V_RETURNMESSAGE || '; NO OF CSTM FIELD ROWS INSERTED IN ATTR_MSTR: '||TRIM(COALESCE (ACTIVITY_COUNT,0));
SET	O_I_RETURNSTATUS = 0;

END;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_FINAL_PROC
-- =============================================
-- AUTHOR             :               PRALHAD KAMATH
-- CREATE DATE   :               23  DEC  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- [15 Mar 2016]: 1. Enhanced the error message to have more specific information in case of failure. 
--                          2. Modified the condition to check for any failure condition to AND from OR

--[7th APR 2016]: Added Validation sub procedure to verify all the attributes of  column_mapping table
--								exists in dbc.tables and vice versa.
--[12th APR 2016]: Optimize the inline view to select only required columns and not all.
-- =============================================
-- Stored Procedure Parameters
 (IN vi_Name VARCHAR (32), 
 IN iExec_Mode CHAR(1),
OUT vo_qryStr VARCHAR(50000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL */
OUT ORETURN_MESSAGE                VARCHAR(250))
MAIN:
 BEGIN
 
 -- Declare variables
 DECLARE output_vld VARCHAR(10000);
 DECLARE output_select VARCHAR(10000);
 DECLARE output_From VARCHAR(10000);
 DECLARE output_where VARCHAR(10000);
 DECLARE v_qryStr VARCHAR(50000);
 
 DECLARE return_code_exp                SMALLINT;
 DECLARE return_message_exp          VARCHAR(1000);
 
 
 DECLARE return_code_vld                SMALLINT;
 DECLARE return_message_vld         VARCHAR(1000);
  
 DECLARE return_code_select                 SMALLINT;
 DECLARE return_message_select          VARCHAR(1000);
 
 DECLARE return_code_from                   SMALLINT;
 DECLARE return_message_from           VARCHAR(1000);
 
 DECLARE return_code_where                 SMALLINT;
 DECLARE return_message_where          VARCHAR(1000);
 
 
 
 DECLARE	EOLStr VARCHAR(5);
 
 
 SET EOLStr ='
';
 
 --CALLING SUB PROCEDURES
 
 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_EXPAND_COL(vi_Name, iExec_Mode, return_code_exp, return_message_exp);
 IF return_code_select <> 0 THEN
    SET oReturn_Code = return_code_exp;
    SET oReturn_Message = 'Expand Portion: ' || return_message_exp;
    LEAVE MAIN;
END IF;

 --CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_VLD(vi_Name, output_vld,return_code_vld, return_message_vld);
 IF return_code_vld <> 0 THEN
    SET oReturn_Code = return_code_vld;
    SET oReturn_Message = 'Validation Portion: ' || return_message_vld;
    LEAVE MAIN;
END IF;

 
 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_SELECT(vi_Name, output_select, return_code_select, return_message_select);
 IF return_code_select <> 0 THEN
    SET oReturn_Code = return_code_select;
    SET oReturn_Message = 'SELECT Portion: ' || return_message_select;
    LEAVE MAIN;
END IF;

 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_FROM(vi_Name, output_From,return_code_from, return_message_from);
  IF return_code_from <> 0 THEN
    SET oReturn_Code = return_code_from;
    SET oReturn_Message = 'FROM Portion: ' || return_message_from;
    LEAVE MAIN;
END IF;
 
 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_WHERE(vi_Name, output_where,return_code_where, return_message_where);
  IF return_code_where <> 0 THEN
    SET oReturn_Code = return_code_where;
    SET oReturn_Message = 'WHERE Portion: ' || return_message_where;
    LEAVE MAIN;
END IF;
 
    IF (return_code_select =0 AND return_code_from = 0 AND return_code_where = 0)
    THEN
	SET v_qryStr = output_select ||EOLStr || output_From || EOLStr ||output_where;
	SET vo_qryStr = v_qryStr;
	
	IF iExec_Mode = 'Y' THEN 
		EXECUTE IMMEDIATE v_qryStr;
		SET oReturn_Code = 0;
		SET oReturn_Message='Success - Executed the generated SQL. Please verify that View has been created.';
		LEAVE MAIN;
	END IF;
	SET oReturn_Code = 0;
	SET oReturn_Message='Success - Did not execute the generated SQL';
	ELSE
	SET oReturn_Code = 5;
	SET oReturn_Message= 'SELECT Portion: ' || COALESCE(return_message_select, '') || EOLStr || 'FROM Portion: ' || COALESCE(return_message_from, '') || EOLStr || 'WHERE Portion: ' || COALESCE (return_message_where, '');
    END IF;  
  	
END;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_FINAL_PROC_QSIT
-- =============================================
-- AUTHOR             :               PRALHAD KAMATH
-- CREATE DATE   :               23  DEC  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- [15 Mar 2016]: 1. Enhanced the error message to have more specific information in case of failure. 
--                          2. Modified the condition to check for any failure condition to AND from OR

--[7th APR 2016]: Added Validation sub procedure to verify all the attributes of  column_mapping table
--								exists in dbc.tables and vice versa.
--[12th APR 2016]: Optimize the inline view to select only required columns and not all.
-- =============================================
-- Stored Procedure Parameters
 (IN vi_Name VARCHAR (32), 
 IN iExec_Mode CHAR(1),
OUT vo_qryStr VARCHAR(50000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL */
OUT ORETURN_MESSAGE                VARCHAR(250))
MAIN:
 BEGIN
 
 -- Declare variables
 DECLARE output_vld VARCHAR(10000);
 DECLARE output_select VARCHAR(10000);
 DECLARE output_From VARCHAR(10000);
 DECLARE output_where VARCHAR(10000);
 DECLARE v_qryStr VARCHAR(50000);
 
 DECLARE return_code_exp                SMALLINT;
 DECLARE return_message_exp          VARCHAR(1000);
 
 
 DECLARE return_code_vld                SMALLINT;
 DECLARE return_message_vld         VARCHAR(1000);
  
 DECLARE return_code_select                 SMALLINT;
 DECLARE return_message_select          VARCHAR(1000);
 
 DECLARE return_code_from                   SMALLINT;
 DECLARE return_message_from           VARCHAR(1000);
 
 DECLARE return_code_where                 SMALLINT;
 DECLARE return_message_where          VARCHAR(1000);
 
 
 
 DECLARE	EOLStr VARCHAR(5);
 
 
 SET EOLStr ='
';
 
 --CALLING SUB PROCEDURES
 
 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_EXPAND_COL_QSIT(vi_Name, iExec_Mode, return_code_exp, return_message_exp);
 IF return_code_select <> 0 THEN
    SET oReturn_Code = return_code_exp;
    SET oReturn_Message = 'Expand Portion: ' || return_message_exp;
    LEAVE MAIN;
END IF;

 --CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_VLD(vi_Name, output_vld,return_code_vld, return_message_vld);
 IF return_code_vld <> 0 THEN
    SET oReturn_Code = return_code_vld;
    SET oReturn_Message = 'Validation Portion: ' || return_message_vld;
    LEAVE MAIN;
END IF;

 
 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_SELECT_QSIT(vi_Name, output_select, return_code_select, return_message_select);
 IF return_code_select <> 0 THEN
    SET oReturn_Code = return_code_select;
    SET oReturn_Message = 'SELECT Portion: ' || return_message_select;
    LEAVE MAIN;
END IF;

 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_FROM_QSIT(vi_Name, output_From,return_code_from, return_message_from);
  IF return_code_from <> 0 THEN
    SET oReturn_Code = return_code_from;
    SET oReturn_Message = 'FROM Portion: ' || return_message_from;
    LEAVE MAIN;
END IF;
 
 
 CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_WHERE_QSIT(NULL, NULL, output_where,return_code_where, return_message_where);
  IF return_code_where <> 0 THEN
    SET oReturn_Code = return_code_where;
    SET oReturn_Message = 'WHERE Portion: ' || return_message_where;
    LEAVE MAIN;
END IF;
 
    IF (return_code_select =0 AND return_code_from = 0 AND return_code_where = 0)
    THEN
	SET v_qryStr = output_select ||EOLStr || output_From || EOLStr ||output_where||';';
	SET vo_qryStr = v_qryStr;
	
	IF iExec_Mode = 'Y' THEN 
		EXECUTE IMMEDIATE v_qryStr;
		SET oReturn_Code = 0;
		SET oReturn_Message='Success - Executed the generated SQL. Please verify that View has been created.';
		LEAVE MAIN;
	END IF;
	SET oReturn_Code = 0;
	SET oReturn_Message='Success - Did not execute the generated SQL';
	ELSE
	SET oReturn_Code = 5;
	SET oReturn_Message= 'SELECT Portion: ' || COALESCE(return_message_select, '') || EOLStr || 'FROM Portion: ' || COALESCE(return_message_from, '') || EOLStr || 'WHERE Portion: ' || COALESCE (return_message_where, '');
    END IF;  
  	
END;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_EXPAND_COL
-- =============================================
-- Description: This Stored Procedure will generate/ derive the attributes for CLMD_COLUMN_MAPPING table.
-- Change log
-- [2016 06 08]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
IN vname 				VARCHAR (32), 
IN iExec_Mode VARCHAR(1),
OUT ORETURN_CODE        SMALLINT,             /* 0: SUCCESSFUL */
OUT ORETURN_MESSAGE     VARCHAR(250)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF 				CHAR(2) 		DEFAULT '0A'XC;
DECLARE vCntr 				INTEGER 		DEFAULT 100;
DECLARE vActivity_Ct 		INTEGER 		DEFAULT -99;
DECLARE vSQL_Ins 			VARCHAR(10000) 	DEFAULT '';
DECLARE vSQL_Del 			VARCHAR(10000) 	DEFAULT '';
DECLARE vSQL_Code 			INTEGER;
DECLARE vSQL_State 			VARCHAR(6);
DECLARE vError_Text 		VARCHAR(256);
DECLARE oSubReturn_Code 	SMALLINT;
DECLARE oSubReturn_Message 	VARCHAR(1000);
DECLARE vDebugLvl 			SMALLINT 		DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg 			VARCHAR(10000);
DECLARE vLogSPName 			VARCHAR(255) 	DEFAULT 'BRLMD_RTN_EXPAND_COL';
DECLARE vPhy_Tbl_Nm			VARCHAR(256);
DECLARE viname				VARCHAR(256);
DECLARE vTab_Alias			VARCHAR(256);
DECLARE vATTR_NM			VARCHAR(256);
DECLARE vCol_Map_Tgt_Tbl	VARCHAR(256);
DECLARE vCol_Map_Version	VARCHAR(256);


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';

SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET vSQL_Ins = '';
SET vSQL_Del = '';

--LOGIC OF SELECT STATEMENT
L1: 
FOR	CSR1 AS 

SELECT
DISTINCT TM.Phy_Tbl_Nm AS Phy_Tbl_Nm, 
TM.Tab_Map_Src_Tbl AS Tab_Alias ,
BCM.Col_Map_Tgt_Tbl AS Col_Map_Tgt_Tbl,
Col_Map_Version AS Col_Map_Version 
FROM QSIT_APRA2_BRL_RRP_VW.brlmd_column_mapping BCM
INNER JOIN QSIT_APRA2_BRL_RRP_VW.brlmd_table_mapping TM
ON BCM.Col_Map_Src_Tbl = TM.Tab_Map_Src_Tbl
WHERE BCM.Col_Map_Src_Col = '*' 
AND BCM.Col_Map_Tgt_Col = '*'
AND TM.Tab_Map_View_Name = vname
AND BCM.Col_Map_View_Name = vname

DO
----------
	SET vPhy_Tbl_Nm = CSR1.Phy_Tbl_Nm;   

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, 200, 'IN LOOOP L1 CSR1.ATTR_NM = '|| CSR1.Phy_Tbl_Nm, 3, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);

L2: 
FOR	CSR2 AS 
SELECT
AM.ATTR_NM AS ATTR_NM
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR AM
INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR EM
ON AM.ENT_ID = EM.ENT_ID
AND EM.ENT_NM = vPhy_Tbl_Nm
--AND EM.SCHM_NM = TM.Src_Db_Nm
WHERE	 1=1
AND AM.ATTR_NM NOT IN ('EFFV_DT','EXPR_DT','USR_ID')
AND AM.ATTR_NM NOT IN (SELECT Col_Map_Tgt_Col 
						FROM QSIT_APRA2_BRL_RRP_VW.brlmd_column_mapping 
						WHERE Col_Map_View_Name = vname
					)
ORDER BY ATTR_NM

DO
----------
SET viname = ''||vname||'';
SET vTab_Alias = ''||CSR1.Tab_Alias||'';
SET vATTR_NM = ''||CSR2.ATTR_NM||'';
SET vCol_Map_Tgt_Tbl = ''||CSR1.Col_Map_Tgt_Tbl||'';
SET vCol_Map_Version = ''||CSR1.Col_Map_Version||'';

IF iExec_Mode = 'Y' THEN 
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.brlmd_column_mapping
	( Col_Map_View_Name,
	Col_Map_Type,
	Col_Map_Src_Tbl,
	Col_Map_Src_Col,
	Col_Map_Tgt_Tbl,
	Col_Map_Tgt_Col,
	Col_Map_Fmt,
	Col_Map_Version,
	Col_Map_Comment
	) 
	VALUES 
	(viname,'ColumnMap',vTab_Alias,vATTR_NM,vCol_Map_Tgt_Tbl,vATTR_NM,'',vCol_Map_Version,'GENERATED');

	SET vActivity_Ct = ACTIVITY_COUNT;

	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Table brlmd_column_mapping Loaded - ' || TRIM(vActivity_Ct) || ' Rows Inserted'||' view name -'|| viname ||' SRC_TAB - '|| vTab_Alias ||' New col -'|| vATTR_NM ||' TGT_TBL - '|| vCol_Map_Tgt_Tbl;
	SET vDebugLvl = 1;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

ELSE

	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Exec Mode N - was supposed to insert '||vATTR_NM  ||' but did not do anything';
	SET vDebugLvl = 1;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


END IF;
----------
END FOR L2;

----------
END FOR L1;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_EXPAND_COL_QSIT
-- =============================================
-- Description: This Stored Procedure will generate/ derive the attributes for CLMD_COLUMN_MAPPING table.
-- Change log
-- [2016 06 08]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
IN vname 				VARCHAR (32), 
IN iExec_Mode VARCHAR(1),
OUT ORETURN_CODE        SMALLINT,             /* 0: SUCCESSFUL */
OUT ORETURN_MESSAGE     VARCHAR(250)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF 				CHAR(2) 		DEFAULT '0A'XC;
DECLARE vCntr 				INTEGER 		DEFAULT 100;
DECLARE vActivity_Ct 		INTEGER 		DEFAULT -99;
DECLARE vSQL_Ins 			VARCHAR(10000) 	DEFAULT '';
DECLARE vSQL_Del 			VARCHAR(10000) 	DEFAULT '';
DECLARE vSQL_Code 			INTEGER;
DECLARE vSQL_State 			VARCHAR(6);
DECLARE vError_Text 		VARCHAR(256);
DECLARE oSubReturn_Code 	SMALLINT;
DECLARE oSubReturn_Message 	VARCHAR(1000);
DECLARE vDebugLvl 			SMALLINT 		DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg 			VARCHAR(10000);
DECLARE vLogSPName 			VARCHAR(255) 	DEFAULT 'BRLMD_RTN_EXPAND_COL';
DECLARE vPhy_Tbl_Nm			VARCHAR(256);
DECLARE viname				VARCHAR(256);
DECLARE vTab_Alias			VARCHAR(256);
DECLARE vATTR_NM			VARCHAR(256);
DECLARE vCol_Map_Tgt_Tbl	VARCHAR(256);
DECLARE vCol_Map_Version	VARCHAR(256);


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';

SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET vSQL_Ins = '';
SET vSQL_Del = '';

--LOGIC OF SELECT STATEMENT
L1: 
FOR	CSR1 AS 

SELECT
DISTINCT TM.Phy_Tbl_Nm AS Phy_Tbl_Nm, 
TM.Tab_Map_Src_Tbl AS Tab_Alias ,
BCM.Col_Map_Tgt_Tbl AS Col_Map_Tgt_Tbl,
Col_Map_Version AS Col_Map_Version 
FROM QSIT_APRA2_BRL_RRP_VW.brlmd_column_mapping BCM
INNER JOIN QSIT_APRA2_BRL_RRP_VW.brlmd_table_mapping TM
ON BCM.Col_Map_Src_Tbl = TM.Tab_Map_Src_Tbl
WHERE BCM.Col_Map_Src_Col = '*' 
AND BCM.Col_Map_Tgt_Col = '*'
AND TM.Tab_Map_View_Name = vname
AND BCM.Col_Map_View_Name = vname

DO
----------
	SET vPhy_Tbl_Nm = CSR1.Phy_Tbl_Nm;   

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, 200, 'IN LOOOP L1 CSR1.ATTR_NM = '|| CSR1.Phy_Tbl_Nm, 3, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);

L2: 
FOR	CSR2 AS 
SELECT
AM.ATTR_NM AS ATTR_NM
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR AM
INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR EM
ON AM.ENT_ID = EM.ENT_ID
AND EM.ENT_NM = vPhy_Tbl_Nm
--AND EM.SCHM_NM = TM.Src_Db_Nm
WHERE	 1=1
AND AM.ATTR_NM NOT IN ('EFFV_DT','EXPR_DT','USR_ID')
AND AM.ATTR_NM NOT IN (SELECT Col_Map_Tgt_Col 
						FROM QSIT_APRA2_BRL_RRP_VW.brlmd_column_mapping 
						WHERE Col_Map_View_Name = vname
					)
ORDER BY ATTR_NM

DO
----------
SET viname = ''||vname||'';
SET vTab_Alias = ''||CSR1.Tab_Alias||'';
SET vATTR_NM = ''||CSR2.ATTR_NM||'';
SET vCol_Map_Tgt_Tbl = ''||CSR1.Col_Map_Tgt_Tbl||'';
SET vCol_Map_Version = ''||CSR1.Col_Map_Version||'';

IF iExec_Mode = 'Y' THEN 
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.brlmd_column_mapping
	( Col_Map_View_Name,
	Col_Map_Type,
	Col_Map_Src_Tbl,
	Col_Map_Src_Col,
	Col_Map_Tgt_Tbl,
	Col_Map_Tgt_Col,
	Col_Map_Fmt,
	Col_Map_Version,
	Col_Map_Comment
	) 
	VALUES 
	(viname,'ColumnMap',vTab_Alias,vATTR_NM,vCol_Map_Tgt_Tbl,vATTR_NM,'',vCol_Map_Version,'GENERATED');

	SET vActivity_Ct = ACTIVITY_COUNT;

	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Table brlmd_column_mapping Loaded - ' || TRIM(vActivity_Ct) || ' Rows Inserted'||' view name -'|| viname ||' SRC_TAB - '|| vTab_Alias ||' New col -'|| vATTR_NM ||' TGT_TBL - '|| vCol_Map_Tgt_Tbl;
	SET vDebugLvl = 1;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

ELSE

	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Exec Mode N - was supposed to insert '||vATTR_NM  ||' but did not do anything';
	SET vDebugLvl = 1;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


END IF;
----------
END FOR L2;

----------
END FOR L1;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE	PROCEDURE  QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_FROM
-- =============================================
-- AUTHOR             :               PRALHAD KAMATH
-- CREATE DATE   :               23  DEC  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- =============================================
-- Stored Procedure Parameters
(
IN vi_name VARCHAR (32),  
OUT ov_str VARCHAR(30000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL*/
OUT ORETURN_MESSAGE           VARCHAR(250)
)
MAIN:
BEGIN	

-- Declare variables
DECLARE	v_str VARCHAR(30000);
DECLARE	v_Inline_View_Name VARCHAR(30000);
DECLARE	EOLStr VARCHAR(5);
DECLARE	v_Left_Str VARCHAR(1000);
DECLARE	v_Right_Str VARCHAR(1000);
DECLARE	v_Join_Str VARCHAR(1000);
DECLARE	oSubReturn_Code             SMALLINT;
DECLARE	oSubReturn_Message          VARCHAR(1000);

-- Error Handler variables
DECLARE	vSQL_Code                   INTEGER;
DECLARE	vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE	vError_Text                 VARCHAR(1999);

DECLARE	EXIT HANDLER FOR SQLEXCEPTION
BEGIN	
	--
	-- Preserve Diagnostic Codes from errored Statement
	--
	SET	vSQL_Code  = SQLCODE;
	SET	vSQL_State = SQLSTATE;
	SET	vError_Text = ' ';
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
	SET	oReturn_Code = 1;
	SET	oReturn_Message = 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

/* ----------------------------------- Perform Error Checking --------------------------------------------------------------------------------*/
IF	vi_name NOT IN (
SELECT	Join_Cond_View_Name 
FROM	 QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition WHERE Join_Cond_View_Name = vi_name GROUP BY 1)
THEN
	SET	VSQL_CODE  = 5;
	SET	VSQL_STATE = 1005;  
	SET	VERROR_TEXT = 'Invalid View Name provided from - FROM statement';
	--LOG INTO ERROR TABLE
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
END IF	;

SET	EOLStr ='
';

SET	v_str = 'FROM'||EOLStr;

--LOGIC OF FROM STATEMENT
L1: 
FOR	CSR1 AS 
	SELECT	Join_Cond_View_Name, Join_Cond_Tbl, Join_Cond_Typ,
	Join_Cond_Left_Tbl, Join_Cond_Left_Col, Join_Cond_Opr, Join_Cond_Right_tbl,
	Join_Cond_Right_Col, Join_Cond_Col_Lnk, 
	ROW_NUMBER() OVER (PARTITION BY Join_Cond_View_Name, Join_Cond_Tbl 
	ORDER	BY Join_Cond_Tbl) AS RowNo
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	INNER JOIN QSIT_APRA2_BRL_RRP_VW.BRLMD_table_mapping
	ON	Join_Cond_Tbl = Tab_Map_Src_Tbl 
	AND	Join_Cond_View_Name = Tab_Map_View_Name
	WHERE	  Join_Cond_View_Name = vi_name
	ORDER	BY Tab_Map_Tbl_Seq_No, Join_Cond_Tbl, RowNo
DO
----------
	IF CSR1.Join_Cond_Typ = '<BLANK>'  THEN
	--{	 This is for first table (driver table)
	
		CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_INLINE_VIEW(CSR1.Join_Cond_View_Name, CSR1.Join_Cond_Tbl, v_Inline_View_Name);
		SET v_str = v_str || v_Inline_View_Name ||EOLStr;	
		
	--}
	ELSE 
	--{ For all  other tables 
	
	IF	CSR1.RowNo=1  THEN 
	--{
		SET v_Str = v_Str || ''||
		CASE	CSR1.Join_Cond_Typ
			WHEN 'INNER' THEN 'INNER JOIN'
			WHEN 'LEFT OUTER' THEN 'LEFT OUTER JOIN'
			WHEN 'CROSS' THEN 'CROSS JOIN'
		END; 
											
		CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_INLINE_VIEW(CSR1.Join_Cond_View_Name, CSR1.Join_Cond_Tbl, v_Inline_View_Name);
		SET v_str = v_str || v_Inline_View_Name ||EOLStr;			
		
		IF CSR1.Join_Cond_Typ <> 'CROSS' THEN
				SET v_Str = v_Str ||' ON '; 		
		END IF;
	--}
	ELSE 
	--{
		SET v_Str = v_Str||'AND ';	
	--}
	END IF	; -- Add join type
	
	IF COALESCE(CSR1.Join_Cond_Left_Tbl,'') <> '' THEN
	--{
		SET v_Left_Str = CSR1.Join_Cond_Left_Tbl ||'.'||CSR1.Join_Cond_Left_Col;
	--}
	ELSE
	--{
		SET v_Left_Str = '';
	--}
	END IF;

	IF COALESCE(CSR1.Join_Cond_Right_tbl,'') <> '' THEN
	--{
			SET v_Right_Str = CSR1.Join_Cond_Right_tbl||'.'||CSR1.Join_Cond_Right_Col;
	--}
	ELSE -- Right table is blank we have RAW/COALESCE/CASE
	--{
		IF CSR1.Join_Cond_Opr  = 'RAW' THEN
		--{ -- Plonk it as is without validation
			SET v_Right_Str = CSR1.Join_Cond_Right_Col;
		
		--}
		ELSE 
		--{ e.g. CC_ID = 'CC-21'
			
				SET v_Right_Str = ''''||CSR1.Join_Cond_Right_Col||'''';

		--}
		END IF;
		
	--}
	END IF;
	
	SET v_Join_Str = CASE CSR1.Join_Cond_Opr WHEN 'RAW' THEN '' ELSE CSR1.Join_Cond_Opr END;
	
	IF CSR1.Join_Cond_Typ <> 'CROSS' THEN
		SET v_str = v_Str||v_Left_Str||' '||v_Join_Str ||' '||v_Right_Str||EOLStr  ;
	
	ELSE
		
		SET v_str = v_Str;
		
	END IF;
	/* - Deprecated functionality - Not used in any mapping
		IF CSR1.Join_Cond_Col_Lnk IS NOT NULL THEN
			SET v_str = v_Str||CSR1.Join_Cond_Col_Lnk;
		END IF;
	*/
		

	--}
	END IF;
----------
END FOR	L1;

--LOG INTO ERROR TABLE

IF vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE 1 = 1 
	AND	join_cond_tbl <> '' 
	AND	join_cond_typ<>'' 
	AND	join_cond_left_tbl<> ''
	AND join_cond_left_col<>'' 
	AND	join_cond_opr <>'' 
	AND	join_cond_right_tbl<>'' 
	AND	join_cond_right_col<>''
	AND Join_Cond_View_Name = vi_name)
	--where Col_Map_Tgt_Col <> ' ' and Col_Map_Type <> ' ')
THEN
	SET	ov_str = v_str ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'FROM statement - Success';
	LEAVE MAIN;
END IF	;

IF	vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE  1=1 
	AND	join_cond_typ='<BLANK>' 
	AND	join_cond_tbl <> '' 
	AND	 join_cond_left_tbl= ''
	AND join_cond_left_col='' 
	AND	join_cond_opr ='' 
	AND	join_cond_right_tbl='' 
	AND join_cond_right_col=''
	AND Join_Cond_View_Name = vi_name)
THEN
SET	ov_str = v_str ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'FROM statement - Success';
	LEAVE MAIN;
END IF	;

IF vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE 1 = 1  
	AND	join_cond_typ<>'<BLANK>' 
	AND	join_cond_tbl <> '' 
	AND	
	( join_cond_left_tbl= '' 
	OR	join_cond_left_col='' 
	OR	join_cond_opr ='' 
	OR	join_cond_right_tbl='' 
	OR join_cond_right_col='')
	AND Join_Cond_View_Name = vi_name)
THEN
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
	SET oReturn_Code = 20;
	SET oReturn_Message = 'Either of the input attributes is Blank - FROM statement';
	LEAVE MAIN;
END IF	;
END MAIN;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE  QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_FROM_QSIT
-- =============================================
-- AUTHOR             :               PRALHAD KAMATH
-- CREATE DATE   :               23  DEC  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- =============================================
-- Stored Procedure Parameters
(
IN vi_name VARCHAR (32),  
OUT ov_str VARCHAR(30000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL*/
OUT ORETURN_MESSAGE           VARCHAR(250)
)
MAIN:
BEGIN	

-- Declare variables
DECLARE	v_str VARCHAR(30000);
DECLARE	v_Inline_View_Name VARCHAR(30000);
DECLARE	EOLStr VARCHAR(5);
DECLARE	v_Left_Str VARCHAR(1000);
DECLARE	v_Right_Str VARCHAR(1000);
DECLARE	v_Join_Str VARCHAR(1000);
DECLARE	oSubReturn_Code             SMALLINT;
DECLARE	oSubReturn_Message          VARCHAR(1000);

-- Error Handler variables
DECLARE	vSQL_Code                   INTEGER;
DECLARE	vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE	vError_Text                 VARCHAR(1999);

DECLARE	EXIT HANDLER FOR SQLEXCEPTION
BEGIN	
	--
	-- Preserve Diagnostic Codes from errored Statement
	--
	SET	vSQL_Code  = SQLCODE;
	SET	vSQL_State = SQLSTATE;
	SET	vError_Text = ' ';
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
	SET	oReturn_Code = 1;
	SET	oReturn_Message = 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

/* ----------------------------------- Perform Error Checking --------------------------------------------------------------------------------*/
IF	vi_name NOT IN (
SELECT	Join_Cond_View_Name 
FROM	 QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition WHERE Join_Cond_View_Name = vi_name GROUP BY 1)
THEN
	SET	VSQL_CODE  = 5;
	SET	VSQL_STATE = 1005;  
	SET	VERROR_TEXT = 'Invalid View Name provided from - FROM statement';
	--LOG INTO ERROR TABLE
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
END IF	;

SET	EOLStr ='
';

SET	v_str = 'FROM'||EOLStr;

--LOGIC OF FROM STATEMENT
L1: 
FOR	CSR1 AS 
	SELECT	Join_Cond_View_Name, Join_Cond_Tbl, Join_Cond_Typ,
	Join_Cond_Left_Tbl, Join_Cond_Left_Col, Join_Cond_Opr, Join_Cond_Right_tbl,
	Join_Cond_Right_Col, Join_Cond_Col_Lnk, 
	ROW_NUMBER() OVER (PARTITION BY Join_Cond_View_Name, Join_Cond_Tbl 
	ORDER	BY Join_Cond_Tbl) AS RowNo
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	INNER JOIN QSIT_APRA2_BRL_RRP_VW.BRLMD_table_mapping
	ON	Join_Cond_Tbl = Tab_Map_Src_Tbl 
	AND	Join_Cond_View_Name = Tab_Map_View_Name
	WHERE	  Join_Cond_View_Name = vi_name
	ORDER	BY Tab_Map_Tbl_Seq_No, Join_Cond_Tbl, RowNo
DO
----------
	IF CSR1.Join_Cond_Typ = '<BLANK>'  THEN
	--{	 This is for first table (driver table)
	
		CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_INLINE_VIEW_QSIT(CSR1.Join_Cond_View_Name, CSR1.Join_Cond_Tbl, v_Inline_View_Name);
		SET v_str = v_str || v_Inline_View_Name ||EOLStr;	
		
	--}
	ELSE 
	--{ For all  other tables 
	
	IF	CSR1.RowNo=1  THEN 
	--{
		SET v_Str = v_Str || ''||
		CASE	CSR1.Join_Cond_Typ
			WHEN 'INNER' THEN 'INNER JOIN'
			WHEN 'LEFT OUTER' THEN 'LEFT OUTER JOIN'
			WHEN 'CROSS' THEN 'CROSS JOIN'
		END; 
											
		CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_INLINE_VIEW_QSIT(CSR1.Join_Cond_View_Name, CSR1.Join_Cond_Tbl, v_Inline_View_Name);
		SET v_str = v_str || v_Inline_View_Name ||EOLStr;			
		
		IF CSR1.Join_Cond_Typ <> 'CROSS' THEN
				SET v_Str = v_Str ||' ON '; 		
		END IF;
	--}
	ELSE 
	--{
		SET v_Str = v_Str||'AND ';	
	--}
	END IF	; -- Add join type
	
	IF COALESCE(CSR1.Join_Cond_Left_Tbl,'') <> '' THEN
	--{
		SET v_Left_Str = CSR1.Join_Cond_Left_Tbl ||'.'||CSR1.Join_Cond_Left_Col;
	--}
	ELSE
	--{
		SET v_Left_Str = '';
	--}
	END IF;

	IF COALESCE(CSR1.Join_Cond_Right_tbl,'') <> '' THEN
	--{
			SET v_Right_Str = CSR1.Join_Cond_Right_tbl||'.'||CSR1.Join_Cond_Right_Col;
	--}
	ELSE -- Right table is blank we have RAW/COALESCE/CASE
	--{
		IF CSR1.Join_Cond_Opr  = 'RAW' THEN
		--{ -- Plonk it as is without validation
			SET v_Right_Str = CSR1.Join_Cond_Right_Col;
		
		--}
		ELSE 
		--{ e.g. CC_ID = 'CC-21'
			
				SET v_Right_Str = ''''||CSR1.Join_Cond_Right_Col||'''';

		--}
		END IF;
		
	--}
	END IF;
	
	SET v_Join_Str = CASE CSR1.Join_Cond_Opr WHEN 'RAW' THEN '' ELSE CSR1.Join_Cond_Opr END;
	
	IF CSR1.Join_Cond_Typ <> 'CROSS' THEN
		SET v_str = v_Str||v_Left_Str||' '||v_Join_Str ||' '||v_Right_Str||EOLStr  ;
	
	ELSE
		
		SET v_str = v_Str;
		
	END IF;
	/* - Deprecated functionality - Not used in any mapping
		IF CSR1.Join_Cond_Col_Lnk IS NOT NULL THEN
			SET v_str = v_Str||CSR1.Join_Cond_Col_Lnk;
		END IF;
	*/
		

	--}
	END IF;
----------
END FOR	L1;

--LOG INTO ERROR TABLE

IF vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE 1 = 1 
	AND	join_cond_tbl <> '' 
	AND	join_cond_typ<>'' 
	AND	join_cond_left_tbl<> ''
	AND join_cond_left_col<>'' 
	AND	join_cond_opr <>'' 
	AND	join_cond_right_tbl<>'' 
	AND	join_cond_right_col<>''
	AND Join_Cond_View_Name = vi_name)
	--where Col_Map_Tgt_Col <> ' ' and Col_Map_Type <> ' ')
THEN
	SET	ov_str = v_str ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'FROM statement - Success';
	LEAVE MAIN;
END IF	;

IF	vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE  1=1 
	AND	join_cond_typ='<BLANK>' 
	AND	join_cond_tbl <> '' 
	AND	 join_cond_left_tbl= ''
	AND join_cond_left_col='' 
	AND	join_cond_opr ='' 
	AND	join_cond_right_tbl='' 
	AND join_cond_right_col=''
	AND Join_Cond_View_Name = vi_name)
THEN
SET	ov_str = v_str ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'FROM statement - Success';
	LEAVE MAIN;
END IF	;

IF vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE 1 = 1  
	AND	join_cond_typ<>'<BLANK>' 
	AND	join_cond_tbl <> '' 
	AND	
	( join_cond_left_tbl= '' 
	OR	join_cond_left_col='' 
	OR	join_cond_opr ='' 
	OR	join_cond_right_tbl='' 
	OR join_cond_right_col='')
	AND Join_Cond_View_Name = vi_name)
THEN
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
	SET oReturn_Code = 20;
	SET oReturn_Message = 'Either of the input attributes is Blank - FROM statement';
	LEAVE MAIN;
END IF	;
END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE	PROCEDURE  QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_FROM1
-- =============================================
-- AUTHOR             :               PRALHAD KAMATH
-- CREATE DATE   :               23  DEC  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- =============================================
-- Stored Procedure Parameters
(
IN vi_name VARCHAR (32),  
OUT ov_str VARCHAR(30000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL*/
OUT ORETURN_MESSAGE           VARCHAR(250)
)
MAIN:
BEGIN	

-- Declare variables
DECLARE	v_str VARCHAR(30000);
DECLARE	v_Inline_View_Name VARCHAR(30000);
DECLARE	EOLStr VARCHAR(5);
DECLARE	v_Left_Str VARCHAR(1000);
DECLARE	v_Right_Str VARCHAR(1000);
DECLARE	v_Join_Str VARCHAR(1000);
DECLARE	oSubReturn_Code             SMALLINT;
DECLARE	oSubReturn_Message          VARCHAR(1000);

-- Error Handler variables
DECLARE	vSQL_Code                   INTEGER;
DECLARE	vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE	vError_Text                 VARCHAR(1999);

DECLARE	EXIT HANDLER FOR SQLEXCEPTION
BEGIN	
	--
	-- Preserve Diagnostic Codes from errored Statement
	--
	SET	vSQL_Code  = SQLCODE;
	SET	vSQL_State = SQLSTATE;
	SET	vError_Text = ' ';
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
	SET	oReturn_Code = 1;
	SET	oReturn_Message = 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

/* ----------------------------------- Perform Error Checking --------------------------------------------------------------------------------*/
IF	vi_name NOT IN (
SELECT	Join_Cond_View_Name 
FROM	 QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition WHERE Join_Cond_View_Name = vi_name GROUP BY 1)
THEN
	SET	VSQL_CODE  = 5;
	SET	VSQL_STATE = 1005;  
	SET	VERROR_TEXT = 'Invalid View Name provided from - FROM statement';
	--LOG INTO ERROR TABLE
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
END IF	;

SET	EOLStr ='
';

SET	v_str = 'FROM'||EOLStr;

--LOGIC OF FROM STATEMENT
L1: 
FOR	CSR1 AS 
	SELECT	Join_Cond_View_Name, Join_Cond_Tbl, Join_Cond_Typ,
	Join_Cond_Left_Tbl, Join_Cond_Left_Col, Join_Cond_Opr, Join_Cond_Right_tbl,
	Join_Cond_Right_Col, Join_Cond_Col_Lnk, 
	ROW_NUMBER() OVER (PARTITION BY Join_Cond_View_Name, Join_Cond_Tbl 
	ORDER	BY Join_Cond_Tbl) AS RowNo
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	INNER JOIN QSIT_APRA2_BRL_RRP_VW.BRLMD_table_mapping
	ON	Join_Cond_Tbl = Tab_Map_Src_Tbl 
	AND	Join_Cond_View_Name = Tab_Map_View_Name
	WHERE	  Join_Cond_View_Name = vi_name
	ORDER	BY Tab_Map_Tbl_Seq_No, Join_Cond_Tbl, RowNo
DO
----------
	IF CSR1.Join_Cond_Typ = '<BLANK>'  THEN
	--{
	
		CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_INLINE_VIEW(CSR1.Join_Cond_View_Name, CSR1.Join_Cond_Tbl, v_Inline_View_Name);
		SET v_str = v_str || v_Inline_View_Name ||EOLStr;	
		
	--}
	ELSE 
	--{
	
	IF	CSR1.RowNo=1  THEN 
	--{
		SET v_Str = v_Str || ''||
		CASE	CSR1.Join_Cond_Typ
			WHEN 'INNER' THEN 'INNER JOIN'
			WHEN 'LEFT OUTER' THEN 'LEFT OUTER JOIN'
		END; 
											
		CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_INLINE_VIEW(CSR1.Join_Cond_View_Name, CSR1.Join_Cond_Tbl, v_Inline_View_Name);
		SET v_str = v_str || v_Inline_View_Name ||EOLStr;			
		SET v_Str = v_Str ||' ON '; 		
	--}
	ELSE 
	--{
		SET v_Str = v_Str||'AND ';	
	--}
	END IF	; -- Add join type
	
	IF COALESCE(CSR1.Join_Cond_Left_Tbl,'') <> '' THEN
	--{
		SET v_Left_Str = CSR1.Join_Cond_Left_Tbl ||'.'||CSR1.Join_Cond_Left_Col;
	--}
	ELSE
	--{
		SET v_Left_Str = '';
	--}
	END IF;

	IF COALESCE(CSR1.Join_Cond_Right_tbl,'') <> '' THEN
	--{
			SET v_Right_Str = CSR1.Join_Cond_Right_tbl||'.'||CSR1.Join_Cond_Right_Col;
	--}
	ELSE -- Right table is blank we have RAW/COALESCE/CASE
	--{
		IF CSR1.Join_Cond_Opr  = 'RAW' THEN
		--{ -- Plonk it as is without validation
			SET v_Right_Str = CSR1.Join_Cond_Right_Col;
		
		--}
		ELSE
		--{ e.g. CC_ID = 'CC-21'
			SET v_Right_Str = ''''||CSR1.Join_Cond_Right_Col||'''';
		--}
		END IF;
		
	--}
	END IF;
	
	SET v_Join_Str = CASE CSR1.Join_Cond_Opr WHEN 'RAW' THEN '' ELSE CSR1.Join_Cond_Opr END;
	
	SET v_str = v_Str||v_Left_Str||' '||v_Join_Str ||' '||v_Right_Str||EOLStr  ;

	/* - Deprecated functionality - Not used in any mapping
		IF CSR1.Join_Cond_Col_Lnk IS NOT NULL THEN
			SET v_str = v_Str||CSR1.Join_Cond_Col_Lnk;
		END IF;
	*/
		
/*
	IF	CSR1.Join_Cond_Typ <> '<BLANK>' AND	CSR1.RowNo=1  THEN 
		SET v_Str = v_Str || ''||
		CASE	CSR1.Join_Cond_Typ
											WHEN 'INNER' THEN 'INNER JOIN'
											WHEN 'LEFT OUTER' THEN 'LEFT OUTER JOIN'
											END; 
	END IF	; -- Add join type

	IF	CSR1.RowNo = 1 THEN 
		CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_INLINE_VIEW(CSR1.Join_Cond_View_Name, CSR1.Join_Cond_Tbl, v_Inline_View_Name);
		SET v_str = v_str || v_Inline_View_Name ||EOLStr;
		ELSE	
		SET v_Str = v_Str||'AND ';
	END IF	; -- Add inline view 

	IF	CSR1.Join_Cond_Typ <> '<BLANK>' 
		AND	CSR1.RowNo=1  THEN 
		SET v_Str = v_Str ||' ON '; 
	END IF	; 												

	IF	CSR1.Join_Cond_Typ <> '<BLANK>' THEN 
		IF COALESCE(CSR1.Join_Cond_Left_Tbl,'') <> '' THEN	
			SET v_Left_Str = CSR1.Join_Cond_Left_Tbl ||'.'||CSR1.Join_Cond_Left_Col;
		ELSE 
			SET v_Left_Str = '';
		END IF;
		IF COALESCE(CSR1.Join_Cond_Right_tbl,'') <> '' THEN
		
			SET v_Right_Str = CSR1.Join_Cond_Right_tbl||'.'||CSR1.Join_Cond_Right_Col;
		ELSEIF CSR1.Join_Cond_Right_Col LIKE '%CASE%' THEN
		    SET v_Right_Str = CSR1.Join_Cond_Right_Col;
        ELSEIF CSR1.Join_Cond_Right_Col LIKE 'COALESCE%' THEN
		    SET v_Right_Str = CSR1.Join_Cond_Right_Col;
		ELSE	
			SET v_Right_Str = ''''||CSR1.Join_Cond_Right_Col||'''';
		END IF; 

		
	IF COALESCE(CSR1.Join_Cond_Left_Tbl,'') = '' AND COALESCE(CSR1.Join_Cond_Left_Col,'') = '' AND CSR1.Join_Cond_Opr = 'RAW'
	THEN SET v_str = v_Str||' '||v_Right_Str||EOLStr  ;
	ELSE
		
	SET v_str = v_Str||v_Left_Str||' '||CSR1.Join_Cond_Opr||' '||v_Right_Str||EOLStr  ;
	END IF;
		IF CSR1.Join_Cond_Col_Lnk IS NOT NULL THEN
			SET v_str = v_Str||CSR1.Join_Cond_Col_Lnk;
		END IF;
	END IF	; -- Add on clauses
*/
	--}
	END IF;
----------
END FOR	L1;

--LOG INTO ERROR TABLE

IF vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE 1 = 1 
	AND	join_cond_tbl <> '' 
	AND	join_cond_typ<>'' 
	AND	join_cond_left_tbl<> ''
	AND join_cond_left_col<>'' 
	AND	join_cond_opr <>'' 
	AND	join_cond_right_tbl<>'' 
	AND	join_cond_right_col<>''
	AND Join_Cond_View_Name = vi_name)
	--where Col_Map_Tgt_Col <> ' ' and Col_Map_Type <> ' ')
THEN
	SET	ov_str = v_str ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'FROM statement - Success';
	LEAVE MAIN;
END IF	;

IF	vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE  1=1 
	AND	join_cond_typ='<BLANK>' 
	AND	join_cond_tbl <> '' 
	AND	 join_cond_left_tbl= ''
	AND join_cond_left_col='' 
	AND	join_cond_opr ='' 
	AND	join_cond_right_tbl='' 
	AND join_cond_right_col=''
	AND Join_Cond_View_Name = vi_name)
THEN
SET	ov_str = v_str ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'FROM statement - Success';
	LEAVE MAIN;
END IF	;

IF vi_name  IN (
	SELECT	Join_Cond_View_Name 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
	WHERE 1 = 1  
	AND	join_cond_typ<>'<BLANK>' 
	AND	join_cond_tbl <> '' 
	AND	
	( join_cond_left_tbl= '' 
	OR	join_cond_left_col='' 
	OR	join_cond_opr ='' 
	OR	join_cond_right_tbl='' 
	OR join_cond_right_col='')
	AND Join_Cond_View_Name = vi_name)
THEN
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
	SET oReturn_Code = 20;
	SET oReturn_Message = 'Either of the input attributes is Blank - FROM statement';
	LEAVE MAIN;
END IF	;
END MAIN;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM. BRLMD_RTN_INLINE_VIEW
(
IN VI_NAME VARCHAR (32),
IN VI_TABLENAME VARCHAR(32),
OUT OV_STR VARCHAR(35000)
)
BEGIN	
DECLARE	V_STR VARCHAR(30000);
DECLARE	V_STR_COL_LIST VARCHAR(30000);
DECLARE V_DBNM VARCHAR(500);
DECLARE	EOLSTR VARCHAR(500);

-- ERROR HANDLER VARIABLES
DECLARE	VSQL_CODE                   INTEGER;
DECLARE	VSQL_STATE                  VARCHAR(6) DEFAULT ''XC;
DECLARE	VERROR_TEXT                 VARCHAR(1999);

SET EOLSTR ='
';

SET V_STR = '';
SET V_DBNM = '';

L1: 
FOR	CSR1 AS 
SELECT
*
FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_TABLE_MAPPING
WHERE 
	TAB_MAP_VIEW_NAME =   VI_NAME 
AND TAB_MAP_SRC_TBL = VI_TABLENAME
DO
----------
/* L2 CURSOR STARTS HERE 
*/
IF	CSR1.SRC_DB_NM <> '' THEN
SET V_DBNM = CSR1.SRC_DB_NM;
ELSE
SET V_DBNM = 'QSIT_APRA2_BRL_RRP_VW';
END IF;

SET V_STR_COL_LIST = '';
 
L2: 
FOR	CSR2 AS 


  SELECT 
 COL_MAP_SRC_TBL, COL_MAP_SRC_COL AS ATTRB
FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_COLUMN_MAPPING
WHERE COL_MAP_VIEW_NAME =VI_NAME
AND COL_MAP_TYPE IN ('COLUMNMAP','STATICLOGIC')
AND 
(COL_MAP_SRC_TBL   = VI_TABLENAME )
AND Col_Map_Tgt_Col NOT IN ('*')
--OR COL_MAP_SRC_COL  = VI_TABLENAME )             

UNION 

SEL JOIN_COND_LEFT_TBL, JOIN_COND_LEFT_COL FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_JOIN_CONDITION
WHERE JOIN_COND_VIEW_NAME =VI_NAME
AND JOIN_COND_LEFT_TBL = VI_TABLENAME

UNION 

SEL JOIN_COND_RIGHT_TBL, JOIN_COND_RIGHT_COL FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_JOIN_CONDITION
WHERE JOIN_COND_VIEW_NAME =VI_NAME
AND JOIN_COND_RIGHT_TBL = VI_TABLENAME

UNION


SEL WHR_TBL, WHR_COL FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_WHR_CLAUSE
WHERE VIEW_NAME  =VI_NAME
AND WHR_TBL = VI_TABLENAME


DO
----------
 --SET V_STR = V_STR||  '( SELECT ' ||CSR1.ATTRB||' FROM QSIT_APRA2_BRL_RRP_VW.'|| CSR1.PHY_TBL_NM ||' WHERE '||EOLSTR;

  SET V_STR_COL_LIST = V_STR_COL_LIST ||'
	,' ||CSR2.ATTRB;
	
	

----------
END FOR L2;


IF V_STR_COL_LIST = '' THEN 
SET V_STR_COL_LIST ='*' ; END IF; -- WILL BE POPULATED HERE 
/* L2 CURSOR ENDS HERE 
*/
--SET V_STR = '( SELECT '||V_STR_COL_LIST||' FROM QSIT_APRA2_BRL_RRP_VW.'|| CSR1.PHY_TBL_NM ||' WHERE '||EOLSTR;
SET V_STR =  '( SELECT 
 	'|| SUBSTR(V_STR_COL_LIST, INDEX(V_STR_COL_LIST, ',')+1)||' FROM '||V_DBNM||'.'|| CSR1.PHY_TBL_NM ||' WHERE ';

CASE CSR1.TAB_MAP_FLTR_TYP
	WHEN 'CL' THEN
		SET V_STR = V_STR ||CSR1.TAB_MAP_FLTR_COL ||' '||CSR1.TAB_MAP_FLTR_COND || ' 
		(SELECT CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL WHERE CL_SCHM_NM = '''||CSR1.TAB_MAP_SCHM_NM||''' AND CL_CD = '''||CSR1.TAB_MAP_CL_CD||''')';
	WHEN 'RAW' THEN	
		SET V_STR = V_STR || CSR1.TAB_MAP_FLTR_CRIT;
	WHEN 'AU' THEN 
		SET V_STR = V_STR ||
		'    AR_TO_AU_RL_TY_ID = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''AR / AU RLTNP TYPE'' AND CL_RL.CL_CD ='''||CSR1.TAB_MAP_AU_RL_TYP ||''')' ||EOLSTR||
		'AND AU_TY_ID          = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''ACCOUNTING UNIT TYPE'' AND CL_RL.CL_CD ='''||CSR1.TAB_MAP_AU_TYP ||''') '||EOLSTR||
		'AND SCENR_ID          = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''SCENARIO'' AND CL_RL.CL_CD ='''||CSR1.TAB_MAP_AU_SCHM ||''') '||EOLSTR||
		'AND UNT_OF_MSR_ID     = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''UNIT OF MEASURE TYPE'' AND CL_RL.CL_CD ='''||CSR1.TAB_MAP_AU_MSR ||''') ';
	ELSE	
		SET V_STR = V_STR || '1=1';
END CASE;		

----------
END FOR L1;

SET OV_STR = V_STR || ') AS '|| VI_TABLENAME;
END;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM. BRLMD_RTN_INLINE_VIEW_QSIT (IN vi_name VARCHAR (32), IN vi_tablename VARCHAR(32),  OUT ov_str VARCHAR(40000))
BEGIN	
DECLARE	v_str VARCHAR(20000);
DECLARE	v_Str_Col_List VARCHAR(20000);
DECLARE v_str_where VARCHAR(1000);
DECLARE output_where VARCHAR(4000);
DECLARE return_code_where INT;
DECLARE return_message_where VARCHAR(200);
DECLARE V_DBNM VARCHAR(500);
DECLARE	EOLStr VARCHAR(500);

SET EOLStr ='
';

SET v_str = '';
SET V_DBNM = '';

L1: 
FOR	CSR1 AS 
SELECT *
FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_table_mapping 
WHERE 
	Tab_Map_View_Name =   vi_name 
AND Tab_Map_Src_Tbl = vi_tablename
DO
----------
/* L2 Cursor starts here 
*/

IF	CSR1.SRC_DB_NM <> '' THEN
SET V_DBNM = CSR1.SRC_DB_NM;
ELSE
SET V_DBNM = 'QSIT_APRA2_BRL_RRP_VW';
END IF;

 SET v_Str_Col_List = '';
 SET v_str_where = '';
 
L2: 
FOR	CSR2 AS 


  SELECT 
 Col_Map_Src_Tbl, Col_Map_Src_Col AS ATTRB
FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping
WHERE Col_Map_View_Name =vi_name
AND Col_Map_Type IN ('ColumnMap','StaticLogic')
AND 
(Col_Map_Src_Tbl   = vi_tablename )
--OR Col_Map_Src_Col  = vi_tablename )             

UNION 

SEL Join_Cond_Left_Tbl, Join_Cond_Left_Col FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
WHERE Join_Cond_View_Name =vi_name
AND Join_Cond_Left_Tbl = vi_tablename

UNION 

SEL Join_Cond_Right_Tbl, Join_Cond_Right_Col FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_join_condition
WHERE Join_Cond_View_Name =vi_name
AND Join_Cond_Right_Tbl = vi_tablename

UNION


SEL Whr_Tbl, Whr_Col FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause
WHERE view_name  =vi_name
AND Whr_Tbl = vi_tablename


DO
----------
 --SET v_str = v_str||  '( SELECT ' ||CSR1.ATTRB||' FROM QSIT_APRA2_BRL_RRP_VW.'|| CSR1.Phy_Tbl_Nm ||' WHERE '||EOLStr;

  SET v_Str_Col_List = v_Str_Col_List ||'
	,' ||CSR2.ATTRB;
/*	
L3: 
FOR	CSR3 AS 
	SELECT Whr_Tbl, Whr_Col, Whr_Join, Whr_Crit  --Whr_Tbl||'.'||Whr_Col||' '||Whr_Join||' '''||Whr_Crit||'''' AS WHR
	FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause 
	WHERE	View_Name =   vi_name
	AND Whr_Tbl = vi_tablename
	GROUP BY 1, 2, 3, 4
	ORDER BY 1, 2, 3, 4

	
DO
-----------	
IF (CSR3.Whr_Col = CSR2.ATTRB AND CSR3.Whr_Col <> CSR3.Whr_Crit) THEN
*/   

 ----------
--END IF;
--END FOR L3;

END FOR L2;

CALL QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_WHERE_QSIT(vi_Name,vi_tablename, output_where,return_code_where, return_message_where);
  
  SET v_str_where = v_str_where || output_where;

IF v_Str_Col_List = '' THEN 
SET v_Str_Col_List ='*' ; END IF; -- will be populated here 
/* L2 Cursor ends here 
*/
--SET v_str = '( SELECT '||v_Str_Col_List||' FROM QSIT_APRA2_BRL_RRP_VW.'|| CSR1.Phy_Tbl_Nm ||' WHERE '||EOLStr;
SET v_str =  '( SELECT 
 	'|| SUBSTR(v_Str_Col_List, INDEX(v_Str_Col_List, ',')+1)||' FROM '||V_DBNM||'.'|| CSR1.PHY_TBL_NM ;

CASE CSR1.Tab_Map_Fltr_Typ
	WHEN 'CL' THEN
		SET v_str = v_str ||CSR1.Tab_Map_Fltr_Col ||' '||CSR1.Tab_Map_Fltr_Cond || ' 
		(SELECT CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL WHERE CL_SCHM_NM = '''||CSR1.Tab_Map_Schm_Nm||''' AND CL_CD = '''||CSR1.Tab_Map_CL_Cd||''')'||v_str_where;
	WHEN 'RAW' THEN	
		SET v_str = v_str || CSR1.Tab_Map_Fltr_Crit||v_str_where;
	WHEN 'AU' THEN 
		SET v_str = v_str ||
		'    AR_TO_AU_RL_TY_ID = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''AR / AU RLTNP TYPE'' AND CL_RL.CL_CD ='''||CSR1.Tab_Map_AU_RL_Typ ||''')' ||v_str_where||EOLStr||
		'AND AU_TY_ID          = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''ACCOUNTING UNIT TYPE'' AND CL_RL.CL_CD ='''||CSR1.Tab_Map_AU_Typ ||''') '||v_str_where||EOLStr||
		'AND SCENR_ID          = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''SCENARIO'' AND CL_RL.CL_CD ='''||CSR1.Tab_Map_AU_Schm ||''') '||v_str_where||EOLStr||
		'AND UNT_OF_MSR_ID     = (SEL CL_CD_ID FROM QSIT_APRA2_BRL_RRP_VW.T_CL CL_RL WHERE CL_RL.CL_SCHM_NM = ''UNIT OF MEASURE TYPE'' AND CL_RL.CL_CD ='''||CSR1.Tab_Map_AU_Msr ||''') '||v_str_where;
	ELSE	
		SET v_str = v_str ||' '||v_str_where;
END CASE;		

----------
END FOR L1;

SET ov_str = v_str || ') AS '|| vi_tablename;
END;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_SELECT 
-- =============================================
-- AUTHOR             :               PRALHAD KAMATH
-- CREATE DATE   :               23  DEC  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- =============================================
-- Stored Procedure Parameters
(
IN vname VARCHAR (32), 
OUT o_count VARCHAR(10000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL */
OUT ORETURN_MESSAGE           VARCHAR(250)
)
MAIN:
BEGIN	

-- Declare variables
DECLARE	v_str VARCHAR(10000);
DECLARE	SqlStr VARCHAR(500);
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);

-- Error Handler variables
DECLARE vSQL_Code                   INTEGER;
DECLARE vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE vError_Text                 VARCHAR(1999);

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	--
	-- Preserve Diagnostic Codes from errored Statement
	--
	SET vSQL_Code  = 0;
	SET vSQL_State =  0;
	SET vError_Text = ' ';
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;

	SET oReturn_Code = 1;
	SET oReturn_Message = 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

/* ----------------------------------- Perform Error Checking --------------------------------------------------------------------------------*/

IF COALESCE (vname, '') NOT IN (SELECT Col_Map_View_Name FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping WHERE Col_Map_View_Name = vname GROUP BY 1)
THEN
	SET VSQL_CODE  = 5;
	SET VSQL_STATE = 1005;  
	SET VERROR_TEXT = 'Invalid View Name provided from - SELECT statement';
	--LOG INTO ERROR TABLE
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
END IF;

SET v_str = '';

--LOGIC OF SELECT STATEMENT
L1: 
FOR	CSR1 AS 
	SELECT	CASE Col_Map_Type             
	WHEN 'StaticLogic' 
	THEN Col_Map_Fmt
	WHEN 'ColumnMap' 
	THEN TRIM(Col_Map_Src_Tbl) ||'.'||TRIM(Col_Map_Src_Col)
	WHEN 'StaticVal' 
	THEN ''''||Col_Map_Fmt||''''
	WHEN 'NullMap' 
	THEN 'NULL'
	END AS ATTRB ,
	Col_Map_Tgt_Col ,
	Col_Map_Comment
	FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping   
	WHERE	Col_Map_View_Name = vname
	AND Col_Map_Tgt_Col NOT IN ('*')
DO
	----------
	SET v_str = v_str ||'
	,' ||CSR1.ATTRB||' AS ' || CSR1.Col_Map_Tgt_Col ;

	IF COALESCE(CSR1.Col_Map_Comment,'') <> '' THEN
		SET v_str = v_str || '          --'||CSR1.Col_Map_Comment;
	END IF;

	----------
END FOR L1;

-- --LOG INTO ERROR TABLE
IF COALESCE (vname, '')  IN (SELECT Col_Map_View_Name FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping
WHERE Col_Map_Tgt_Col <> ' ' AND Col_Map_Type <> ' ' AND Col_Map_View_Name = vname)
THEN
	SET o_count = 'REPLACE VIEW QSIT_APRA2_BRL_RRP_GEN_VW.'||vname ||
	' AS 
	LOCKING ROW FOR ACCESS
	SELECT 
	'|| SUBSTR(v_str, INDEX(v_str, ',')+1);
	SET oReturn_Code = 0;
	SET oReturn_Message =  'Select statement - Success';
	LEAVE MAIN;

	IF COALESCE (vname, '')  IN (SELECT Col_Map_View_Name FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping
	WHERE 1=1 AND (Col_Map_Tgt_Col = '' OR Col_Map_Type = '') AND Col_Map_View_Name = vname)
	THEN
		SET VSQL_CODE  = 20;
		SET VSQL_STATE = 1020;  
		SET VERROR_TEXT =  'Either Attribute Col_Map_Tgt_Col or Col_Map_Type is Blank - Select statement';
		INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
		SET oReturn_Code = 20;
		SET oReturn_Message = 'Either Attribute Col_Map_Tgt_Col or Col_Map_Type is Blank - Select statement';
		LEAVE MAIN;
	ELSE 
		SET oReturn_Code = 30;
		SET oReturn_Message = 'Check Log_Table_BRL for error';
		LEAVE MAIN;
	END IF;
END IF;
END MAIN;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_SELECT_QSIT 
-- =============================================
-- AUTHOR             :               PRALHAD KAMATH
-- CREATE DATE   :               23  DEC  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- =============================================
-- Stored Procedure Parameters
(
IN vname VARCHAR (32), 
OUT o_count VARCHAR(10000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL */
OUT ORETURN_MESSAGE           VARCHAR(250)
)
MAIN:
BEGIN	

-- Declare variables
DECLARE	v_str VARCHAR(10000);
DECLARE	SqlStr VARCHAR(500);
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);

-- Error Handler variables
DECLARE vSQL_Code                   INTEGER;
DECLARE vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE vError_Text                 VARCHAR(1999);

DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
	--
	-- Preserve Diagnostic Codes from errored Statement
	--
	SET vSQL_Code  = 0;
	SET vSQL_State =  0;
	SET vError_Text = ' ';
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;

	SET oReturn_Code = 1;
	SET oReturn_Message = 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

/* ----------------------------------- Perform Error Checking --------------------------------------------------------------------------------*/

IF COALESCE (vname, '') NOT IN (SELECT Col_Map_View_Name FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping WHERE Col_Map_View_Name = vname GROUP BY 1)
THEN
	SET VSQL_CODE  = 5;
	SET VSQL_STATE = 1005;  
	SET VERROR_TEXT = 'Invalid View Name provided from - SELECT statement';
	--LOG INTO ERROR TABLE
	INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
END IF;

SET v_str = '';

--LOGIC OF SELECT STATEMENT
L1: 
FOR	CSR1 AS 
	SELECT	CASE Col_Map_Type             
	WHEN 'StaticLogic' 
	THEN Col_Map_Fmt
	WHEN 'ColumnMap' 
	THEN TRIM(Col_Map_Src_Tbl) ||'.'||TRIM(Col_Map_Src_Col)
	WHEN 'StaticVal' 
	THEN ''''||Col_Map_Fmt||''''
	WHEN 'NullMap' 
	THEN 'NULL'
	END AS ATTRB ,
	Col_Map_Tgt_Col ,
	Col_Map_Comment
	FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping   
	WHERE	Col_Map_View_Name = vname
	AND Col_Map_Tgt_Col NOT IN ('*')
DO
	----------
	SET v_str = v_str ||'
	,' ||CSR1.ATTRB||' AS ' || CSR1.Col_Map_Tgt_Col ;

	IF COALESCE(CSR1.Col_Map_Comment,'') <> '' THEN
		SET v_str = v_str || '          --'||CSR1.Col_Map_Comment;
	END IF;

	----------
END FOR L1;

-- --LOG INTO ERROR TABLE
IF COALESCE (vname, '')  IN (SELECT Col_Map_View_Name FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping
WHERE Col_Map_Tgt_Col <> ' ' AND Col_Map_Type <> ' ' AND Col_Map_View_Name = vname)
THEN
	SET o_count = 'REPLACE VIEW QSIT_APRA2_BRL_RRP_GEN_VW.'||vname ||
	' AS 
	LOCKING ROW FOR ACCESS
	SELECT 
	'|| SUBSTR(v_str, INDEX(v_str, ',')+1);
	SET oReturn_Code = 0;
	SET oReturn_Message =  'Select statement - Success';
	LEAVE MAIN;

	IF COALESCE (vname, '')  IN (SELECT Col_Map_View_Name FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping
	WHERE 1=1 AND (Col_Map_Tgt_Col = '' OR Col_Map_Type = '') AND Col_Map_View_Name = vname)
	THEN
		SET VSQL_CODE  = 20;
		SET VSQL_STATE = 1020;  
		SET VERROR_TEXT =  'Either Attribute Col_Map_Tgt_Col or Col_Map_Type is Blank - Select statement';
		INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL VALUES (vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);
		SET oReturn_Code = 20;
		SET oReturn_Message = 'Either Attribute Col_Map_Tgt_Col or Col_Map_Type is Blank - Select statement';
		LEAVE MAIN;
	ELSE 
		SET oReturn_Code = 30;
		SET oReturn_Message = 'Check Log_Table_BRL for error';
		LEAVE MAIN;
	END IF;
END IF;
END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
REPLACE	PROCEDURE  QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_VLD
-- =============================================
-- AUTHOR             :               DEEPAK PATHAK
-- CREATE DATE   :               21  MARCH  2015
-- DESCRIPTION   :               CONSUMPTION LAYER
-- VERSION            :               1.0V
-- =============================================
-- =============================================
--    Change log
--  
-- =============================================
-- Stored Procedure Parameters
(
IN vi_name VARCHAR (32),  
OUT ov_str VARCHAR(10000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL*/
OUT ORETURN_MESSAGE           VARCHAR(100)
)
MAIN:
BEGIN	

-- Declare variables
DECLARE	v_str VARCHAR(10000);
DECLARE	oSubReturn_Code             SMALLINT;
DECLARE	oSubReturn_Message          VARCHAR(1000);

 DECLARE	EOLStr VARCHAR(5);
 
 
 SET EOLStr ='
';

 
 SET 	v_str = '';
--MATCH ALL THE COLUMNS OF A VIEW WITH DBC.COLUMNS TABLE
L1: 
FOR	CSR1 AS 
   SEL 
cl.ColumnName 
--BRLMD.Col_Map_Tgt_Col  
FROM DBC.COLUMNS cl
WHERE 
cl.ColumnName NOT IN
(SEL BRLMD.Col_Map_Tgt_Col FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping      BRLMD
WHERE BRLMD.Col_Map_View_Name =vi_name
 OR         BRLMD.Col_Map_View_Name IS NULL
)
AND  cl.TABLENAME  IN
(
SEL	DISTINCT BRLMD.Col_Map_Tgt_Tbl 
FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping    BRLMD
WHERE Col_Map_View_Name = vi_name
)
AND cl.DatabaseName = 'QSIT_APRA2_BRL_RRP_T' 
 
AND	cl.ColumnName NOT IN ('BSNS_DT','ETL_SRC_VIEW_NM') 
 
	 
DO
----------
 
	IF	CSR1.ColumnName IS NOT NULL  THEN 
		SET v_Str =  v_Str ||EOLStr|| TRIM(CSR1.ColumnName)|| ' does not exist in Mapping ';----------
 ELSE
 SET v_Str = '';
				END IF;
----------
END FOR	L1;


/*IF v_str <>'' THEN
	SET  ov_str = v_str ;
	SET	oReturn_Code = 1;
	SET	oReturn_Message =  'Validation Failed';

ELSE
	SET	ov_str = 'All Attributes are Present' ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'Validation Success';
	
	END IF;
*/
--MATCH ALL THE COLUMNS OF A VIEW WITH DBC.COLUMNS TABLE
L2: 
FOR	CSR1 AS 
 SEL 
BRLMD.Col_Map_Tgt_Col  
FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Column_Mapping    BRLMD
WHERE BRLMD.Col_Map_View_Name   = vi_name
AND BRLMD.Col_Map_Tgt_Col NOT IN
(SEL  cl.ColumnName FROM dbc.COLUMNS    cl
WHERE cl.TABLENAME = (SELECT Col_Map_Tgt_Tbl FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_column_mapping WHERE Col_Map_View_Name = vi_name GROUP BY 1)

 
 AND cl.DatabaseName = 'QSIT_APRA2_BRL_RRP_T' 
 AND	cl.ColumnName NOT IN ('BSNS_DT','ETL_SRC_VIEW_NM') -------------------------------
) 

	 
DO
----------
 
	IF	CSR1.Col_Map_Tgt_Col IS NOT NULL  THEN 
		SET v_Str = v_Str ||EOLStr||  TRIM(CSR1.Col_Map_Tgt_Col)|| ' does not exist in database';----------------
 ELSE
 SET v_Str = '';
				END IF;
----------
END FOR	L2;


IF v_str <>'' THEN
	SET  ov_str = v_str ;
	SET	oReturn_Code = 1;
	SET	oReturn_Message =  'Validation Failed';

ELSE
	SET	ov_str = 'All Attributes are Present' ;
	SET	oReturn_Code = 0;
	SET	oReturn_Message =  'Validation Success';
END IF;	

 
END MAIN;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_WHERE
-- =============================================
-- AUTHOR           :               PRALHAD KAMATH
-- CREATE DATE   	:               23  DEC  2015
-- DESCRIPTION   	:               CONSUMPTION LAYER
-- VERSION          :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- [23 Mar 2016]: Fixed it to append a single quotes to each token if multiple comma separated values are passed in Criteria field
-- (31 Marc 2016): Added condition to handle no values in where table
-- (31 May 2016) : Added RAW join condition which bypasses all validations and is to be used when all else fails
-- =============================================
-- Stored Procedure Parameters
(
IN vname VARCHAR (32), 
OUT o_count VARCHAR(1000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL*/
OUT ORETURN_MESSAGE                VARCHAR(250)
)
MAIN:
BEGIN	
-- Declare variables
DECLARE	v_str VARCHAR(1000);
DECLARE	SqlStr VARCHAR(500);
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);
-- Error Handler variables
DECLARE	vSQL_Code                   INTEGER;
DECLARE	vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE	vError_Text                 VARCHAR(1999);
DECLARE vWhr_Crit VARCHAR(1000) DEFAULT '';
DECLARE vCnt SMALLINT DEFAULT 0;
DECLARE	vCOUNT                   INTEGER;

DECLARE	EXIT HANDLER FOR SQLEXCEPTION
BEGIN	
	--
	-- Preserve Diagnostic Codes from errored Statement
	--
	SET	vSQL_Code  = 0;
	SET	vSQL_State = 0;
	SET	vError_Text = ' ';
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;

	SET	oReturn_Code = 1;
	SET	oReturn_Message = 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

/* ----------------------------------- Perform Error Checking --------------------------------------------------------------------------------*/

/*******************NO DATA IN WHERE STARTS***************************/
SELECT	COUNT(*) INTO :vCOUNT
FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause 
WHERE View_Name = vname;
		
IF	vCOUNT = 0 
	THEN
		SET	o_count = 'WHERE  1=1'||';';
	    SET	oReturn_Code = 0;
		SET	oReturn_Message = 'No where condition mentioned';
		LEAVE MAIN;
END IF	;

/*******************NO DATA IN WHERE ENDS***************************/

SET	v_str = '';
--LOGIC OF WHERE STATEMENT
L1: 
FOR	CSR1 AS 
	SELECT Whr_Tbl, Whr_Col, Whr_Join, Whr_Crit  --Whr_Tbl||'.'||Whr_Col||' '||Whr_Join||' '''||Whr_Crit||'''' AS WHR
	FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause 
	WHERE	View_Name =   vname
	GROUP BY 1, 2, 3, 4
	ORDER BY 1, 2, 3, 4


 DO
----------
SET vCnt = 1;
SET vWhr_Crit = '';
--- CHANGES FOR RAW JOIN
IF CSR1.Whr_Join <> 'RAW' THEN 
--- END CHANGES FOR RAW JOIN
L2:
WHILE STRTOK(CSR1.Whr_Crit, ',', vCnt) IS NOT NULL DO
BEGIN
	
	SET vWhr_Crit = vWhr_Crit || 
CASE	
	WHEN vCnt = 1 THEN '' 
	ELSE ''',''' 
end	|| TRIM(STRTOK(CSR1.Whr_Crit, ',', vCnt));
	SET vCnt = vCnt + 1;

END;
END WHILE L2;

CASE 
	WHEN CSR1.Whr_Col<>CSR1.Whr_Crit  -- LHS = RHS are skipped here; they are added to allow select to work properly
	AND CSR1.Whr_Join = 'IS' THEN 
SET	v_str = v_str ||'AND '||CSR1.Whr_Tbl||'.'||CSR1.Whr_Col||' '||CSR1.Whr_Join||' '||vWhr_Crit||'
';
	WHEN CSR1.Whr_Col<>CSR1.Whr_Crit  THEN									--	Added Logic for Inline View
SET	v_str = v_str ||'AND '||CSR1.Whr_Tbl||'.'||CSR1.Whr_Col||' '||CSR1.Whr_Join||' ('''||vWhr_Crit||''')'||'
';
	ELSE 
SET v_str = v_str ;
END CASE; 
--- CHANGES FOR RAW JOIN
ELSE


SET v_str = v_str || 'AND '|| CSR1.Whr_Crit||'
';

END IF;
--- END CHANGES FOR RAW JOIN
---------
END FOR	L1;

IF	vname   IN (
	SELECT	COALESCE (View_Name, '') 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause
	WHERE	1=1 
	AND	Whr_Tbl<>'' 
	AND	Whr_Col<>'' 
	AND	Whr_Join<>'' 
	AND	Whr_Crit<>''
--	AND TRIM(Whr_Col) <> TRIM(Whr_Crit)
	AND View_Name = vname
	GROUP BY 1)
THEN
	SET	o_count = 'WHERE  1=1
'|| v_str||'  ;';

	IF vname   IN (
		SELECT	COALESCE (View_Name, '') 
		FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause
		WHERE 1=1 
		--- CHANGES FOR RAW JOIN
		AND Whr_Join <> 'RAW'
		---  END CHANGES FOR RAW JOIN
		AND	(Whr_Tbl='' 
		OR	Whr_Col='' 
		OR	Whr_Join='' 
		OR	Whr_Crit='')
		AND View_Name = vname
		GROUP BY 1)
		THEN
		SET VSQL_CODE  = 20;
		SET VSQL_STATE = 1020;  
		SET VERROR_TEXT ='Either of the input attributes is Blank - WHERE statement';

		INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL 
VALUES	(vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);

		SET oReturn_Code = 20;
		SET oReturn_Message ='Either of the input attributes is Blank - WHERE statement';
		LEAVE MAIN;
	END IF;
	ELSE	
	SET	oReturn_Message = 'Invalid View Name provided from WHERE statement';
	LEAVE MAIN;
END IF	;

SET	oReturn_Code = 0;
SET	oReturn_Message = 'WHERE Statement - Success';
END MAIN;
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BRLMD_RTN_WHERE_QSIT
-- =============================================
-- AUTHOR           :               PRALHAD KAMATH
-- CREATE DATE   	:               23  DEC  2015
-- DESCRIPTION   	:               CONSUMPTION LAYER
-- VERSION          :               1.1V
-- =============================================
-- =============================================
--    Change log
-- [28 JAN 2016]: Modified to add Error Handeling
-- [23 Mar 2016]: Fixed it to append a single quotes to each token if multiple comma separated values are passed in Criteria field
-- (31 Marc 2016): Added condition to handle no values in where table
-- (31 May 2016) : Added RAW join condition which bypasses all validations and is to be used when all else fails
-- =============================================
-- Stored Procedure Parameters
(
IN vname VARCHAR (32), 
IN vi_tablename VARCHAR(32),
OUT o_count VARCHAR(1000),
OUT ORETURN_CODE                   SMALLINT,             /* 0: SUCCESSFUL*/
OUT ORETURN_MESSAGE                VARCHAR(250)
)
MAIN:
BEGIN	
-- Declare variables
DECLARE	v_str VARCHAR(1000);
DECLARE	SqlStr VARCHAR(500);
DECLARE oSubReturn_Code             SMALLINT;
DECLARE oSubReturn_Message          VARCHAR(1000);
-- Error Handler variables
DECLARE	vSQL_Code                   INTEGER;
DECLARE	vSQL_State                  VARCHAR(6) DEFAULT ''XC;
DECLARE	vError_Text                 VARCHAR(1999);
DECLARE vWhr_Crit VARCHAR(1000) DEFAULT '';
DECLARE vCnt SMALLINT DEFAULT 0;
DECLARE	vCOUNT                   INTEGER;

DECLARE	EXIT HANDLER FOR SQLEXCEPTION
BEGIN	
	--
	-- Preserve Diagnostic Codes from errored Statement
	--
	SET	vSQL_Code  = 0;
	SET	vSQL_State = 0;
	SET	vError_Text = ' ';
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;

	SET	oReturn_Code = 1;
	SET	oReturn_Message = 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

/* ----------------------------------- Perform Error Checking --------------------------------------------------------------------------------*/

/*******************NO DATA IN WHERE STARTS***************************/
SELECT	COUNT(*) INTO :vCOUNT
FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause 
WHERE View_Name = vname;
		
IF	vCOUNT = 0 
	THEN
		SET	o_count = 'WHERE  1=1';
	    SET	oReturn_Code = 0;
		SET	oReturn_Message = 'No where condition mentioned';
		LEAVE MAIN;
END IF	;

/*******************NO DATA IN WHERE ENDS***************************/

SET	v_str = '';
--LOGIC OF WHERE STATEMENT
L1: 
FOR	CSR1 AS 
	SELECT Whr_Tbl, Whr_Col, Whr_Join, Whr_Crit  --Whr_Tbl||'.'||Whr_Col||' '||Whr_Join||' '''||Whr_Crit||'''' AS WHR
	FROM QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause 
	WHERE	View_Name =   vname
	AND Whr_Tbl = vi_tablename
	GROUP BY 1, 2, 3, 4
	ORDER BY 1, 2, 3, 4


 DO
----------
SET vCnt = 1;
SET vWhr_Crit = '';
--- CHANGES FOR RAW JOIN
IF CSR1.Whr_Join <> 'RAW' THEN 
--- END CHANGES FOR RAW JOIN
L2:
WHILE STRTOK(CSR1.Whr_Crit, ',', vCnt) IS NOT NULL DO
BEGIN
	
	SET vWhr_Crit = vWhr_Crit || 
CASE	
	WHEN vCnt = 1 THEN '' 
	ELSE ''',''' 
end	|| TRIM(STRTOK(CSR1.Whr_Crit, ',', vCnt));
	SET vCnt = vCnt + 1;

END;
END WHILE L2;

CASE 
	WHEN CSR1.Whr_Col<>CSR1.Whr_Crit  -- LHS = RHS are skipped here; they are added to allow select to work properly
	AND CSR1.Whr_Join = 'IS' THEN 
SET	v_str = v_str ||'AND '||CSR1.Whr_Tbl||'.'||CSR1.Whr_Col||' '||CSR1.Whr_Join||' '||vWhr_Crit||'
';
	WHEN CSR1.Whr_Col<>CSR1.Whr_Crit  THEN									--	Added Logic for Inline View
SET	v_str = v_str ||'AND '|| CSR1.Whr_Col||' '||CSR1.Whr_Join||' ('''||vWhr_Crit||''')'||'
';
 

	ELSE 
SET v_str = v_str ;
END CASE; 
--- CHANGES FOR RAW JOIN
ELSE


SET v_str = v_str || 'AND '|| CSR1.Whr_Crit||'
';

END IF;
--- END CHANGES FOR RAW JOIN
---------
END FOR	L1;

IF	vname   IN (
	SELECT	COALESCE (View_Name, '') 
	FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause
	WHERE	1=1 
	AND	Whr_Tbl<>'' 
	AND	Whr_Col<>'' 
	AND	Whr_Join<>'' 
	AND	Whr_Crit<>''
--	AND TRIM(Whr_Col) <> TRIM(Whr_Crit)
	AND View_Name = vname
	GROUP BY 1)
THEN
	SET	o_count = 'WHERE  1=1
'|| v_str;

	IF vname   IN (
		SELECT	COALESCE (View_Name, '') 
		FROM	QSIT_APRA2_BRL_RRP_VW.BRLMD_Whr_Clause
		WHERE 1=1 
		--- CHANGES FOR RAW JOIN
		AND Whr_Join <> 'RAW'
		---  END CHANGES FOR RAW JOIN
		AND	(Whr_Tbl='' 
		OR	Whr_Col='' 
		OR	Whr_Join='' 
		OR	Whr_Crit='')
		AND View_Name = vname
		GROUP BY 1)
		THEN
		SET VSQL_CODE  = 20;
		SET VSQL_STATE = 1020;  
		SET VERROR_TEXT ='Either of the input attributes is Blank - WHERE statement';

		INSERT INTO QSIT_APRA2_BRL_RRP_VW.Log_Table_BRL 
VALUES	(vSQL_Code, vSQL_State, vError_Text, CURRENT_TIMESTAMP);

		SET oReturn_Code = 20;
		SET oReturn_Message ='Either of the input attributes is Blank - WHERE statement';
		LEAVE MAIN;
	END IF;
	ELSE	
	SET	oReturn_Message = 'Invalid View Name provided from WHERE statement';
	LEAVE MAIN;
END IF	;

SET	oReturn_Code = 0;
SET	oReturn_Message = 'WHERE Statement - Success';
END MAIN;
--------------------------------------------------------------------------------

REPLACE MACRO QSIT_APRA2_BRL_RRP_PGM.BSNSMD_DELETE_OBSOLETE_RULES (
-- This macro will delete records from all config tables for a obsolete rule id
Rul_Id INTEGER
)
AS (

DELETE FROM QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL WHERE RUL_ID = :Rul_Id;
INSERT INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_LOG VALUES (SESSION, USER, 'BSNSMD_DELETE_OBSOLETE_RULES', 100, 'For Rul_Id = ' || TRIM(:Rul_Id) || ' Rows deleted from RUL_TRG_DTL : ', CURRENT_TIMESTAMP(6));

DELETE FROM QSIT_APRA2_BRL_RRP_VW.LST_VAL WHERE LST_ID IN (SELECT LST_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR WHERE RUL_ID  = :Rul_Id GROUP BY 1);
INSERT INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_LOG VALUES (SESSION, USER, 'BSNSMD_DELETE_OBSOLETE_RULES', 200, 'For Rul_Id = ' || TRIM(:Rul_Id) || ' Rows deleted from LST_VAL : ', CURRENT_TIMESTAMP(6));

DELETE FROM QSIT_APRA2_BRL_RRP_VW.LST_RNG WHERE LST_ID IN (SELECT LST_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR WHERE RUL_ID  = :Rul_Id GROUP BY 1);
INSERT INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_LOG VALUES (SESSION, USER, 'BSNSMD_DELETE_OBSOLETE_RULES', 300, 'For Rul_Id = ' || TRIM(:Rul_Id) || ' Rows deleted from LST_RNG : ', CURRENT_TIMESTAMP(6));

DELETE FROM QSIT_APRA2_BRL_RRP_VW.LST_MSTR WHERE LST_ID IN (SELECT LST_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR WHERE RUL_ID  = :Rul_Id GROUP BY 1);
INSERT INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_LOG VALUES (SESSION, USER, 'BSNSMD_DELETE_OBSOLETE_RULES', 400, 'For Rul_Id = ' || TRIM(:Rul_Id) || ' Rows deleted from LST_MSTR : ', CURRENT_TIMESTAMP(6));

DELETE FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR WHERE RUL_ID  = :Rul_Id;
INSERT INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_LOG VALUES (SESSION, USER, 'BSNSMD_DELETE_OBSOLETE_RULES', 500, 'For Rul_Id = ' || TRIM(:Rul_Id) || ' Rows deleted from RUL_ATTR : ', CURRENT_TIMESTAMP(6));

DELETE FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR WHERE RUL_ID  = :Rul_Id;
INSERT INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_LOG VALUES (SESSION, USER, 'BSNSMD_DELETE_OBSOLETE_RULES', 600, 'For Rul_Id = ' || TRIM(:Rul_Id) || ' Rows deleted from RUL_MSTR : ', CURRENT_TIMESTAMP(6));

);
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG
-- =============================================
-- Description: This procedure will make execute SQL statement and make an entry into log table for both success and failure
-- Change log
--      [2015 01 26]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iStrdPrcdr_Nm VARCHAR(255),
 IN iStep_No SMALLINT,
 IN iMsg CLOB,
 IN iDebug_Lvl SMALLINT, /* 0  = highest level logging; 5 = verbose level logging */
 OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
 OUT oReturn_Message VARCHAR(1000)
) 
MAIN:
BEGIN
-- Declare variables
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE vDebug_Lvl SMALLINT;

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    SET oReturn_Code = 2;
    SET oReturn_Message = ' Call to SP failed. SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

-- Select the environment debug level into a variable
SELECT CAST(PRMTR_VAL AS SMALLINT) INTO vDebug_Lvl FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S WHERE PRMTR_NM = 'DEBUG_LVL';

IF iDebug_Lvl <= vDebug_Lvl THEN  /* ensure that input debug level is greater or equal to env debug level eg. input high & env medium should return true */
SET oReturn_Code = 0;
SET oReturn_Message = 'Successfully Completed - Entry made in log table. ';
INSERT INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_LOG (
       Session_Id,
       Usid,
       StrdPrcdr_Nm,
       Step_No,
       Msg,
       Log_Ts
) VALUES
(      SESSION,
        USER,
        iStrdPrcdr_Nm,
        iStep_No,
        iMsg,
        CURRENT_TIMESTAMP(6)
);
ELSE
SET oReturn_Code = 0;
SET oReturn_Message = 'Successfully Completed - Entry not made as input debug level was less than environment debug level. ';
END IF;

END MAIN;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_GEN_OVRD_VIEWS
-- =============================================
-- DESCRIPTION: THIS PROCEDURE WILL CREATE THE ATTRIBUTE OVERRIDE VIEWS
-- CHANGE LOG
--      [2016 03 16]: INITIAL VERSION 
-- =============================================
-- STORED PROCEDURE PARAMETERS

(
 IN ISRC_TBLNM VARCHAR(40),
 IN IEXEC_MODE CHAR(1),
 OUT ORETURN_CODE SMALLINT, /* 0: SUCCESSFUL; NON-ZERO: ERROR */
 OUT ORETURN_MESSAGE VARCHAR(1000),
 OUT OSQL_TEXT VARCHAR(50000)
 ) 
 MAIN:
BEGIN
-- DECLARE VARIABLES
DECLARE VSQL_CODE 			INTEGER;
DECLARE VSQL_STATE 			VARCHAR(6);
DECLARE VERROR_TEXT 		VARCHAR(256);
DECLARE VSQL_TEXT 			VARCHAR(10000);
DECLARE VSQL_ONTEXT 		VARCHAR(10000);
DECLARE EOLSTR 				VARCHAR(2) ;
DECLARE OSUBRETURN_CODE 	INTEGER;
DECLARE OSUBRETURN_MESSAGE 	VARCHAR(1000);
DECLARE VLOOPCOUNT 			INTEGER;
DECLARE VCOMMA 				CHAR(1);
DECLARE VSRC_DB_NM 			VARCHAR(40);
DECLARE VTRG_DB_NM 			VARCHAR(40);
DECLARE VSRC_ENT_ID 		INTEGER;
DECLARE VATTR_NM 			VARCHAR(40);
DECLARE VSRC_DB 			VARCHAR(40);
DECLARE VSRC_TBNM 			VARCHAR(40);
DECLARE VTGT_TBNM 			VARCHAR(40);
DECLARE VATTR_OVRD_TBNM 	VARCHAR(40);
DECLARE VCOUNT 				INTEGER;
DECLARE	vBRL_VW				VARCHAR(50);

--DECLARE RETURN_CODE_CREATE SMALLINT;

-- ERROR HANDLER
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET VSQL_CODE  = SQLCODE;
    SET VSQL_STATE = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 VERROR_TEXT = MESSAGE_TEXT;
    SET ORETURN_CODE = 2;
    SET ORETURN_MESSAGE = ' CALL TO SP FAILED. SQL EXCEPTION, SQLCODE = '||TRIM(VSQL_CODE)||', SQLSTATE = '||VSQL_STATE||', ERROR MSG = '||VERROR_TEXT;
END;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_GEN_OVRD_VIEWS', 0, 'STARTED - ISRC_TBLNM '||ISRC_TBLNM, 0, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);

SET EOLSTR	= '
';

SET VSQL_TEXT = '';
SET VSQL_ONTEXT = '';
SET VLOOPCOUNT = 0;

SET VSRC_TBNM = 'OUT_'||UPPER(ISRC_TBLNM);
SET VTGT_TBNM = 'EFFV_'||VSRC_TBNM;
SET VATTR_OVRD_TBNM = 'ATTR_OVRD_'||UPPER(ISRC_TBLNM);

-- FETCH SOURCE DB NAME
SELECT PRMTR_VAL INTO :VSRC_DB_NM FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S  WHERE PRMTR_NM = 'CNSUM_GEN';

-- FETCH SOURCE DB NAME FOR ATTR_OVRD Views
SELECT PRMTR_VAL INTO :vBRL_VW FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S  WHERE PRMTR_NM = 'ATR_OVR_DB';

-- FETCH TARGET DB NAME
SELECT TRIM(PRMTR_VAL) INTO :VTRG_DB_NM FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S WHERE PRMTR_NM = 'GEN_DB_NM' ;

--FETCH ENT_ID FOR THE SOURCE TABLE
SELECT ENT_ID INTO :VSRC_ENT_ID FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR WHERE ENT_NM =  VSRC_TBNM;


CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_GEN_OVRD_VIEWS', 100, 'SOURCE DB NAME =' || VSRC_DB_NM||'TARGET DB NAME =  '|| VTRG_DB_NM||' SOURCE TABLE NAME = '|| VSRC_TBNM||' TARGET TABLE NAME = '|| VTGT_TBNM||'LOOK UP TABLE = '||VATTR_OVRD_TBNM, 2, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);

SET VSQL_TEXT = 'REPLACE VIEW '||VTRG_DB_NM||'.'||VTGT_TBNM||' AS'||EOLSTR||'SELECT';

L1: 
FOR	CSR1 AS
---CURSOR TO SELECT THE OVERRIDE COLUMN.
SELECT
ATTR_NM AS ATTR_NM,
IS_OVRD AS IS_OVRD,
IS_PK AS IS_PK
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR
ORDER BY ATTR_ID
WHERE ENT_ID = VSRC_ENT_ID

DO
----------

SET VLOOPCOUNT = VLOOPCOUNT + 1;

IF VLOOPCOUNT = 1 THEN
	SET VCOMMA = '';
ELSE 
	SET VCOMMA = ',';
END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_GEN_OVRD_VIEWS', 200, 'IN LOOOP L1 CSR1.ATTR_NM '|| CSR1.ATTR_NM, 3, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);

CASE WHEN COALESCE(CSR1.IS_OVRD,'') = 'Y' THEN
SET VSQL_TEXT = VSQL_TEXT || EOLSTR ||VCOMMA||'COALESCE ('||VATTR_OVRD_TBNM||'.'||CSR1.ATTR_NM||','||VSRC_TBNM||'.'||CSR1.ATTR_NM||')'||' AS '||CSR1.ATTR_NM|| EOLSTR ||','||VSRC_TBNM||'.'||CSR1.ATTR_NM||' AS'||' ORG_'||CSR1.ATTR_NM;
ELSE
SET VSQL_TEXT = VSQL_TEXT || EOLSTR ||VCOMMA||VSRC_TBNM||'.'||CSR1.ATTR_NM;
END CASE;

IF COALESCE(CSR1.IS_PK,'') = 'Y' THEN
SET VSQL_ONTEXT = VSQL_ONTEXT||EOLSTR||'AND '||VSRC_TBNM||'.'||CSR1.ATTR_NM||' = '||VATTR_OVRD_TBNM||'.'||CSR1.ATTR_NM;

END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_GEN_OVRD_VIEWS', 300, 'VSQL_TEXT '|| VSQL_TEXT, 3, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);	

----------
END FOR	L1;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_GEN_OVRD_VIEWS', 300, 'VSQL_TEXT '|| VSQL_TEXT, 3, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);	

/******************************************************************************************************/

SELECT COUNT(*) INTO :VCOUNT FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR WHERE ENT_ID = VSRC_ENT_ID AND IS_OVRD = 'Y';

IF VCOUNT = 0 THEN 
SET VSQL_TEXT = VSQL_TEXT||EOLSTR||'FROM '||VSRC_DB_NM||'.'||VSRC_TBNM;
ELSE
SET VSQL_TEXT = VSQL_TEXT||EOLSTR||'FROM '||VSRC_DB_NM||'.'||VSRC_TBNM||EOLSTR ||'LEFT OUTER JOIN '||vBRL_VW||'.'||VATTR_OVRD_TBNM||EOLSTR||'ON  1=1' ;
SET VSQL_TEXT = VSQL_TEXT||VSQL_ONTEXT;
END IF;

/******************************************************************************************************/

SET OSQL_TEXT = VSQL_TEXT||';';


SET ORETURN_CODE = 0;
SET ORETURN_MESSAGE = 'SUCCESSFULLY COMPLETED ';

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_GEN_OVRD_VIEWS', 0, 'ENDED SP - ISRC_TBLNM '||VSRC_TBNM || 'OSQL_TEXT:'||OSQL_TEXT, 0, OSUBRETURN_CODE, OSUBRETURN_MESSAGE);

    IF (OSUBRETURN_CODE =0)
    THEN

---TO RUN THE SQL GENERATED.	
	IF IEXEC_MODE = 'Y' THEN 
		EXECUTE IMMEDIATE VSQL_TEXT;
		SET ORETURN_CODE = 0;
		SET ORETURN_MESSAGE='SUCCESS - EXECUTED THE GENERATED SQL. PLEASE VERIFY THAT VIEW HAS BEEN CREATED.';
		LEAVE MAIN;
	END IF;
	SET ORETURN_CODE = 0;
	SET ORETURN_MESSAGE='SUCCESS - DID NOT EXECUTE THE GENERATED SQL';
	ELSE
	SET ORETURN_CODE = 5;
	SET ORETURN_MESSAGE= 'SELECT PORTION: ' || OSUBRETURN_MESSAGE;
    END IF; 

END MAIN;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_FROM
-- =============================================
-- Description: This Stored Procedure will dynamically generate the "From" portion of the query.
-- Change log
--      [2016 01 28]: Initial version 
--		[2016 02  5]: Modified Code to include TY in RUL_MSTR Table
-- =============================================
-- Stored Procedure Parameters
 ( IN iRul_Id 				INTEGER,
   IN iCC_Id 				INTEGER,
   OUT oReturn_Code			SMALLINT,			/* 0: Successful; Non-Zero: Error */
   OUT oReturn_Message		VARCHAR(1000),
   OUT oSQL_Text			VARCHAR(5000)
  )

  BEGIN
  
  -- Declare variables
DECLARE cLF 				CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr 				INTEGER DEFAULT 100;
DECLARE vSQL_Code 			INTEGER;
DECLARE vSQL_State 			VARCHAR(6);
DECLARE vError_Text 		VARCHAR(256);
DECLARE oSubReturn_Code 	SMALLINT;
DECLARE oSubReturn_Message 	VARCHAR(1000);
DECLARE vDebugLvl 			SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg 			VARCHAR(1000);
DECLARE vLogMsgFixed 		VARCHAR(1000);
DECLARE vLogMsgVariable 	VARCHAR(1000);
DECLARE vLogSPName 			VARCHAR(255) DEFAULT 'BSNSMD_RTN_FROM';
DECLARE	vSQL_TEXT 			VARCHAR(5000) DEFAULT '';
DECLARE vSQL_TEXT_ST1	    VARCHAR(2000) DEFAULT '';
DECLARE vSQL_TEXT_ST2 		VARCHAR(2000) DEFAULT '';
DECLARE vSQL_TEXT_DN		VARCHAR(2000) DEFAULT '';
DECLARE	vSRC_SCHM_NM		VARCHAR(50);
DECLARE	vTRG_SCHM_NM		VARCHAR(50);
DECLARE	vWK_SCHM_NM		VARCHAR(50);
DECLARE	vSRC_TB_NM			VARCHAR(50);
DECLARE	vALLIAS				VARCHAR(54);
DECLARE vRUL_TY				VARCHAR(50) DEFAULT '';
DECLARE	vSRC_SYS_DFLT		VARCHAR (2);
DECLARE	vATTR_NM			VARCHAR(1000) DEFAULT '';
DECLARE	vTRG_TB_NM			VARCHAR(50);
DECLARE vATTR1 				CHAR(1);
DECLARE vATTR2 				CHAR(1);
DECLARE vCOUNT 				INTEGER;
DECLARE	vATTR_NM1			VARCHAR(1000) DEFAULT '';
DECLARE	vBRL_VW				VARCHAR(50);

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

	SET oSQL_Text = '';
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;


-- Set the fixed part of the log message.
SET vLogMsgFixed =  'For Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id);

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Process - Started';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET cLF	= '
';
-- Fetch Source DB Name

SELECT PRMTR_VAL 
INTO :vBRL_VW 
FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S  
WHERE PRMTR_NM = 'SRC_DB_NM';

--- Setting the Target Database as WK 
 
 SELECT TRIM(PRMTR_VAL) 
 INTO vWK_SCHM_NM 
 FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S 
 WHERE PRMTR_NM = 'WK_DB_NM' ;

  
   -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vBRL_VW - Completed; Value - '||vBRL_VW;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- Select TY from RUL_MSTR into a variable

SELECT DISTINCT 
RM.TY, 
RM.IS_DFLT 
INTO 
:vRUL_TY, 
:vSRC_SYS_DFLT 
FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S RM
WHERE RM.RUL_ID = iRUL_Id
AND	RM.CC_ID  = iCC_Id;

 -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch TY from RUL_MSTR - Completed ; Value - '||vRUL_TY;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- Select IS_DFLT from RUL_MSTR into a variable
--SELECT SOURCE TABLE NAME --- DATA_ENT_ID
SELECT ENT_NM,SCHM_NM 
INTO :vSRC_TB_NM,:vSRC_SCHM_NM
FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S  
WHERE ENT_ID IN
(
SELECT DATA_ENT_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S 
WHERE RUL_ID = iRUL_ID
);

SET vALLIAS = 'SRC_'||vSRC_TB_NM;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vSRC_TB_NM - Completed; Value - '||vSRC_TB_NM;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


 
--HOLD TARGET TABLE NAME INTO A VARIABLE --- TRG_ENT_ID
SEL ENT_NM,SCHM_NM 
 INTO :vTRG_TB_NM,:vTRG_SCHM_NM
FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S  
WHERE  ENT_ID IN
(
SEL TRG_ENT_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S   --DATA_ENT_ID = TRG_ENT_ID IN NEW DM
WHERE RUL_ID = iRUL_ID
)  ;
 
 -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vTRG_TB_NM - Completed; Value - '||vTRG_TB_NM;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

/***********************************************************************************************************************************/
  
  IF vRUL_TY IN ('CP TYPE', 'REDUCING') OR vSRC_SYS_DFLT IN ('Y')
  THEN SET vATTR1 = 'Y';
  ELSE SET vATTR1 = 'N';
  END IF;
  
  -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vATTR1 - Completed; Value - '||vATTR1;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

  
  SELECT COUNT(*)  
  INTO :vCOUNT
  FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S 
  WHERE RUL_ID =iRUL_ID 
  AND LINK_TY = 'LOOKUP' ;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vCOUNT - Completed; Value - '||vCOUNT;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

IF vCOUNT > 0
THEN SET vATTR2 = 'Y';
ELSE SET  vATTR2 = 'N';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vATTR2 - Completed; Value - '||vATTR2;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

/*------------ CP TYPE ------------------*/
 
 IF vATTR1 = 'Y' AND vATTR2 = 'N'
 THEN 
 -- Append keywords to form the complete SQL

SET vATTR_NM = '';
L1:
 --HOLD TARGET TABLE ATTRIBUTE NAME  INTO A CURSOR
FOR CSR1 AS
 SEL DISTINCT ATTR_NM 
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S  WHERE ENT_ID IN
	(
	SEL ENT_ID 
	FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S  WHERE ENT_ID IN
(
SEL TRG_ENT_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S
WHERE RUL_ID =iRUL_ID
)) 
AND IS_PK = 'Y'

DO 
 SET vATTR_NM	=	vATTR_NM||' AND '||vALLIAS||'.'||CSR1.ATTR_NM|| '=' ||vTRG_TB_NM||'.'||CSR1.ATTR_NM; 
 
  -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch ATTR_NM - Completed; Value - '||CSR1.ATTR_NM;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

 
  SET vSQL_TEXT_ST1 = 'FROM  (SEL '||vALLIAS||'.'||'* FROM '||cLF||vSRC_SCHM_NM||'.'||vSRC_TB_NM||' AS '||vALLIAS||cLF||' LEFT OUTER JOIN '||cLF|| vWK_SCHM_NM  ||'.'||vTRG_TB_NM||' ON 1=1 ';
  SET vSQL_TEXT_DN=  vATTR_NM;
  SET vSQL_TEXT_ST2 = 'AND '||vTRG_TB_NM||'.CC_ID = '||TRIM(iCC_ID)||' WHERE  '||vTRG_TB_NM||'.'||CSR1.ATTR_NM||' IS NULL )'||vSRC_TB_NM;
  SET vSQL_TEXT			=	vSQL_TEXT_ST1||vSQL_TEXT_DN||cLF||vSQL_TEXT_ST2;
 
 
  -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Set SQL TEXT - Completed; Value - '||vSQL_TEXT;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
  

 END FOR L1;  
 
END IF;

/*----------------PASSTHRU-----------------*/

 IF vATTR1 = 'N' AND vATTR2 = 'Y'
 THEN
 --HOLD ATTRIBUTE NAME OF SOURCE TABLE

SELECT ATTR_NM 
 INTO :vATTR_NM 
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S 
WHERE ATTR_ID IN
(
SEL ATTR_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S 
WHERE RUL_ID =iRUL_ID AND LINK_TY = 'LOOKUP')
;


-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vATTR_NM - Completed; Value - '||vATTR_NM;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


SET vSQL_TEXT_ST1 = 'FROM  (SELECT '||vALLIAS||'.'||'*, CC_VAL_S.CC_VAL_ID FROM '||vSRC_SCHM_NM||'.'||vSRC_TB_NM||' AS '||vALLIAS
 										||cLF||' INNER JOIN '||cLF||vBRL_VW||'.CC_VAL_S  ON 1=1 ';
  
SET vSQL_TEXT_DN=  'AND '|| vALLIAS||'.'||vATTR_NM||' = CC_VAL_S.CC_VAL_NM';
 
SET vSQL_TEXT_ST2 = 'AND CC_VAL_S.CC_ID ='||iCC_ID||' ) ' || vSRC_TB_NM;
 
SET vSQL_TEXT			=	vSQL_TEXT_ST1||vSQL_TEXT_DN||cLF||vSQL_TEXT_ST2;


 END IF;
 
 /*---------------REDUCING------------------*/
 
 IF vATTR1 = 'Y' AND vATTR2 = 'Y'
 
  THEN

  --HOLD ATTRIBUTE NAME OF SOURCE TABLE

SELECT ATTR_NM 
 INTO :vATTR_NM 
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S 
WHERE ATTR_ID IN
(
SEL ATTR_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S 
WHERE RUL_ID =iRUL_ID AND LINK_TY = 'LOOKUP')
;


-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vATTR_NM - Completed; Value - '||vATTR_NM;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

 -- Append keywords to form the complete SQL

SET vATTR_NM1 = '';
L2:
 --HOLD TARGET TABLE ATTRIBUTE NAME  INTO A CURSOR
FOR CSR2 AS
 SEL DISTINCT ATTR_NM 
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S  WHERE ENT_ID IN
	(
	SEL ENT_ID 
	FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S  WHERE ENT_ID IN
(
SEL TRG_ENT_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S
WHERE RUL_ID =iRUL_ID
)) 
AND IS_PK = 'Y'

DO 

 SET vATTR_NM1	=	vATTR_NM1||' AND '||vSRC_TB_NM||'.'||CSR2.ATTR_NM|| '=' ||vTRG_TB_NM||'.'||CSR2.ATTR_NM; 
  
  -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch ATTR_NM - Completed; Value - '||CSR2.ATTR_NM;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET vSQL_TEXT_ST1 = 'FROM  (SEL '||vSRC_TB_NM||'.'||'* FROM '||cLF;
SET vSQL_TEXT_ST1 = vSQL_TEXT_ST1||'(SELECT '||vALLIAS||'.'||'*, CC_VAL_S.CC_VAL_ID FROM '||vSRC_SCHM_NM||'.'||vSRC_TB_NM||' AS '||vALLIAS||cLF||' INNER JOIN '||cLF||vBRL_VW||'.CC_VAL_S  ON 1=1 ';
SET vSQL_TEXT_ST1=  vSQL_TEXT_ST1||cLF||'AND '|| vALLIAS||'.'||vATTR_NM||' = CC_VAL_S.CC_VAL_NM';
SET vSQL_TEXT_ST1 = vSQL_TEXT_ST1||cLF||'AND CC_VAL_S.CC_ID ='||iCC_ID||' ) ' || vSRC_TB_NM;
SET vSQL_TEXT_ST1 = vSQL_TEXT_ST1||cLF||' LEFT OUTER JOIN '||cLF||vWK_SCHM_NM||'.'||vTRG_TB_NM||' ON 1=1 ';
SET vSQL_TEXT_ST1 = vSQL_TEXT_ST1||cLF||vATTR_NM1;
SET vSQL_TEXT_ST1 = vSQL_TEXT_ST1||cLF||'AND '||vTRG_TB_NM||'.CC_ID = '||TRIM(iCC_ID)||' WHERE  '||vTRG_TB_NM||'.'||CSR2.ATTR_NM||' IS NULL )'||vSRC_TB_NM;

SET vSQL_TEXT = vSQL_TEXT_ST1;

  -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Set SQL TEXT - Completed; Value - '||vSQL_TEXT;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
 
 END FOR L2;  
 
END IF;

/*-------NOT IN CP TYPE/PASSTHRU/REDUCING------------*/

 IF vATTR1 = 'N' AND vATTR2 = 'N'
THEN SET vSQL_TEXT  =  'FROM  '||vSRC_SCHM_NM||'.'||vSRC_TB_NM;
  END IF;

     -- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'FROM Statement - Completed; Value - '||vSQL_TEXT;
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
  

SET oSQL_Text = vSQL_TEXT;
SET oReturn_Code = 0;
SET oReturn_Message = 'Process Completed for Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id);

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Process - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

END;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_GROUP_CLAUSE
-- =============================================
-- Description: This Stored Procedure will dynamically generate the "Insert" portion of the query.
-- Change log
--      [2015 01 29]: Initial version
-- =============================================
-- Stored Procedure Parameters
(
IN iRul_Id INTEGER, 
IN iCc_Id INTEGER, 
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000),
OUT oSQL_Text VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vEnt_Id INTEGER;
DECLARE vEnt_Nm VARCHAR(255);
DECLARE vTRG_DB_NM VARCHAR(255);
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(1000);
DECLARE vLogMsgFixed VARCHAR(1000);
DECLARE vLogMsgVariable VARCHAR(1000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'BSNSMD_RTN_GROUP_CLAUSE';
DECLARE vLTY VARCHAR(15);


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

	SET oSQL_Text = '';
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsgVariable || ''' because no rows were returned from the sql.';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

	SET oSQL_Text = '';
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;

-- Set the fixed part of the log message.
SET vLogMsgFixed =  'For Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id);

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Process - Started';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- Fetch Target DB Name
SELECT TRIM(PRMTR_VAL) INTO vTRG_DB_NM FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S WHERE PRMTR_NM = 'TRG_DB_NM' ;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vTRG_DB_NM - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- Get Rule Type
SELECT CASE WHEN RM.TY = 'PASSTHRU' THEN RM.TY 
                             WHEN RM.TY = 'RANKING' THEN RM.TY
                             ELSE COALESCE(RA.LINK_TY , 'DEFAULT') END INTO vLTY 
FROM  QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S RM 
LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S RA 
ON RA.RUL_ID = RM.RUL_ID
AND RA.LINK_TY = 'LOOKUP' 
WHERE 1=1
AND  RM.RUL_ID = iRul_ID;

IF vLTY = 'RANKING' THEN 
	SET vSQL_Text = cLF || ';';  -- Do not add group by for Ranking Rules
	
	SET oSQL_Text = vSQL_Text;
	SET oReturn_Code = 0;
	SET oReturn_Message = 'Process Completed for Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id);
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Process - Completed';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	LEAVE MAIN;
END IF;

-- Fetch Target table Name into a variable 
-- NOTE: Cursor has not been used as "Currently the stored procedure supports only 1 target table per rule" 
SELECT TRIM(em.Ent_Id), TRIM(em.Ent_Nm) INTO vEnt_Id, vEnt_Nm
FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S rtd
INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S em
ON rtd.Trg_Ent_Id = em.Ent_Id
WHERE rtd.Rul_Id = iRul_Id;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vEnt_Id, vEnt_Nm - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

L1: 
-- Fetch Entity Name from Entity Master where entity Id = C1.Target Entity ID. For each row, query values from Attribute Master  and concatenate the names in correct syntax 
FOR	CSR1 AS 
SELECT	TRIM(Attr_Nm) AS Attr_Nm
	FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S  
	WHERE Ent_Id = vEnt_Id
	ORDER BY Attr_Id
DO
IF vSQL_Text <> '' THEN -- Comma should be prefixed to the attribute name 
	SET vSQL_Text = vSQL_Text || cLF || ',' || CSR1.Attr_Nm;
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Inside Loop - Getting Next Attribute Name - Completed';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 2;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
ELSE -- This is the first attribute, do not prefix a comma
	SET vSQL_Text = vSQL_Text || cLF || ' ' || CSR1.Attr_Nm;
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Inside Loop - Getting First Attribute Name - Completed';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 2;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
END IF;
END FOR L1;

-- Append keywords to form the complete SQL
SET vSQL_Text = 'GROUP BY  ' || vSQL_Text || cLF || ';' ;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'SQL Generation - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
SET oSQL_Text = vSQL_Text;
SET oReturn_Code = 0;
SET oReturn_Message = 'Process Completed for Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id);

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Process - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_INSERT
-- =============================================
-- Description: This Stored Procedure will dynamically generate the "Insert" portion of the query.
-- Change log
--      [2015 01 29]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
IN iRul_Id INTEGER, 
IN iCc_Id INTEGER, 
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000),
OUT oSQL_Text VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vEnt_Id INTEGER;
DECLARE vEnt_Nm VARCHAR(255);
DECLARE vTRG_DB_NM VARCHAR(255);
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(1000);
DECLARE vLogMsgFixed VARCHAR(1000);
DECLARE vLogMsgVariable VARCHAR(1000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'BSNSMD_RTN_INSERT';


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

	SET oSQL_Text = '';
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsgVariable || ''' because no rows were returned from the sql.';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

	SET oSQL_Text = '';
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;

-- Set the fixed part of the log message.
SET vLogMsgFixed =  'For Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id);

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Process - Started';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 0;
--CALL  QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- Fetch Target DB Name
SELECT TRIM(PRMTR_VAL) INTO vTRG_DB_NM FROM QSIT_APRA2_BRL_RRP_VW.PRMTR_MSTR_S WHERE PRMTR_NM = 'WK_DB_NM' ;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vTRG_DB_NM - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
--CALL  QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- Fetch Target table Name into a variable 
-- NOTE: Cursor has not been used as "Currently the stored procedure supports only 1 target table per rule" 
SELECT TRIM(em.Ent_Id), TRIM(em.Ent_Nm) INTO vEnt_Id, vEnt_Nm
FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S rtd
INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S em
ON rtd.Trg_Ent_Id = em.Ent_Id
WHERE rtd.Rul_Id = iRul_Id;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Fetch vEnt_Id, vEnt_Nm - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
--CALL  QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

L1: 
-- Fetch Entity Name from Entity Master where entity Id = C1.Target Entity ID. For each row, query values from Attribute Master  and concatenate the names in correct syntax 
FOR	CSR1 AS 
SELECT	TRIM(Attr_Nm) AS Attr_Nm
	FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S  
	WHERE Ent_Id = vEnt_Id
	ORDER BY Attr_Id
DO
IF vSQL_Text <> '' THEN -- Comma should be prefixed to the attribute name 
	SET vSQL_Text = vSQL_Text || cLF || ',' || CSR1.Attr_Nm;
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Inside Loop - Getting Next Attribute Name - Completed';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 2;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
ELSE -- This is the first attribute, do not prefix a comma
	SET vSQL_Text = vSQL_Text || cLF || ' ' || CSR1.Attr_Nm;
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Inside Loop - Getting First Attribute Name - Completed';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 2;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
END IF;
END FOR L1;

-- Append keywords to form the complete SQL
SET vSQL_Text = 'INSERT INTO ' || vTRG_DB_NM || '.' || vEnt_Nm || ' (' || vSQL_Text || cLF || ')' ;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'SQL Generation - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 1;
--CALL  QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
SET oSQL_Text = vSQL_Text;
SET oReturn_Code = 0;
SET oReturn_Message = 'Process Completed for Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id);

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsgVariable = 'Process - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET vDebugLvl = 0;
--CALL  QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_LIST
-- =============================================
-- Description: This Stored Procedure will dynamically generate the 'List 'part of WHERE' clause
-- Change log
--      [2015 01 29]: Initial version 
--      (2016 02 05): Added condition for different HighCond and LowCond.
--      (2016 02 26): Added conditions for LinkType = (Contains, Begin with ,End with)
-- =============================================
-- Stored Procedure Parameters
(
IN iList_ID INTEGER,
IN iCc_Id INTEGER,
OUT oSQL_Text1 VARCHAR(5000),
OUT oSQL_Text2 VARCHAR(5000),
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000)
)
 
BEGIN
DECLARE	v_str VARCHAR(1000);
DECLARE iLinkType VARCHAR(50);
DECLARE iVal VARCHAR (50);
DECLARE iHigh_Val VARCHAR (50);
DECLARE iLow_Val VARCHAR (50);
DECLARE iHighCond VARCHAR (10);
DECLARE iLowCond VARCHAR (10);
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR (1000);
DECLARE vReporting_Period_Start_Date VARCHAR(30);
DECLARE vReporting_Period_End_Date VARCHAR(30);

DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE vDebug_Lvl SMALLINT;
DECLARE vSQL_Text1 VARCHAR(5000);
DECLARE vSQL_Text2 VARCHAR(5000);

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    SET oReturn_Code = 2;
    SET oReturn_Message = ' Call to SP failed. SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

DECLARE EXIT HANDLER FOR SQLSTATE '02000'
BEGIN
    SET vSQL_Text1 = '';
	SET vSQL_Text2 ='';
	SET oReturn_Code = 1;
	SET oReturn_Message = 'Error: List Id does not exist for List_Id '||TRIM(iList_ID)|| ' OR does not have value of either ''''RANGE'''' OR ''''LOV'''' ';
END;

/* Error Logging procedure called */
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Process Started for List Id = ' || TRIM(iList_ID) || ' and Cc_Id = ' || TRIM(iCc_Id), 0, oSubReturn_Code, oSubReturn_Message);

SELECT CAST (CAST (END_TS AS DATE FORMAT 'YYYY-MM-DD') AS VARCHAR(30)) INTO vReporting_Period_End_Date FROM QSIT_APRA2_BRL_RRP_VW.T_APRA_RPT_PRD ;
SELECT CAST (CAST (STRT_TS AS DATE FORMAT 'YYYY-MM-DD') AS VARCHAR(30)) INTO vReporting_Period_Start_Date FROM QSIT_APRA2_BRL_RRP_VW.T_APRA_RPT_PRD ;
SET vCntr = vCntr + 1; -- Increase step number by 1
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Reporting Period Start and End Dates fetched' , 1, oSubReturn_Code, oSubReturn_Message);


-- Fetch the value for column List Type (RANGE, LOV). Value should exist in the list master table.
SEL Lst_Ty 
INTO :iLinkType 
FROM QSIT_APRA2_BRL_RRP_VW.LST_MSTR_S  
WHERE Lst_id =iList_ID
AND Lst_Ty IN ('RANGE', 'LOV', 'CONTAINS', 'BEGIN WITH', 'END WITH');

SET vCntr = vCntr + 1; -- Increase step number by 1
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Validation completed to ensure if the List type id of either RANGE or  LOV' , 1, oSubReturn_Code, oSubReturn_Message);

/* Need to check the value of  variable iLinkType here and it is type RANGE then execute the below sql, otherwise exit handler will be called  automatically */

IF iLinkType ='RANGE' THEN     -- Fetch the values from table  List  Range to assign based on the Link type values
SELECT
Hgh_Val 
,Low_Val  
,CASE COALESCE (TRIM(Hgh_Cond),'') WHEN '' THEN NULL ELSE TRIM(Hgh_Cond) END AS Hgh_Cond
,CASE COALESCE (TRIM(Low_Cond),'') WHEN '' THEN NULL ELSE TRIM(Low_Cond) END AS Low_Cond
INTO  iHigh_Val , iLow_Val , iHighCond, iLowCond
 FROM (
				 SELECT 
				 RA.Lst_Id AS Lst_Id
				,RA.Link_Ty AS Link_Type
				,CASE WHEN Low_Val = '#Reporting_Period_Start_Date#' THEN '''' || vReporting_Period_Start_Date || ''''
				          WHEN Low_Val = '#Reporting_Period_End_Date#' THEN '''' || vReporting_Period_End_Date || ''''
				ELSE Low_Val END AS Low_Val                       
				,Low_Cond                      
				,CASE WHEN Hgh_Val = '#Reporting_Period_Start_Date#' THEN '''' || vReporting_Period_Start_Date || ''''
				          WHEN Hgh_Val = '#Reporting_Period_End_Date#' THEN '''' || vReporting_Period_End_Date || ''''
				ELSE Hgh_Val END AS Hgh_Val                       
				,Hgh_Cond    
				 FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S  RA
				 INNER JOIN QSIT_APRA2_BRL_RRP_VW.Lst_Rng_S LR
				 ON RA.Lst_Id =LR.Lst_Id
				 CROSS JOIN QSIT_APRA2_BRL_RRP_VW.T_APRA_RPT_PRD
				 WHERE RA.Lst_Id = iList_ID
		     )   LIST_RANGE ;
 	 

IF 
(  iLowCond = '>=' AND iHighCond = '<=' ) THEN 

SET vSQL_Text1 = 'BETWEEN'||' '||iLow_Val||' '||'AND'||' '||iHigh_Val ;
SET vSQL_Text2 = NULL ;
/*
      ELSE 
	  
	  SET vSQL_Text1 = iLowCond||' '|| TRIM(CAST(iLow_Val AS INTEGER));
      SET vSQL_Text2 = iHighCond||' '||TRIM(CAST(iHigh_Val AS INTEGER));*/

END IF ;

/***** For LowCond or HighCond is Null **/
IF
(  iLowCond IS NULL AND iHighCond IS NOT NULL ) THEN

      SET vSQL_Text1 = iHighCond||' '|| iHigh_Val; --TRIM(CAST(iHigh_Val AS INTEGER));
      SET vSQL_Text2 = NULL ;

END IF;

/***** For LowCond is not null and HighCond is Null **/
IF
(  iHighCond IS NULL AND iLowCond IS NOT NULL ) THEN

      SET vSQL_Text1 = iLowCond||' '|| iLow_Val; --TRIM(CAST(iLow_Val AS INTEGER));
      SET vSQL_Text2 = NULL ;

END IF;

/***** For LowCond and HighCond is not Null and both are not inclusive **/
IF
(  iHighCond IS NOT NULL AND iLowCond IS NOT NULL AND iLowCond <> '>=' AND iHighCond <> '<=' ) THEN

	  SET vSQL_Text1 = iLowCond||' '|| TRIM(CAST(iLow_Val AS INTEGER));
      SET vSQL_Text2 = iHighCond||' '||TRIM(CAST(iHigh_Val AS INTEGER));

END IF;

/***** For LowCond and HighCond is Null **/
IF 
(  iLowCond IS NULL AND iHighCond IS NULL ) THEN

SET vCntr = vCntr + 1; -- Increase step number by 1
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Error- value for either HighCond or LowCond should exist' , 0, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 1;
SET oReturn_Message = 'Error- value for either HighCond or LowCond should exist'||TRIM(iList_ID)|| ' OR does not have value of either ''''HighCond'''' OR ''''LowCond	'''' ';
END IF;
END IF;

 
/*** ENDS **/
	  	  
SET v_str = '' ;	
IF iLinkType ='LOV' THEN 
L1: 
			FOR	CSR1 AS 

			SELECT
			CASE WHEN LV.Val = '#Reporting_Period_Start_Date#' THEN vReporting_Period_Start_Date
                      WHEN LV.Val = '#Reporting_Period_End_Date#' THEN vReporting_Period_End_Date
                      ELSE LV.Val END AS VAL			 
			FROM QSIT_APRA2_BRL_RRP_VW.LST_VAL_S LV
			WHERE Lst_Id = iList_ID
			 
			DO
			------------
			SET v_str = v_str||','||''''||CSR1.VAL||'''' ; /* Concatenating values in a string */
			
           SET vCntr = vCntr + 1; -- Increase step number by 1
           CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Fetching values for List Type LOV' , 2, oSubReturn_Code, oSubReturn_Message);

			END FOR L1 ;
			-- Substring required to by pass the first comma in the above loop
            SET vSQL_Text1 = '('||' '||SUBSTR (v_str,2)||' '||')' ;
            SET vSQL_Text2 = NULL ;

END IF;

IF iLinkType ='CONTAINS' THEN 
L2: 
			FOR	CSR2 AS 

			SELECT
			 CASE WHEN LV.Val = '#Reporting_Period_Start_Date#' THEN vReporting_Period_Start_Date
                      WHEN LV.Val = '#Reporting_Period_End_Date#' THEN vReporting_Period_End_Date
                      ELSE LV.Val END AS VAL			
			FROM QSIT_APRA2_BRL_RRP_VW.LST_VAL_S LV
			WHERE Lst_Id = iList_ID
			 
			DO
			------------
			SET v_str = v_str||','||''''||'%'||CSR2.VAL||'%'||'''' ; /* Concatenating values in a string */
			
           SET vCntr = vCntr + 1; -- Increase step number by 1
           CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Fetching values for List Type LOV' , 2, oSubReturn_Code, oSubReturn_Message);

			END FOR L2 ;
			-- Substring required to by pass the first comma in the above loop
            SET vSQL_Text1 = '('||' '||SUBSTR (v_str,2)||' '||')' ;
            SET vSQL_Text2 = NULL ;

END IF;

IF iLinkType ='BEGIN WITH' THEN 
L3: 
			FOR	CSR3 AS 

			SELECT
			 CASE WHEN LV.Val = '#Reporting_Period_Start_Date#' THEN vReporting_Period_Start_Date
                      WHEN LV.Val = '#Reporting_Period_End_Date#' THEN vReporting_Period_End_Date
                      ELSE LV.Val END AS VAL			
			FROM QSIT_APRA2_BRL_RRP_VW.LST_VAL_S LV
			WHERE Lst_Id = iList_ID
			 
			DO
			------------
			SET v_str = v_str||','||''''||CSR3.VAL||'%'||'''' ; /* Concatenating values in a string */
			
           SET vCntr = vCntr + 1; -- Increase step number by 1
           CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Fetching values for List Type LOV' , 2, oSubReturn_Code, oSubReturn_Message);

			END FOR L3 ;
			-- Substring required to by pass the first comma in the above loop
            SET vSQL_Text1 = '('||' '||SUBSTR (v_str,2)||' '||')' ;
            SET vSQL_Text2 = NULL ;

END IF;

IF iLinkType ='END WITH' THEN 
L4: 
			FOR	CSR4 AS 

			SELECT
			 CASE WHEN LV.Val = '#Reporting_Period_Start_Date#' THEN vReporting_Period_Start_Date
                      WHEN LV.Val = '#Reporting_Period_End_Date#' THEN vReporting_Period_End_Date
                      ELSE LV.Val END AS VAL			
			FROM QSIT_APRA2_BRL_RRP_VW.LST_VAL_S LV
			WHERE Lst_Id = iList_ID
			 
			DO
			------------
			SET v_str = v_str||','||''''||'%'||CSR4.VAL||'''' ; /* Concatenating values in a string */
			
           SET vCntr = vCntr + 1; -- Increase step number by 1
           CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Fetching values for List Type LOV' , 2, oSubReturn_Code, oSubReturn_Message);

			END FOR L4 ;
			-- Substring required to by pass the first comma in the above loop
            SET vSQL_Text1 = '('||' '||SUBSTR (v_str,2)||' '||')' ;
            SET vSQL_Text2 = NULL ;

END IF;

SET oReturn_Code = 0;

SET oSQL_Text1 = vSQL_Text1;
SET oSQL_Text2 = vSQL_Text2;

SET vCntr = vCntr + 1; -- Increase step number by 1
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, '/*vSQLText 1 = */' || vSQL_Text1 || ' /*vSQLText 2 = */' || vSQL_Text2, 0, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Message = 'Successfully Completed for List Id = ' || TRIM(iList_ID) || ' and Cc_Id = ' || TRIM(iCc_Id);
SET vCntr = vCntr + 1; -- Increase step number by 1
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_LIST', vCntr, 'Process successfully completed for List Id = ' || TRIM(iList_ID) || ' and Cc_Id = ' || TRIM(iCc_Id), 0, oSubReturn_Code, oSubReturn_Message);
END;
--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_QUERY
-- =============================================
-- Description: This procedure will generate the "Select" portion of the Rule Query
-- Change log
--      [2015 01 29]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iRul_ID INTEGER,
 IN iCC_ID INTEGER,
 OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
 OUT oReturn_Message VARCHAR(1000),
 OUT oSQL_Text VARCHAR(30000)
 ) 
MAIN:
BEGIN
-- Declare variables
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code INTEGER;
DECLARE oSubReturn_Message VARCHAR(1000);

DECLARE cLF CHAR(2) DEFAULT '0A'XC;

DECLARE vSQL_Text VARCHAR(30000);
DECLARE vInsertSQL_Text VARCHAR(10000);
DECLARE vSelectSQL_Text VARCHAR(10000);
DECLARE vFromSQL_Text VARCHAR(10000);
DECLARE vWhereSQL_Text VARCHAR(10000);
DECLARE vGroupSQL_Text VARCHAR(1000);

DECLARE oSubReturn_Code_insert                SMALLINT;
 DECLARE oSubReturn_Message_insert         VARCHAR(1000);
 
DECLARE oSubReturn_Code_select               SMALLINT;
 DECLARE oSubReturn_Message_select         VARCHAR(1000);
 
 DECLARE oSubReturn_Code_from                SMALLINT;
 DECLARE oSubReturn_Message_from         VARCHAR(1000);
 
 DECLARE oSubReturn_Code_where              SMALLINT;
 DECLARE oSubReturn_Message_where        VARCHAR(1000);
 
 DECLARE oSubReturn_Code_group              SMALLINT;
 DECLARE oSubReturn_Message_group        VARCHAR(1000);


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    SET oReturn_Code = 2;
    SET oReturn_Message = ' Call to SP failed. SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 100, 'Started - iRul_ID '||iRul_ID ||' iCC_ID:'||iCC_ID, 0, oSubReturn_Code, oSubReturn_Message);

----- Insert Part Begin
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 200, 'Before calling BSNSMD_RTN_INSERT', 0, oSubReturn_Code, oSubReturn_Message);


CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_INSERT(iRul_ID, iCC_ID, oSubReturn_Code_insert, oSubReturn_Message_insert, vInsertSQL_Text);

 IF oSubReturn_Code_insert <> 0 THEN
    SET oReturn_Code = oSubReturn_Code_insert;
    SET oReturn_Message = oSubReturn_Message_insert;
    LEAVE MAIN;
 
END IF;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 300, 'After calling BSNSMD_RTN_INSERT SQL Text :'||vInsertSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

----- Insert Part End 	


----- Select Part Begin
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 400, 'Before calling BSNSMD_RTN_SELECT', 0, oSubReturn_Code, oSubReturn_Message);
 
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_SELECT(iRul_ID, iCC_ID, oSubReturn_Code_select, oSubReturn_Message_select, vSelectSQL_Text);

IF oSubReturn_Code_select <> 0 THEN
    SET oReturn_Code = oSubReturn_Code_select ;
    SET oReturn_Message = oSubReturn_Message_select ;
 	LEAVE MAIN;
 	
END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 500, 'After calling BSNSMD_RTN_SELECT SQL Text :'||vSelectSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

----- Select Part End 


----- From Part Begin
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 600, 'Before calling BSNSMD_RTN_FROM', 0, oSubReturn_Code, oSubReturn_Message);
										

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_FROM(iRul_ID, iCC_ID, oSubReturn_Code_from , oSubReturn_Message_from , vFromSQL_Text);

IF oSubReturn_Code_from <> 0 THEN
    SET oReturn_Code = oSubReturn_Code_from ;
    SET oReturn_Message = oSubReturn_Message_from ;
    
    LEAVE MAIN;

END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 700, 'After calling BSNSMD_RTN_FROM SQL Text :'||vFromSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

----- From Part End 



----- Where Part Begin
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 800, 'Before calling BSNSMD_RTN_WHERE', 0, oSubReturn_Code, oSubReturn_Message);
										



CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_WHERE(iRul_ID, iCC_ID, oSubReturn_Code_where , oSubReturn_Message_where , vWhereSQL_Text);

IF oSubReturn_Code_where <> 0 THEN
    SET oReturn_Code = oSubReturn_Code_where ;
    SET oReturn_Message = oSubReturn_Message_where ;
 	LEAVE MAIN;
    
END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 900, 'After calling BSNSMD_RTN_WHERE SQL Text :'||vWhereSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

----- Where Part End 


----- Group by Clause Part Begin
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 800, 'Before calling BSNSMD_RTN_GROUP_CLAUSE', 0, oSubReturn_Code, oSubReturn_Message);
										



CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_GROUP_CLAUSE(iRul_ID, iCC_ID, oSubReturn_Code_group , oSubReturn_Message_group , vGroupSQL_Text);

IF oSubReturn_Code_group <> 0 THEN
    SET oReturn_Code = oSubReturn_Code_group ;
    SET oReturn_Message = oSubReturn_Message_group ;
 	LEAVE MAIN;
    
END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 900, 'After calling BSNSMD_RTN_GROUP_CLAUSE SQL Text :'||vWhereSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

----- Group by Clause Part End 


SET vSQL_Text = vInsertSQL_Text || cLF|| vSelectSQL_Text || cLF || vFromSQL_Text || cLF || vWhereSQL_Text || cLF || vGroupSQL_Text;
SET oSQL_Text = vSQL_Text;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 901,vInsertSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 902,vSelectSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 903,vFromSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 904,vWhereSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 905,vGroupSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 999,vSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0 ;
SET oReturn_Message = 'Success' ;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 1000,vSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_QUERY', 1100, 'Ended SP - iRul_ID '||iRul_ID ||' iCC_ID:'||iCC_ID || 'oSQL_Text:'||vSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_SELECT
-- =============================================
-- Description: This procedure will generate the "Select" portion of the Rule Query
-- Change log
--      [2015 01 29]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
 IN iRul_ID INTEGER,
 IN iCC_ID INTEGER,
 OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
 OUT oReturn_Message VARCHAR(1000),
 OUT oSQL_Text VARCHAR(10000)
 ) 
MAIN:
BEGIN
-- Declare variables
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE vEnt_ID INTEGER;
DECLARE vCC_ID INTEGER;
DECLARE vSQL_Text VARCHAR(10000);
DECLARE vCC_Val_ID INTEGER;
DECLARE cLF VARCHAR(2) ;
DECLARE oSubReturn_Code INTEGER;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vLoopCount INTEGER;
DECLARE vComma CHAR(1);
DECLARE vTY VARCHAR(255);
DECLARE vPRTY INTEGER;
DECLARE vLTY VARCHAR(15);

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    SET oReturn_Code = 2;
    SET oReturn_Message = ' Call to SP failed. SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_SELECT', 0, 'Started - iRul_ID '||iRul_ID ||' iCC_ID:'||iCC_ID, 0, oSubReturn_Code, oSubReturn_Message);

SET cLF	= '
';
									
SET vSQL_Text = '';
SET vLoopCount = 0;

SELECT 
	CC_VAL_ID 
INTO vCC_Val_ID
FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S 
WHERE RUL_ID = iRul_ID;

SELECT DISTINCT 
	TRG_ENT_ID, 
	CC_ID, 
	TY 
INTO 
	vEnt_ID, 
	vCC_ID, 
	vTY
FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S 
WHERE RUL_ID = iRul_ID;

--SELECT 
--	DISTINCT TY INTO vTY
--FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S WHERE RUL_ID = iRul_ID;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_SELECT', 100, 'Data ENT_ID '|| vEnt_ID||' CC_VAL_ID:'|| vCC_Val_ID||' vTY : '||vTY, 2, oSubReturn_Code, oSubReturn_Message);

L1: 
FOR	CSR1 AS 

SELECT 
	 TGT_ATTR.ATTR_NM 	 AS TGT_ATTR_NM
	,COALESCE (TRG_TY, 'INSERT') AS TRG_TY
	,SRC_ATTR.ATTR_NM 	 AS SRC_ATTR_NM
	,RTD.TRG_VAL 		 AS TRG_VAL
	,RTD.PRTN_BY_COL_LST AS PRTN_BY_COL_LST
	,RTD.ORDR_BY_COL_LST AS ORDR_BY_COL_LST
FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S   TGT_ATTR

LEFT OUTER JOIN (SELECT RUL_ID, SRC_ATTR_ID, TRG_ATTR_ID, TRG_VAL, TRG_TY, PRTN_BY_COL_LST, ORDR_BY_COL_LST FROM QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL_S GROUP BY 1, 2, 3, 4, 5, 6, 7) RTD
ON RTD.TRG_ATTR_ID = TGT_ATTR.ATTR_ID

LEFT OUTER JOIN  (SELECT ATTR_ID, ATTR_NM FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S GROUP BY 1, 2)  SRC_ATTR
ON RTD.SRC_ATTR_ID = SRC_ATTR.ATTR_ID

WHERE 1=1
AND COALESCE(RTD.RUL_ID,iRul_ID) = iRul_ID
AND TGT_ATTR.ENT_ID = vEnt_ID
ORDER BY TGT_ATTR.ATTR_ID 

DO
----------

SET vLoopCount = vLoopCount + 1;

IF vLoopCount = 1 THEN
	SET vComma = '';
ELSE 
	SET vComma = ',';
END IF;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_SELECT', 200, 'In looop L1 CSR1.TGT_ATTR_NM = '|| CSR1.TGT_ATTR_NM , 3, oSubReturn_Code, oSubReturn_Message);

SELECT PRTY INTO vPRTY 
FROM  QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S WHERE RUL_ID = iRul_ID;

SELECT 
CASE 
WHEN RM.TY = 'PASSTHRU' THEN RM.TY
WHEN RM.TY = 'RANKING' THEN RM.TY
ELSE COALESCE(RA.LINK_TY , 'DEFAULT')
END
INTO vLTY 
FROM  QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S RM 
LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S RA 
ON RA.RUL_ID = RM.RUL_ID
AND RA.LINK_TY = 'LOOKUP' 
WHERE 1=1
AND  RM.RUL_ID = iRul_ID;

IF vLTY IS NULL THEN 
SET vLTY = 'DEFAULT';
END IF;

IF 	CSR1.TRG_TY = 'INSERT' THEN
	CASE 
	
	WHEN  CSR1.TGT_ATTR_NM = 'PRTY' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| TRIM(vPRTY) ||' AS PRTY';
	
	WHEN  CSR1.TGT_ATTR_NM = 'RUL_ID' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| TRIM(iRul_ID) ||' AS RUL_ID';
/*	WHEN CSR1.TGT_ATTR_NM = 'CC_ID' AND vTY <> 'PASSTHRU'THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| TRIM(iCC_ID) ||' AS CC_ID';*/
	--WHEN CSR1.TGT_ATTR_NM = 'CC_ID' AND vTY = 'PASSTHRU'THEN 
	
	WHEN CSR1.TGT_ATTR_NM = 'CC_ID' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| TRIM(vCC_ID) ||' AS CC_ID';
		
	WHEN CSR1.TGT_ATTR_NM = 'CC_VAL_ID' AND COALESCE (vLTY, 'DEFAULT') = 'LOOKUP' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| 'CC_VAL_ID AS CC_VAL_ID';		
	
	WHEN CSR1.TGT_ATTR_NM = 'CC_VAL_ID' AND COALESCE (vLTY, 'DEFAULT') <> 'PASSTHRU' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| COALESCE (TRIM(vCC_Val_ID), -99) ||' AS CC_VAL_ID';
	
	WHEN CSR1.TGT_ATTR_NM = 'CC_VAL_ID' AND COALESCE (vLTY, 'DEFAULT') = 'PASSTHRU' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| 'CC_VAL_ID AS CC_VAL_ID';
	
	WHEN CSR1.TGT_ATTR_NM = 'VAL' AND COALESCE (vLTY, 'DEFAULT') = 'RANKING' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| 'ROW_NUMBER() OVER ( PARTITION BY ' ||  COALESCE(CSR1.PRTN_BY_COL_LST,-1)
							 || ' ORDER BY ' || COALESCE(CSR1.ORDR_BY_COL_LST, -1) || ') AS VAL';
	
	WHEN CSR1.TGT_ATTR_NM = 'ATTR_ID' AND COALESCE (vLTY, 'DEFAULT') = 'RANKING' THEN 
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| COALESCE(TRIM(CSR1.TRG_VAL), -1) ||' AS ATTR_ID';
	
    ELSE  
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| TRIM(COALESCE(CSR1.SRC_ATTR_NM, 'NULL ')) ||' AS '||TRIM(CSR1.TGT_ATTR_NM);		
        --SET vSQL_Text = vSQL_Text;-- || cLF ||vComma|| TRIM(COALESCE(CSR1.SRC_ATTR_NM, 'NULL ')) ||' AS '||TRIM(CSR1.TGT_ATTR_NM);		
	END CASE;
END IF;

IF 	CSR1.TRG_TY = 'SETVAL' THEN
		SET vSQL_Text = vSQL_Text || cLF ||vComma|| TRIM(COALESCE('''' || CSR1.TRG_VAL || '''', 'NULL ')) ||' AS '||TRIM(CSR1.TGT_ATTR_NM);
END IF;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_SELECT', 300, 'vSQL_Text '|| vSQL_Text, 3, oSubReturn_Code, oSubReturn_Message);	
----------
END FOR	L1;

SET oSQL_Text = 'SELECT'||vSQL_Text;

SET oReturn_Code = 0;
SET oReturn_Message = 'Successfully Completed ';

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_SELECT', 0, 'Ended SP - iRul_ID '||iRul_ID ||' iCC_ID:'||iCC_ID || 'oSQL_Text:'||oSQL_Text, 0, oSubReturn_Code, oSubReturn_Message);

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_WHERE
(
-- =============================================
-- Description: This Stored Procedure will dynamically generate the 'WHERE' clause
-- Change log
--      [2015 01 29]: Initial version 
--      [2015 02 08]: Enhanced the script for Link_Type = * and Src_Sys_Cd = GLOBAL
--      [2015 02 26]: Modified Link_Type = LIKE for GL adjustments
-- =============================================
-- Stored Procedure Parameters
IN iRul_Id INTEGER,
IN iCc_Id INTEGER,
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000),
OUT oSQL_Text VARCHAR(1000)
 )
 
MAIN:
BEGIN	
DECLARE	v_str VARCHAR(1000);
DECLARE oSQL_Text1 VARCHAR (1000);
DECLARE oSQL_Text2 VARCHAR (1000);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR (1000);
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE oSubReturn_Code1 SMALLINT;
DECLARE oSubReturn_Message1 VARCHAR (1000);

DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE vDebug_Lvl SMALLINT;
DECLARE vChk_iRul_Id INTEGER ;
DECLARE vChk_iCc_Id INTEGER ;
DECLARE vChk_Lst_Id INTEGER ;
DECLARE vChk_iSrc_Sys_Cd VARCHAR(10) ;
DECLARE Check_Condition CONDITION FOR SQLSTATE VALUE  '02000';

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    SET oReturn_Code = 2;
    SET oReturn_Message = ' Call to SP failed. SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
END;

-- Error handler for SQLSTATE '02000'
DECLARE EXIT HANDLER FOR Check_Condition
BEGIN
    SET oSQL_Text = '';
	SET oReturn_Code = 1;
	SET oReturn_Message = 'Error:'||cLF||'Table Rul_Mstr does not contain any record for Rul_id and Cc_Id '|| TRIM(iRul_Id)||', '||TRIM(iCc_Id)||cLF|| 'OR '||cLF||'List Id does not exist for Rul_Id '||TRIM(iRul_Id);
END;

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_WHERE', vCntr, 'Process Started for Rule Id = ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id), 0, oSubReturn_Code1, oSubReturn_Message1);

-- Check if  input variables iRul_Id and iCc_id are valid. This needs to be checked in Rul_Mstr table as it has both the ids.
SELECT
 Rul_Id
 ,Cc_Val_Id
 ,Src_Sys_Cd 
INTO 
 :vChk_iRul_Id 
 ,:vChk_iCc_Id
 ,:vChk_iSrc_Sys_Cd 
FROM 
 QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S
WHERE 
 Rul_Id = iRul_Id; 


IF ACTIVITY_COUNT =0 THEN
    
	SIGNAL Check_Condition;

END IF ; 

L0: 
FOR	CSR0 AS 

SELECT
Lst_Id
FROM 
QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S
WHERE
Rul_Id = iRul_Id
AND LINK_TY  IN ('IN','NOT IN', 'RANGE', 'IS NULL', 'IS NOT NULL','*' )

DO 
------------
IF ACTIVITY_COUNT =0  THEN

SIGNAL Check_Condition;

END IF ; 

END FOR L0;

SET vCntr = vCntr + 1; -- Increase step number by 1
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_WHERE', vCntr, 'Validation of Rule Id and Link Type completed for' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id), 1, oSubReturn_Code1, oSubReturn_Message1);


SET v_str ='WHERE  1=1 ';

L1: 
FOR	CSR1 AS 
 
SELECT
 A.Link_Type
 ,A.Lst_Id
 ,A.Attr_nm
 FROM (
			 SELECT 
			 C.Attr_nm  AS  Attr_nm
			 ,LM.Lst_Id AS Lst_Id
			 ,RA.Link_Ty AS Link_Type
			 FROM 
			 QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S  RA
			 INNER JOIN 
			 QSIT_APRA2_BRL_RRP_VW.LST_MSTR_S LM
			 ON RA.Lst_Id = LM.Lst_Id
			 INNER JOIN 
			 QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S  C
			 ON LM.Attr_id = C.Attr_Id
			 WHERE 
			 Rul_Id = iRul_Id
			  ) A 
  
 DO
-------------------

CASE
WHEN (CSR1.LINK_TYPE IN('*','LOOKUP'))THEN /* Do Nothing */
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_WHERE', vCntr, '* Link Type found ' , 2, oSubReturn_Code1, oSubReturn_Message1);

WHEN (CSR1.Link_Type  = 'IN' OR CSR1.Link_Type='NOT IN' OR CSR1.Link_Type='LIKE') THEN
 
    CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_LIST (CSR1.Lst_Id, iCc_Id, "oSQL_Text1", "oSQL_Text2", "oSubReturn_Code", "oSubReturn_Message") ;
    SET v_str =v_str||cLF||' AND '||CSR1.Attr_nm||' '||CSR1.Link_Type||' '||oSQL_Text1;
	
WHEN (CSR1.Link_Type  = 'RANGE' ) THEN 
    
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_LIST (CSR1.Lst_Id, iCc_Id, "oSQL_Text1", "oSQL_Text2", "oSubReturn_Code", "oSubReturn_Message") ;
 
         IF ( oSQL_Text2  IS NULL AND oSQL_Text1 IS NOT NULL ) 
		 THEN		  
		      SET v_str= v_str||cLF||' AND ' ||CSR1.Attr_nm||' '||oSQL_Text1;
	     ELSE
	          SET v_str = v_str||cLF||' AND '||CSR1.Attr_nm||"oSQL_Text1"||' AND '||CSR1.Attr_nm||"oSQL_Text2";
	     END IF ;

WHEN (CSR1.Link_Type  = 'IS NULL' OR CSR1.Link_Type  = 'IS NOT NULL' ) THEN
     
	 SET v_str= v_str||cLF||' AND '||CSR1.Attr_nm||' '||CSR1.Link_Type;

 END CASE ;
-------------------
END FOR L1;

/*** Processing for source system code = GLOBAL ***/

IF 
	vChk_iSrc_Sys_Cd NOT IN ('GLOBAL' ,'NA')
THEN
	SET v_str = v_str||cLF||' AND SRC_SYS_CD = '''|| vChk_iSrc_Sys_Cd || '''';
	SET vCntr = vCntr + 1; -- Increase step number by 1
 CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_WHERE', vCntr, 'Added Source system code filter ' ||v_str, 2, oSubReturn_Code1, oSubReturn_Message1);

END IF ;


SET vCntr = vCntr + 1; -- Increase step number by 1
 CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_WHERE', vCntr, 'Where clause formation completed based on the link type values ' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id), 2, oSubReturn_Code1, oSubReturn_Message1);


SET oSQL_Text = v_str ;
SET oReturn_Code = 0;
SET oReturn_Message = 'Successfully Completed.';


SET vCntr = vCntr + 1; -- Increase step number by 1
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG ('BSNSMD_RTN_WHERE', vCntr, 'Output for Rule ID :' || TRIM(iRul_Id) || ' and Cc_Id = ' || TRIM(iCc_Id)||' : '||oSQL_Text, 0, oSubReturn_Code1, oSubReturn_Message1);

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE MACRO  QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RUL_DWNLDR
AS (
   
  SELECT DISTINCT
 RLMSTR.RUL_NM
, RLMSTR.RUL_DESC
, RLMSTR.TY		
, CASE RLMSTR.DQ_TY  
  WHEN 'Y' THEN 'YES'
  ELSE 'NO' END AS DQ_TY
, RLMSTR.SRC_SYS_CD             
,ENTSTR.BSNS_ENT_NM
,CCMSTR.CC_SHRT_NM        
,CCVAL.CC_VAL_SHRT_NM         
,CASE RLMSTR.IS_DFLT     
WHEN 'Y' THEN 'YES'
ELSE 'NO' END AS IS_DFLT
,RLMSTR.PRTY    
,ATRMSTR.BSNS_ATTR_NM 
,RLATTR.LINK_TY
  ,
CASE	WHEN LSTVAL.VAL IS NOT NULL
  THEN LSTVAL.VAL
  ELSE NULL
  END  AS VAL
 ,
CASE	WHEN  VAL IS NULL
 THEN LSTRNG.LOW_COND
 ELSE ''		
 END AS LOW_COND
 ,
CASE	WHEN  VAL IS NULL
 THEN LSTRNG.LOW_VAL
 ELSE ''		
 END AS LOW_VAL
 ,
CASE	WHEN  VAL IS NULL
 THEN LSTRNG.HGH_COND
 ELSE ''		
 END AS HGH_COND
 ,
CASE	WHEN  VAL IS NULL
 THEN LSTRNG.HGH_VAL
 ELSE ''		
 END AS HGH_VAL
 ,ENTSTR1.BSNS_ENT_NM
 ,TRGDTL.PRTN_BY_COL_LST
 ,TRGDTL.ORDR_BY_COL_LST
 FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S          			RLMSTR
 INNER JOIN
 QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S          						ENTSTR
 ON RLMSTR.DATA_ENT_ID = ENTSTR.ENT_ID
  INNER JOIN
 QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S          						ENTSTR1
 ON RLMSTR.TRG_ENT_ID = ENTSTR1.ENT_ID
INNER JOIN
QSIT_APRA2_BRL_RRP_VW.CC_MSTR_S 										CCMSTR
	ON	RLMSTR.CC_ID = CCMSTR.CC_ID
INNER JOIN
QSIT_APRA2_BRL_RRP_VW.CC_VAL_S											CCVAL
	ON	
CCMSTR.CC_ID = CCVAL.CC_VAL_ID       
INNER JOIN
QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S 									RLATTR
	ON	RLMSTR.RUL_ID = RLATTR.RUL_ID

INNER JOIN
QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S								ATRMSTR 
	ON	RLATTR.ATTR_ID = ATRMSTR.ATTR_ID

INNER JOIN
QSIT_APRA2_BRL_RRP_VW.LST_MSTR_S									LSTMSTR
	ON	ATRMSTR.ATTR_ID = LSTMSTR.ATTR_ID
  AND RLATTR.LST_ID = LSTMSTR.LST_ID
 
LEFT OUTER JOIN
QSIT_APRA2_BRL_RRP_VW.LST_VAL_CSV										LSTVAL
	ON	LSTMSTR.LST_ID = LSTVAL.LST_ID

 
LEFT OUTER JOIN 
QSIT_APRA2_BRL_RRP_VW.LST_RNG_S										LSTRNG
	ON	LSTMSTR.LST_ID = LSTRNG.LST_ID

INNER JOIN
QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL_S					 		TRGDTL
	ON	RLMSTR.RUL_ID= TRGDTL.RUL_ID

-- WHERE RLMSTR.RUL_NM =  'CAP APRA Loan Purpose'


QUALIFY	RANK() OVER(PARTITION BY LSTVAL.LST_ID 
ORDER	BY LSTVAL.ROW_NBR) = 1;
   );
--------------------------------------------------------------------------------

REPLACE	PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RUL_EXEC
-- =============================================
-- Description: This Stored Procedure calls the RTN_QUERY proc for an optional CC_ID and RUL_ID.
-- Change log
--      [2016 02 02]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(  
IN iExec_Mode							VARCHAR(2),
IN iRul_Id 									INTEGER,
IN iCC_Id 									INTEGER,
OUT oReturn_Code					SMALLINT,			/* 0: Successful; Non-Zero: Error */
OUT oReturn_Message			VARCHAR(1000)
)

MAIN:
BEGIN
-- Declare variables
DECLARE	cLF 							CHAR(2) DEFAULT '0A'XC;
DECLARE	vCC_ID 							VARCHAR(10000) DEFAULT '';
DECLARE	vRUL_ID 						VARCHAR(10000) DEFAULT '';
DECLARE	sql_stmt 						VARCHAR(50000);
DECLARE	sql_stmt1 						VARCHAR(50000);
DECLARE	inCC_Id 						INTEGER DEFAULT NULL;
DECLARE	inRUL_ID						INTEGER DEFAULT NULL;
DECLARE	oSubReturn_Code                	SMALLINT;
DECLARE oSubReturn_Message         		VARCHAR(1000);
DECLARE	oSubReturn_Code_rtn_query       SMALLINT;
DECLARE oSubReturn_Message_rtn_query    VARCHAR(30000);
DECLARE oSubReturnSQL_Text				VARCHAR(30000);
DECLARE vTRG_ENT_NM 					VARCHAR(255);
DECLARE vActivity_Count 				INTEGER;
DECLARE	vCntr 							INTEGER DEFAULT 100;
DECLARE	vSQL_Code 						INTEGER;
DECLARE	vSQL_State 						VARCHAR(6);
DECLARE	vError_Text 					VARCHAR(256);
DECLARE	vDebugLvl 						SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE	vLogMsg 						VARCHAR(1000);
DECLARE	vLogMsgFixed 					VARCHAR(1000);
DECLARE	vLogMsgVariable 				VARCHAR(1000);
DECLARE	vLogSPName 						VARCHAR(255) DEFAULT 'BSNSMD_RUL_EXEC';
DECLARE vRULID_COUNT 					INTEGER;
DECLARE vCCID_COUNT 					INTEGER;
DECLARE	vStr1 							VARCHAR(1000) DEFAULT '';
DECLARE	vStr2 							VARCHAR(1000) DEFAULT '';
DECLARE vENT_NM VARCHAR(1000) DEFAULT '';
-- Error Handler
DECLARE	EXIT HANDLER FOR SQLEXCEPTION
BEGIN	
	SET vSQL_Code  = SQLCODE;
	SET vSQL_State = SQLSTATE;
	GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg,
	vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
	SET oReturn_Code = 2;
	SET oReturn_Message = vLogMsg;
END	;
/*
-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE	EXIT HANDLER FOR NOT FOUND
BEGIN	
			
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsgVariable = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsgVariable || ''' because no rows were returned from the sql.';
	SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg,
	vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;			
END	;
*/
-- Set the fixed part of the log message.
SET	vLogMsgFixed =  'For Rule Id = ' || TRIM(vRUL_ID) || ' and Cc_Id = ' || TRIM(iCc_Id);

-- Message Log portion
SET	vCntr = vCntr + 1; -- Increase step number by 1
SET	vLogMsgVariable = 'Process - Started';
SET	vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET	vDebugLvl = 0;
CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

/*************************INVALID RUL_ID & CC_ID****************************************/
IF iRUL_ID <> '' OR  iCC_ID <> '' THEN
IF iRUL_ID <> ''  THEN
	SELECT COUNT(*) INTO :vRULID_COUNT FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S RUL_MSTR WHERE RUL_MSTR.RUL_ID = iRUL_ID;
ELSE SET vRULID_COUNT=1;
END IF;

IF iCC_ID <> ''THEN 
	SELECT COUNT(*) INTO :vCCID_COUNT FROM QSIT_APRA2_BRL_RRP_VW.CC_MSTR_S CC_MSTR WHERE CC_MSTR.CC_ID = iCC_ID;
ELSE 
	SET vCCID_COUNT=1;
END IF;


CASE 	WHEN vRULID_COUNT= 0 THEN
		SET	vStr1 = 'Process - Failed. Invalid RUL_ID submitted = ' ||iRUL_ID;
		ELSE SET	vStr1 ='';
END CASE;
CASE
		WHEN vCCID_COUNT= 0 THEN
		SET	vStr2 = 'Process - Failed. Invalid CC_ID submitted = ' ||iCC_ID;
		ELSE SET	vStr2 ='';
END CASE;

IF vRULID_COUNT= 0 OR vCCID_COUNT= 0 THEN
SET vCntr = vCntr + 1; -- Increase step number by 1
SET	vLogMsgVariable = vStr1||cLF||vStr2;
SET vLogMsg = vLogMsgVariable;
SET	vDebugLvl = 1;
CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
SET oReturn_Code = 2  ;
SET oReturn_Message = vLogMsg;		
LEAVE MAIN;
END IF;	
END IF;	

/*****************************************************************/

-- If the input ICC_ID is blank - Set it to NULL 
IF	iCC_ID = '' THEN
	SET	inCC_ID = NULL;
ELSE	
	SET	inCC_ID = iCC_ID;
END IF;

-- If the input ICC_ID is blank - Set it to NULL 
IF	iRUL_ID = '' THEN
	SET	inRUL_ID = NULL;
ELSE	
	SET	inRUL_ID = iRUL_ID;
END IF;

/*
L_DEL: -- Target table cleanup
FOR CSR_DEL AS
			SELECT DISTINCT ENT_NM 
			FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S em 
			INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_MSTR rm
			ON em.ENT_ID = rm.TRG_ENT_ID
			AND rm.CC_ID =COALESCE(inCC_ID, rm.CC_ID)
			AND rm.RUL_ID = COALESCE(inRUL_ID, rm.RUL_ID)
DO
			IF iExec_Mode = 'Y' THEN 	
						
				SET vTRG_ENT_NM = CSR_DEL.ENT_NM;
						
			
				IF vTRG_ENT_NM = 'OVRD_AR_TO_GL_LNK_INTER' THEN 
					SET sql_stmt = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.' || vTRG_ENT_NM || ';';
				ELSE
					SET sql_stmt = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.' || vTRG_ENT_NM || '  TRG WHERE 1=1'
											|| ' AND RUL_ID = COALESCE(' || COALESCE(inRUL_ID,'NULL') || ', TRG.RUL_ID) '
											|| ' AND CC_ID = COALESCE(' ||COALESCE(inCC_ID,'NULL')  || ', TRG.CC_ID); ';
				END IF;

				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsg =   '/* Generated SQL is: */ /*' || cLF ||  sql_stmt;
				SET	vDebugLvl = 5;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
				EXECUTE IMMEDIATE sql_stmt;
				SET vActivity_Count = ACTIVITY_COUNT;
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsgVariable = 'For iExec_Mode = Y, Records in target table ' || vTRG_ENT_NM || 'deleted for  RUL_ID = ' ||  COALESCE(inRUL_ID,'ALL') || ' No of affected rows are :' || TRIM(vActivity_Count);
				SET	vLogMsg =  vLogMsgVariable;
				SET	vDebugLvl = 1;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
	 END IF;
	 
	
END FOR L_DEL;
*/
L1: 
-- Open a Cursor to hold a CC_ID value
FOR	CSR1 AS 
	SELECT	CC_ID
	FROM QSIT_APRA2_BRL_RRP_VW.CC_MSTR_S 
	WHERE CC_ID = COALESCE(inCC_ID,CC_ID) 
	--WHERE CC_ID = inCC_ID  
	ORDER BY PRTY
DO
	SET vCC_ID = CSR1.CC_ID;   

	-- Message Log portion
	SET	vCntr = vCntr + 1; -- Increase step number by 1
	SET	vLogMsgVariable = 'Fetch CC_ID value into 1st Cursor - Completed   Value : '||TRIM(vCC_ID);
	SET	vLogMsg = vLogMsgVariable;
	SET	vDebugLvl = 1;
	CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	

L_DEL:-- Work  table cleanup
FOR CSR_DEL AS
			SELECT DISTINCT ENT_NM 
			FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S em 
			INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_MSTR rm
			ON em.ENT_ID = rm.TRG_ENT_ID
			AND rm.CC_ID =CSR1.CC_ID
			AND rm.RUL_ID = COALESCE(inRUL_ID, rm.RUL_ID)

DO
			IF iExec_Mode = 'Y' THEN 	
						
				SET vTRG_ENT_NM = CSR_DEL.ENT_NM;
				SET sql_stmt = 'DELETE FROM QSIT_APRA2_BRL_RRP_WK.' || vTRG_ENT_NM || ';';

				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsg =   '/* Generated SQL is: */ ' || cLF ||  sql_stmt;
				SET	vDebugLvl = 5;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
				EXECUTE IMMEDIATE sql_stmt;
				SET vActivity_Count = ACTIVITY_COUNT;
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsgVariable = 'For iExec_Mode = Y, Records in target WK  table ' || vTRG_ENT_NM || 'deleted for  RUL_ID = ' ||  COALESCE(inRUL_ID,'ALL') || ' No of affected rows are :' || TRIM(vActivity_Count);
				SET	vLogMsg =  vLogMsgVariable;
				SET	vDebugLvl = 1;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
	 END IF;
	 
	
END FOR L_DEL;


	L2: 
	-- Open a Cursor to hold the RUL_ID value for the above given CC_ID
	FOR CSR2 AS
		SELECT RUL_MSTR.RUL_ID AS RUL_ID, COALESCE (RUL_MSTR.TRG_ENT_ID, '') AS TRG_ENT_ID
		FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S RUL_MSTR
		LEFT OUTER JOIN 	QSIT_APRA2_BRL_RRP_VW.CC_VAL_S CC_VAL
			ON RUL_MSTR.CC_ID = CC_VAL.CC_ID
			AND CASE RUL_MSTR.TY WHEN 'PASSTHRU' THEN -99 ELSE CC_VAL.CC_VAL_ID END = COALESCE (RUL_MSTR.CC_VAL_ID, '')
 WHERE RUL_MSTR.CC_ID= CSR1.CC_ID
	 		AND COALESCE(inRUL_ID, RUL_ID) = RUL_ID
		GROUP BY RUL_MSTR.RUL_ID, RUL_MSTR.TRG_ENT_ID,  CC_VAL.PRTY, RUL_MSTR.PRTY
		ORDER BY COALESCE(CC_VAL.PRTY,1), RUL_MSTR.PRTY
	DO
		SET vRUL_ID = CSR2.RUL_ID;  
		
		SET	vLogMsgFixed =  'For Rule Id = ' || TRIM(vRUL_ID) || ' and Cc_Id = ' || TRIM(iCc_Id);

		-- Message Log portion
		SET	vCntr = vCntr + 1; -- Increase step number by 1
		SET	vLogMsgVariable = 'Fetch RUL_ID value into 2nd Cursor - Completed  Value : '||TRIM(vRUL_ID);
		SET	vLogMsg =  vLogMsgVariable;
		SET	vDebugLvl = 1;
		CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
		
		-- SQL Generation 
		CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_RTN_QUERY(vRUL_ID,vCC_ID,oSubReturn_Code_rtn_query, oSubReturn_Message_rtn_query,oSubReturnSQL_Text );
		
		-- Message Log portion
		SET	vCntr = vCntr + 1; -- Increase step number by 1
		SET	vLogMsgVariable = 'SQL Generation - Completed';
		SET	vLogMsg = vLogMsgVariable;
		SET	vDebugLvl = 1;
		CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
		
		-- Insert the generated SQL into a target table
		SET sql_stmt = 'MERGE INTO QSIT_APRA2_BRL_RRP_WK.BSNSMD_RUL_GEN_QUERY tgt
										USING
										( SELECT 
											' || vRUL_ID || ' AS Rul_Id
											,' || vCC_ID || ' AS CC_Id
											,''' || OREPLACE(oSubReturnSQL_Text,'''','''''')  || ''' AS GenSQLText
											,CURRENT_TIMESTAMP(6) AS Log_Ts
											,USER AS Usr_Id
										) src
										ON tgt.Rul_Id = src.Rul_Id
										AND tgt.CC_Id = src.CC_Id
										WHEN MATCHED THEN 
										UPDATE 
										SET GenSQLText = src.GenSQLText,
										          Log_Ts = src.Log_Ts,
												  Usr_ID = src.Usr_ID
										WHEN NOT MATCHED THEN
										INSERT (
										       src.Rul_Id,
										       src.CC_Id,
										       src.GenSQLText,
										       src.Log_Ts,
										       src.Usr_Id
										);';
		
		-- Message Log portion
		SET	vCntr = vCntr + 1; -- Increase step number by 1
		SET	vLogMsgVariable = '--SQL that will be executed is:' || cLF || sql_stmt;
		SET	vLogMsg = vLogMsgVariable;
		SET	vDebugLvl = 1;
		CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
		
		EXECUTE IMMEDIATE sql_stmt;
		SET vActivity_Count = ACTIVITY_COUNT;
		
		-- Message Log portion
		SET	vCntr = vCntr + 1; -- Increase step number by 1
		SET	vLogMsgVariable = TRIM(vActivity_Count) || ' records into table QSIT_APRA2_BRL_RRP_WK.BSNSMD_RUL_GEN_QUERY';
		SET	vLogMsg = vLogMsgVariable;
		SET	vDebugLvl = 1;
		CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
		IF oSubReturn_Code = 0 THEN
			SET oReturn_Code = oSubReturn_Code_rtn_query ;
			SET oReturn_Message = oSubReturn_Message_rtn_query ;
	
			-- Execute the SQL generated by the RTN_QUERY procedure for the below condition.
			IF iExec_Mode = 'Y' THEN 	
					
				
				-- Insert data
				SET sql_stmt =   oSubReturnSQL_Text;
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsg =   '/* Generated SQL is: */' || cLF ||  sql_stmt;
				SET	vDebugLvl = 5;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsgVariable = 'Rule ID = '||TRIM(CSR2.Rul_ID) || ' , Before Execute Immediate';
				SET	vLogMsg =  vLogMsgVariable;
				SET	vDebugLvl = 1;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
			
				
				EXECUTE IMMEDIATE sql_stmt;
				SET vActivity_Count = ACTIVITY_COUNT;
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsgVariable = 'Rule ID = '||TRIM(CSR2.Rul_ID) || ' , After Execute Immediate - Completed. No of affected rows are :' || TRIM(vActivity_Count);
				SET	vLogMsg =  vLogMsgVariable;
				SET	vDebugLvl = 1;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
			
			END IF;	
		ELSE	
			SET oReturn_Code = oSubReturn_Code_rtn_query  ;
			SET oReturn_Message = oSubReturn_Message_rtn_query  ;		
		END IF;
SET	vLogMsgFixed =  'For Rule Id = ' || TRIM(vRUL_ID) || ' and Cc_Id = ' || TRIM(iCc_Id);


L_STATS:-- Collect stats on WK table starts here
FOR CSR_DEL AS
			SELECT DISTINCT ENT_NM 
			FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S em 
			INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_MSTR rm
			ON em.ENT_ID = rm.TRG_ENT_ID
			AND rm.CC_ID =CSR1.CC_ID
			AND rm.RUL_ID = COALESCE(inRUL_ID, rm.RUL_ID)

DO
			IF iExec_Mode = 'Y' THEN 	
						
				SET vTRG_ENT_NM = CSR_DEL.ENT_NM;
				SET sql_stmt = 'COLLECT STATISTICS on temporary QSIT_APRA2_BRL_RRP_WK.' || vTRG_ENT_NM || ';';
	
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsg =   '/* Generated statement is: */ ' || cLF ||  sql_stmt;
				SET	vDebugLvl = 5;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
				EXECUTE IMMEDIATE sql_stmt;
				SET vActivity_Count = ACTIVITY_COUNT;
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsgVariable = 'Statistics collected on ' || vTRG_ENT_NM || ' :' || TRIM(vActivity_Count);
				SET	vLogMsg =  vLogMsgVariable;
				SET	vDebugLvl = 1;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
	 END IF;
	 
	
END FOR L_STATS;

	END FOR L2;
	

	---------------- Merging into Main Table stats here

	L_MERGE: -- Target table cleanup
FOR CSR_MERGE AS
			SELECT DISTINCT ENT_NM 
			FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S em 
			INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_MSTR rm
			ON em.ENT_ID = rm.TRG_ENT_ID
			AND rm.CC_ID =CSR1.CC_ID
			AND rm.RUL_ID = COALESCE(inRUL_ID, rm.RUL_ID)
DO
			IF iExec_Mode = 'Y' THEN 	
			        
					SET vTRG_ENT_NM = CSR_MERGE.ENT_NM;
                    IF vTRG_ENT_NM = 'OVRD_AR_TO_GL_LNK_INTER' THEN 
							SET sql_stmt = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.' || vTRG_ENT_NM || ';';
					ELSE
							SET sql_stmt = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.' || vTRG_ENT_NM || '  TRG WHERE 1=1'
											|| ' AND RUL_ID = COALESCE(' || COALESCE(inRUL_ID,'NULL') || ', TRG.RUL_ID) '
											|| ' AND CC_ID = COALESCE(' ||COALESCE(inCC_ID,'NULL')  || ', TRG.CC_ID); ';
					END IF;

				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsg =   '/* Generated SQL is: */' || cLF ||  sql_stmt;
				SET	vDebugLvl = 5;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
				EXECUTE IMMEDIATE sql_stmt;
				SET vActivity_Count = ACTIVITY_COUNT;
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsgVariable = 'For iExec_Mode = Y, Records in target table ' || vTRG_ENT_NM || 'deleted for  RUL_ID = ' ||  COALESCE(inRUL_ID,'ALL') || ' No of affected rows are :' || TRIM(vActivity_Count);
				SET	vLogMsg =  vLogMsgVariable;
				SET	vDebugLvl = 1;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

				SET SQL_STMT = ' INSERT INTO QSIT_APRA2_BRL_RRP_VW.'||vTRG_ENT_NM||'   SELECT * FROM QSIT_APRA2_BRL_RRP_WK.'||vTRG_ENT_NM||';';
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsg =   '/* Generated SQL is: */' || cLF ||  sql_stmt;
				SET	vDebugLvl = 5;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
				EXECUTE IMMEDIATE sql_stmt;
				SET vActivity_Count = ACTIVITY_COUNT;
				
				-- Message Log portion
				SET	vCntr = vCntr + 1; -- Increase step number by 1
				SET	vLogMsgVariable = 'For iExec_Mode = Y, Records Merged in target table ' || vTRG_ENT_NM || ' for  RUL_ID = ' ||  COALESCE(inRUL_ID,'ALL') || ' No of affected rows are :' || TRIM(vActivity_Count);
				SET	vLogMsg =  vLogMsgVariable;
				SET	vDebugLvl = 1;
				CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
				
	 END IF;
	 
	
END FOR L_MERGE;

	---------------- Merging into Main Table ends here
	
	END FOR L1;

SET vCntr = vCntr + 1; -- Increase step number by 1
SET	vLogMsgVariable = 'Process - Completed';
SET vLogMsg = vLogMsgFixed || ' : ' || vLogMsgVariable;
SET	vDebugLvl = 1;
CALL	QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
END;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR
-- =============================================
-- Description: This Stored Procedure will call the other procedures for uploader activity.
-- Change log
--      [2015 02 04]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
IN iEffv_Dt DATE,
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vActivity_Ct INTEGER DEFAULT -99;
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(1000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'BSNSMD_UPLDR';


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

--UNCOMMENT IT WHEN GOOD DATA IS AVAILABLE
/*-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsg || ''' because no rows were returned from the sql.';
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;
*/

SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** to generate surrogate keys
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Before calling procedure to generate surrogate keys';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_GEN_SURRKEY (oSubReturn_Code, oSubReturn_Message);
IF oSubReturn_Code <> 0 THEN
    SET oReturn_Code = oSubReturn_Code;
    SET oReturn_Message = oSubReturn_Message;
    LEAVE MAIN;
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'After calling procedure to generate surrogate keys';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- **************************************************** validate data (pre merge)
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Before calling procedure to validate data (pre merge)';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

 CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_VALIDATE_PREMERGE (oSubReturn_Code, oSubReturn_Message);
IF oSubReturn_Code <> 0 THEN
    SET oReturn_Code = oSubReturn_Code;
    SET oReturn_Message = oSubReturn_Message;
    LEAVE MAIN;
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'After calling procedure to validate data (pre merge)';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- **************************************************** merge data
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Before calling procedure to merge data';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_MERGE (iEffv_Dt, oSubReturn_Code, oSubReturn_Message);
IF oSubReturn_Code <> 0 THEN
    SET oReturn_Code = oSubReturn_Code;
    SET oReturn_Message = oSubReturn_Message;
    LEAVE MAIN;
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'After calling procedure to merge data';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

/*
-- **************************************************** validate data (post merge)
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Before calling procedure to validate data (post merge)';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_VALIDATE_PSTMERGE (oSubReturn_Code, oSubReturn_Message);
IF oSubReturn_Code <> 0 THEN
    SET oReturn_Code = oSubReturn_Code;
    SET oReturn_Message = oSubReturn_Message;
    LEAVE MAIN;
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'After calling procedure to validate data (post merge)';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
*/

-- ****************************************************  exiting
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_GEN_SURRKEY
-- =============================================
-- Description: This Stored Procedure will generate/ derive the surrogate keys and update the uploader intermediate table.
-- Change log
--      [2015 02 04]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vActivity_Ct INTEGER DEFAULT -99;
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(10000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'BSNSMD_UPLDR_GEN_SURRKEY';


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;
/*
-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsg || ''' because no rows were returned from the sql.';
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;
*/
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';

SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- *********************************** Delete Intermediate Table
SET vSQL_Text = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_UPLDR_INTER Deleted - ' || TRIM(vActivity_Ct) || ' Rows Deleted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- *********************************** Load Intermediate Table
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER (
 RUL_NM
,RUL_ID
,RUL_DESC
,TY
,DQ_TY
,SRC_SYS_CD
,SRC_ENT_NM
,BSNS_SRC_ENT_NM
,SRC_ENT_ID
,CC
,CC_ID
,CC_VAL
,CC_VAL_ID
,IS_DFLT
,PRTY
,FLTR_COL_NM
,BSNS_FLTR_COL_NM
,FLTR_COL_ATTR_ID
,LST_ID
,FLTR_COND
,FLTR_VAL
,FLTR_LOW_COND
,FLTR_LOW_VAL
,FLTR_HGH_COND
,FLTR_HGH_VAL
,TRG_TBL
,BSNS_TRG_TBL
,TRG_TBL_ENT_ID
,SRC_ATTR_ID
,TRG_ATTR_ID
,PRCS_FLG
,ERR_MSG
,IS_GEN
,PRTN_BY_COL_LST
,ORDR_BY_COL_LST
)
SELECT
 RUL_NM AS RUL_NM
,NULL AS RUL_ID
,RUL_DESC AS RUL_DESC
,TY AS TY
,DQ_TY AS DQ_TY
,SRC_SYS_CD AS SRC_SYS_CD
,NULL AS SRC_ENT_NM
,SRC_ENT_NM AS BSNS_SRC_ENT_NM -- source data hold business names
,NULL AS SRC_ENT_ID
,CC AS CC
,NULL AS CC_ID
,CC_VAL AS CC_VAL
,NULL AS CC_VAL_ID
,IS_DFLT AS IS_DFLT
,PRTY AS PRTY
,NULL AS FLTR_COL_NM
,FLTR_COL_NM AS BSNS_FLTR_COL_NM -- source data hold business names
,NULL AS FLTR_COL_ATTR_ID
,NULL AS LST_ID
,FLTR_COND AS FLTR_COND
,FLTR_VAL AS FLTR_VAL
,FLTR_LOW_COND AS FLTR_LOW_COND
,FLTR_LOW_VAL AS FLTR_LOW_VAL
,FLTR_HGH_COND AS FLTR_HGH_COND
,FLTR_HGH_VAL AS FLTR_HGH_VAL
,NULL AS TRG_TBL
,TRG_TBL AS BSNS_TRG_TBL -- source data hold business names
,NULL AS TRG_TBL_ENT_ID
,NULL AS SRC_ATTR_ID
,NULL AS TRG_ATTR_ID
,''N'' AS PRCS_FLG
,NULL AS ERR_MSG
,''N'' AS IS_GEN
,PRTN_BY_COL_LST AS PRTN_BY_COL_LST
,ORDR_BY_COL_LST AS ORDR_BY_COL_LST
FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_UPLDR_INTER Loaded - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- *********************************** Update FLTR_COL_NM in Intermediate Table
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
FROM (SELECT ATTR_NM, BSNS_ATTR_NM, ENT_NM, BSNS_ENT_NM FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S a INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S e ON a.ENT_ID = e.ENT_ID ) a
SET FLTR_COL_NM = ATTR_NM
WHERE BSNS_FLTR_COL_NM = BSNS_ATTR_NM
AND BSNS_SRC_ENT_NM = BSNS_ENT_NM;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Update FLTR_COL_NM in Intermediate Table - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- *********************************** Update TRG_TBL in Intermediate Table
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
FROM (SELECT ENT_NM, BSNS_ENT_NM FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S) a
SET TRG_TBL = ENT_NM
WHERE BSNS_TRG_TBL = BSNS_ENT_NM;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Update TRG_TBL in Intermediate Table - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- *********************************** Update SRC_ENT_NM in Intermediate Table
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
FROM (SELECT ENT_NM, BSNS_ENT_NM FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S) a
SET SRC_ENT_NM = ENT_NM
WHERE BSNS_SRC_ENT_NM = BSNS_ENT_NM;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Update SRC_ENT_NM in Intermediate Table - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

--*********************************** SRC_ENT_ID derivation
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER  
                FROM (SELECT ENT_ID, ENT_NM FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S GROUP BY 1, 2) src
                SET SRC_ENT_ID = src.ENT_ID
                WHERE SRC_ENT_NM = src.ENT_NM;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Column SRC_ENT_ID derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

--*********************************** CC_ID derivation
SET vSQL_Text = ' UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER  
						                FROM (SELECT CC_ID, CC_SHRT_NM FROM QSIT_APRA2_BRL_RRP_VW.CC_MSTR_S GROUP BY 1, 2) src
						                SET CC_ID = src.CC_ID
						                WHERE CC = src.CC_SHRT_NM;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Column CC_ID derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

--*********************************** CC_VAL_ID derivation
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER  
                FROM (SELECT rui.TY, rui.CC_ID, CASE WHEN  rui.TY = ''PASSTHRU'' THEN -99 ELSE ccv.CC_VAL_ID END AS CC_VAL_ID, COALESCE (ccv.CC_VAL_SHRT_NM, rui.CC_VAL) AS CC_VAL_NM
                                 FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER rui
                                 LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.CC_VAL_S ccv
                                 ON rui.CC_ID = ccv.CC_ID
                                 ---AND rui.CC_VAL = ccv.CC_VAL_NM
                                 AND rui.CC_VAL = ccv.CC_VAL_SHRT_NM
                                 GROUP BY 1, 2, 3, 4) src
                SET CC_VAL_ID = SRC.CC_VAL_ID
                WHERE CC_VAL = src.CC_VAL_NM
                --WHERE CC_VAL = src.CC_VAL_SHRT_NM
                AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.TY = src.TY
                AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.CC_ID = src.CC_ID;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Column CC_VAL_ID derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

--*********************************** FLTR_COL_ATTR_ID derivation
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER  
                FROM (SELECT ENT_ID, ATTR_ID, ATTR_NM, BSNS_ATTR_NM FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S   GROUP BY 1, 2, 3, 4) src
                SET FLTR_COL_ATTR_ID = src.ATTR_ID
                WHERE FLTR_COL_NM = src.ATTR_NM
				AND BSNS_FLTR_COL_NM = src.BSNS_ATTR_NM
                AND SRC_ENT_ID = src.ENT_ID;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Column FLTR_COL_ATTR_ID derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

--*********************************** TRG_TBL_ENT_ID derivation
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER  
                FROM (SELECT ENT_ID, ENT_NM FROM QSIT_APRA2_BRL_RRP_VW.ENT_MSTR_S GROUP BY 1, 2) src
                SET TRG_TBL_ENT_ID = src.ENT_ID
                WHERE TRG_TBL = src.ENT_NM;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Column TRG_TBL_ENT_ID derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

--*********************************** SRC_ATTR_ID & TRG_ATTR_ID derivation - for 1st PK column
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
										FROM (
											SELECT	
											 RUI.RUL_NM
											,COALESCE(RUI.FLTR_COL_ATTR_ID,0) AS FLTR_COL_ATTR_ID
											,RUI.SRC_ENT_ID
											,RUI.TRG_TBL_ENT_ID
											,AM_SRC.ATTR_ID AS SRC_ATTR_ID
											,AM_TRG.ATTR_ID AS TRG_ATTR_ID
											FROM	QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER RUI
											LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S  AM_SRC
											ON RUI.SRC_ENT_ID = AM_SRC.ENT_ID
												AND COALESCE (AM_SRC.IS_PK, '''') = ''Y''
											LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S   AM_TRG
											ON RUI.TRG_TBL_ENT_ID = AM_TRG.ENT_ID
												AND COALESCE (AM_TRG.IS_PK, '''') = ''Y''
											WHERE RUI.IS_GEN = ''N''
											AND AM_TRG.ATTR_NM = AM_SRC.ATTR_NM
											AND COALESCE(AM_SRC.ATTR_ID, AM_TRG.ATTR_ID) IS NOT NULL
											QUALIFY ROW_NUMBER() OVER(PARTITION BY RUL_NM, COALESCE(FLTR_COL_ATTR_ID, 0) ORDER BY COALESCE(AM_SRC.ATTR_ID, AM_TRG.ATTR_ID)) = 1 -- Update the values for 1st PK column
										) SRC
										SET SRC_ATTR_ID = SRC.SRC_ATTR_ID
													,TRG_ATTR_ID = SRC.TRG_ATTR_ID
										WHERE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.RUL_NM = SRC.RUL_NM
										AND COALESCE(QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.FLTR_COL_ATTR_ID, 0) = SRC.FLTR_COL_ATTR_ID
										AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.SRC_ENT_ID = SRC.SRC_ENT_ID
										AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.TRG_TBL_ENT_ID = SRC.TRG_TBL_ENT_ID;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Columns SRC_ATTR_ID & TRG_ATTR_ID derivation - for 1st PK column derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


--*********************************** SRC_ATTR_ID & TRG_ATTR_ID derivation - Next set of PKs
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER (
										 RUL_NM
										,RUL_ID
										,RUL_DESC
										,TY
										,DQ_TY
										,SRC_SYS_CD
										,SRC_ENT_NM
										,BSNS_SRC_ENT_NM
										,SRC_ENT_ID
										,CC
										,CC_ID
										,CC_VAL
										,CC_VAL_ID
										,IS_DFLT
										,PRTY
										,FLTR_COL_NM
										,BSNS_FLTR_COL_NM
										,FLTR_COL_ATTR_ID
										,LST_ID
										,FLTR_COND
										,FLTR_VAL
										,FLTR_LOW_COND
										,FLTR_LOW_VAL
										,FLTR_HGH_COND
										,FLTR_HGH_VAL
										,TRG_TBL
										,BSNS_TRG_TBL
										,TRG_TBL_ENT_ID
										,SRC_ATTR_ID
										,TRG_ATTR_ID
										,PRCS_FLG
										,ERR_MSG
										,IS_GEN
										)
										SELECT	
										 RUI.RUL_NM
										,RUI.RUL_ID
										,RUI.RUL_DESC
										,RUI.TY
										,RUI.DQ_TY
										,RUI.SRC_SYS_CD
										,RUI.SRC_ENT_NM
										,RUI.BSNS_SRC_ENT_NM
										,RUI.SRC_ENT_ID
										,RUI.CC
										,RUI.CC_ID
										,RUI.CC_VAL
										,RUI.CC_VAL_ID
										,RUI.IS_DFLT
										,RUI.PRTY
										,RUI.FLTR_COL_NM
										,RUI.BSNS_FLTR_COL_NM
										,RUI.FLTR_COL_ATTR_ID
										,RUI.LST_ID
										,RUI.FLTR_COND
										,RUI.FLTR_VAL
										,RUI.FLTR_LOW_COND
										,RUI.FLTR_LOW_VAL
										,RUI.FLTR_HGH_COND
										,RUI.FLTR_HGH_VAL
										,RUI.TRG_TBL
										,RUI.BSNS_TRG_TBL
										,RUI.TRG_TBL_ENT_ID
										,AM_SRC.ATTR_ID AS SRC_ATTR_ID
										,AM_TRG.ATTR_ID AS TRG_ATTR_ID
										,RUI.PRCS_FLG
										,RUI.ERR_MSG
										,''Y'' AS IS_GEN
										FROM	QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER RUI
										LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S  AM_SRC
										ON RUI.SRC_ENT_ID = AM_SRC.ENT_ID
											AND COALESCE (AM_SRC.IS_PK, '''') IN (''Y'',''P'')
										LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR_S   AM_TRG
										ON RUI.TRG_TBL_ENT_ID = AM_TRG.ENT_ID
											AND COALESCE (AM_TRG.IS_PK, '''')  IN (''Y'',''P'')
										WHERE RUI.IS_GEN = ''N''
										AND AM_TRG.ATTR_NM = AM_SRC.ATTR_NM
										AND COALESCE(AM_SRC.ATTR_ID, AM_TRG.ATTR_ID) IS NOT NULL -- Insert the values for other PK columns (more than 1 PK)
										QUALIFY ROW_NUMBER() OVER(PARTITION BY RUL_NM, COALESCE(FLTR_COL_ATTR_ID, 0) ORDER BY COALESCE(AM_SRC.ATTR_ID, AM_TRG.ATTR_ID)) > 1; ';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Columns SRC_ATTR_ID & TRG_ATTR_ID derivation - Next set of PKs derived - ' || TRIM(vActivity_Ct) || ' ROWS Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


--*********************************** RUL_ID derivation
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER  
                FROM (SELECT 
								ru.Rul_Nm AS Rul_Nm
								,COALESCE(rm.Rul_Id, Max_Cnt + Row_Cnt) AS Rul_Id
								FROM
								(SELECT Rul_Nm, ROW_NUMBER() OVER(ORDER BY Rul_Nm) AS Row_Cnt
								FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER GROUP BY 1 )  ru
								LEFT OUTER JOIN (SELECT Rul_Id, Rul_Nm FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S ) rm
								ON ru.RUL_NM = rm.RUL_NM
								CROSS JOIN (SELECT COALESCE (MAX(Rul_Id),0) AS Max_Cnt FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR_S ) rm_max
								GROUP BY 1, 2) src
                SET RUL_ID = src.RUL_ID
                WHERE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.Rul_Nm = src.Rul_Nm;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Column RUL_ID derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

--*********************************** LST_ID derivation
SET vSQL_Text = 'UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER  
                FROM (SELECT 
								ru.Rul_Id AS Rul_Id
								,COALESCE(rm.LST_ID, Max_Cnt + Row_Cnt) AS LST_ID		
								,FLTR_COL_ATTR_ID
								,FLTR_COND						
								FROM
								(SELECT RUL_ID, FLTR_COL_ATTR_ID, FLTR_COND, ROW_NUMBER() OVER(ORDER BY Rul_Id) AS Row_Cnt
								FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
							--	WHERE FLTR_COND NOT IN (''NULL'', ''NOT NULL'') 
								GROUP BY 1, 2, 3 )  ru
								LEFT OUTER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S  rm
								ON ru.Rul_Id = rm.Rul_Id
								AND ru.FLTR_COND = rm.LINK_TY
								AND ru.FLTR_COL_ATTR_ID = rm.ATTR_ID
								CROSS JOIN (SELECT COALESCE (MAX(LST_ID),0) AS Max_Cnt FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR_S ) rm_max
								GROUP BY 1, 2, 3,4) src
                SET LST_ID = src.LST_ID
                WHERE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.Rul_Id = src.Rul_Id
                AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.FLTR_COL_ATTR_ID = SRC.FLTR_COL_ATTR_ID
                AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.FLTR_COND = SRC.FLTR_COND;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Column LST_ID derived - ' || TRIM(vActivity_Ct) || ' Rows Updated';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_MERGE
-- =============================================
-- Description: This Stored Procedure will perform insert into BSNS metadata tables from uploader intermediate table.
-- Change log
--      [2015 02 04]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
IN iEffv_Dt DATE,
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vActivity_Ct INTEGER DEFAULT -99;
DECLARE vFilterExpression VARCHAR(10000);
DECLARE vTokenCount SMALLINT DEFAULT 0;
DECLARE vToken VARCHAR(100);
DECLARE vIs_Present CHAR(1);
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(10000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'BSNSMD_UPLDR_MERGE';
DECLARE vEffv_Dt VARCHAR(30);
DECLARE vExpr_Dt VARCHAR(30) ;
DECLARE vUsr_Id VARCHAR(255) DEFAULT USER;

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;
/*
-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsg || ''' because no rows were returned from the sql.';
	
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;
*/
-- Set the fixed part of the log message.
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- set values for effective date and expiry date
SET vEffv_Dt = CAST(iEffv_Dt AS DATE FORMAT 'YYYY-MM-DD');
SET vExpr_Dt = CAST('9999-12-31' AS DATE FORMAT 'YYYY-MM-DD');


-- ***********************************  RUL_MSTR - Expiration
SET vSQL_Text = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.RUL_MSTR WHERE RUL_ID IN (SELECT RUL_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER GROUP BY 1);';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_MSTR Cleaned Up - ' || TRIM(vActivity_Ct) || ' Rows Deleted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);



-- ***********************************  RUL_MSTR - Insert
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_MSTR ( 
																							RUL_ID
																				           ,RUL_NM
																				           ,TY
																				           ,RUL_DESC
																				           ,DQ_TY
																				           ,SRC_SYS_CD
																				           ,IS_DFLT
																				           ,DATA_ENT_ID
																				           ,TRG_ENT_ID
																				           ,CC_ID
																				           ,CC_VAL_ID
																				           ,PRTY
																				           ,EFFV_DT
																				           ,EXPR_DT
																				           ,USR_ID
																							) SELECT 
																							 RUL_ID
																							,RUL_NM
																							,TY
																							,RUL_DESC 
																							,DQ_TY
																							,SRC_SYS_CD
																							,IS_DFLT 
																							,SRC_ENT_ID AS DATA_ENT_ID
																							,TRG_TBL_ENT_ID AS TRG_ENT_ID
																							,CC_ID
																							,CC_VAL_ID
																							,PRTY
																							, ''' || vEffv_Dt || ''' (date)' || cLF
																							|| ',''' || vExpr_Dt || ''' (date)' || cLF
																							|| ',''' || vUsr_Id || '''
																							FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
																							GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_MSTR Inserted - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- ***********************************  RUL_ATTR - Expiration
SET vSQL_Text = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.RUL_ATTR WHERE RUL_ID IN (SELECT RUL_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER GROUP BY 1);';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_ATTR Cleaned Up - ' || TRIM(vActivity_Ct) || ' Rows Deleted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  RUL_ATTR - Insert
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_ATTR ( 
																							RUL_ID
																				           ,ATTR_ID
																				           ,LINK_TY
																				           ,LST_ID
																				           ,EFFV_DT
																						   ,EXPR_DT
																						   ,USR_ID
																							) 
																							SELECT 
																							RUL_ID
																							,FLTR_COL_ATTR_ID AS ATTR_ID
																							,CASE 
																							 WHEN FLTR_COND = ''BEGIN WITH'' THEN ''LIKE''
																							 WHEN FLTR_COND = ''END WITH'' THEN ''LIKE''
																							 WHEN FLTR_COND = ''CONTAINS'' THEN ''LIKE''
																							 WHEN FLTR_COND = ''NULL'' THEN ''IS NULL''
																							 WHEN FLTR_COND = ''NOT NULL'' THEN ''IS NOT NULL''
																							 ELSE FLTR_COND
																							 END AS LINK_TY
																							,LST_ID
																							, ''' || vEffv_Dt || ''' (date)' || cLF
																							|| ',''' || vExpr_Dt || ''' (date)' || cLF
																							|| ',''' || vUsr_Id || '''
																							FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
																							WHERE FLTR_COND <> ''''
																							AND   FLTR_COND <> ''SETVAL''
																							GROUP BY 1, 2, 3, 4, 5, 6, 7;';
-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_ATTR Inserted - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  LST_MSTR - Expiration
SET vSQL_Text = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.LST_MSTR WHERE LST_ID IN (SELECT ra.LST_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER ru INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_ATTR ra ON ru.RUL_ID = ra.RUL_ID GROUP BY 1)';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table LST_MSTR Cleaned Up - ' || TRIM(vActivity_Ct) || ' Rows Deleted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  LST_MSTR - Insert
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.LST_MSTR (LST_ID, ATTR_ID, LST_TY, EFFV_DT, EXPR_DT ,USR_ID )
																																SELECT 
																																LST_ID
																																,FLTR_COL_ATTR_ID AS ATTR_ID
																																,CASE 
																																             WHEN FLTR_COND IN (''IN'', ''NOT IN'') THEN ''LOV''
																																             WHEN FLTR_COND = ''RANGE'' THEN ''RANGE''
																																			 ELSE FLTR_COND 
																																END AS LST_TY
																																, ''' || vEffv_Dt || ''' (date)' || cLF
																																|| ',''' || vExpr_Dt || ''' (date)' || cLF
																																|| ',''' || vUsr_Id || '''
																																FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
																																WHERE FLTR_COND NOT IN ('''', ''SETVAL'') -- , ''NULL'', ''NOT NULL'')
																																GROUP BY 1, 2, 3, 4, 5, 6;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table LST_MSTR Inserted - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- ***********************************  LST_RNG - Expiration
SET vSQL_Text = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.LST_RNG WHERE LST_ID IN (SELECT ra.LST_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER ru INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_ATTR ra ON ru.RUL_ID = ra.RUL_ID GROUP BY 1)';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table LST_RNG Cleaned Up - ' || TRIM(vActivity_Ct) || ' Rows Deleted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  LST_RNG - Insert
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.LST_RNG ( 
																							LST_ID
																				           ,LOW_VAL
																				           ,LOW_COND
																				           ,HGH_VAL
																				           ,HGH_COND
																				           ,EFFV_DT
																				           ,EXPR_DT
																				           ,USR_ID
																							) SELECT 
																							LST_ID
																							,FLTR_LOW_VAL AS LOW_VAL
																							,FLTR_LOW_COND AS LOW_COND
																							,FLTR_HGH_VAL AS HGH_VAL
																							,FLTR_HGH_COND AS HGH_COND
																							, ''' || vEffv_Dt || ''' (date)' || cLF
																							|| ',''' || vExpr_Dt || ''' (date)' || cLF
																							|| ',''' || vUsr_Id || '''
																							FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
																							WHERE FLTR_COND = ''RANGE''
																							GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table LST_RNG Inserted - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  LST_VAL - Expiration
	-- NOTE: After addition of audit column in LST_VAL table. This should be replaced by UPDATE statement to expire existing records for a LST_ID
	SET vSQL_Text = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.LST_VAL WHERE LST_ID IN (SELECT ra.LST_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER ru INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_ATTR ra ON ru.RUL_ID = ra.RUL_ID GROUP BY 1)';
	
	-- Log SQL Text 
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
	SET vDebugLvl = 5;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
	EXECUTE IMMEDIATE vSQL_Text;
	SET vActivity_Ct = ACTIVITY_COUNT;
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Table LST_VAL Cleaned Up - ' || TRIM(vActivity_Ct) || ' Rows Deleted';
	SET vDebugLvl = 1;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  LST_VAL - Insert
L1:
FOR	CSR1 AS 
	SELECT	
	 LST_ID
	 ,FLTR_VAL
	 ,CHARACTERS(TRIM(FLTR_VAL)) - CHARACTERS(TRIM(OREPLACE(FLTR_VAL,',',''))) + 1 AS TokenCount
	FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
	WHERE FLTR_COND IN  ('IN' , 'NOT IN', 'CONTAINS', 'BEGIN WITH', 'END WITH')
	GROUP BY 1, 2, 3
	ORDER BY LST_ID
DO
	SET vFilterExpression = CSR1.FLTR_VAL;
	SET vTokenCount = 0 ;
    
    -- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'First Level Loop - Fetched Filter Value.';
	SET vDebugLvl = 4;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

    L2: LOOP
    BEGIN
		SET vTokenCount = vTokenCount + 1;
		SET vToken = TRIM(STRTOK(vFilterExpression, ',', vTokenCount));
		
	    -- Message Log portion
		SET vCntr = vCntr + 1; -- Increase step number by 1
		SET vLogMsg = 'Second Level Loop  - Extracted Individual Token from Filter Value';
		SET vDebugLvl = 5;
		CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

		SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.LST_VAL (LST_ID, SQNC_NBR, VAL, EFFV_DT, EXPR_DT, USR_ID)
													VALUES ( ' || CSR1.LST_ID || ',' || vTokenCount || ',''' || vToken || ''',''' || vEffv_Dt || ''' (date), ''' || vExpr_Dt || ''' (date), ''' || vUsr_Id || ''');';
		
		-- Log SQL Text 
		SET vCntr = vCntr + 1; -- Increase step number by 1
		SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
		SET vDebugLvl = 5;
		CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

		EXECUTE IMMEDIATE vSQL_Text;
		SET vActivity_Ct = ACTIVITY_COUNT;
		
		-- Message Log portion
		SET vCntr = vCntr + 1; -- Increase step number by 1
		SET vLogMsg = 'Table LST_VAL Inserted - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
		SET vDebugLvl = 1;
		CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

		-- If the loop's token count is greater of equal to # of tokens present in token value then come out of the loop
		IF vTokenCount >= CSR1.TokenCount THEN
			LEAVE L2;
		END IF;
		
	END;
	END LOOP L2;
END FOR L1;


-- ***********************************  RUL_TRG_DTL - Expiration
SET vSQL_Text = 'DELETE FROM QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL WHERE RUL_ID IN (SELECT RUL_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER GROUP BY 1);';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_TRG_DTL Cleaned Up - ' || TRIM(vActivity_Ct) || ' Rows Deleted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);



-- ***********************************  RUL_TRG_DTL - Insert 
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL ( 
										RUL_ID
										,SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										) SELECT 
										RUL_ID
										,ROW_NUMBER () OVER(PARTITION BY RUL_ID ORDER BY RUL_ID) AS SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										FROM (
										SELECT 
										 RUL_ID
										,''INSERT'' AS TRG_TY										
										,NULL AS TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,NULL AS PRTN_BY_COL_LST
										,NULL AS ORDR_BY_COL_LST
										, ''' || vEffv_Dt || ''' (date) AS EFFV_DT' || cLF
										|| ',''' || vExpr_Dt || ''' (date) AS EXPR_DT' || cLF
										|| ',''' || vUsr_Id || ''' AS USR_ID
										FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										WHERE FLTR_COND <> ''SETVAL''
										GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) a
										;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_TRG_DTL Inserted - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  RUL_TRG_DTL - Insert for ATTR_ID Column (TY = Ranking)
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL ( 
										RUL_ID
										,SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										) SELECT 
										a.RUL_ID
										, COALESCE (rui_max.max_cnt, 0) + ROW_NUMBER () OVER(PARTITION BY a.RUL_ID ORDER BY a.RUL_ID) AS SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										FROM (
										SELECT 
										 rui.RUL_ID
										,''INSERT'' AS TRG_TY										
										,ATTR_SRC.ATTR_ID AS TRG_VAL
										,ATTR_TGT.ATTR_ID AS TRG_ATTR_ID
										,NULL AS SRC_ATTR_ID
										,NULL AS PRTN_BY_COL_LST
										,NULL AS ORDR_BY_COL_LST
										, ''' || vEffv_Dt || ''' (date) AS EFFV_DT' || cLF
										|| ',''' || vExpr_Dt || ''' (date) AS EXPR_DT' || cLF
										|| ',''' || vUsr_Id || ''' AS USR_ID
										FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER rui
										INNER JOIN(SELECT rui.RUL_ID, am.ATTR_ID, am.ENT_ID
																		FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR am
																		INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR em
																		ON am.ENT_ID = em.ENT_ID
																		INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER rui
																		ON em.ENT_NM = rui.SRC_ENT_NM
																		AND rui.TY = ''RANKING''							
																		AND ATTR_NM = ''RNK_ID'' 
																		GROUP BY 1, 2, 3 ) ATTR_SRC
										ON rui. SRC_ENT_ID = ATTR_SRC.ENT_ID
										AND rui.RUL_ID = ATTR_SRC.RUL_ID		
										INNER JOIN(SELECT rui.RUL_ID, am.ATTR_ID, am.ENT_ID
																		FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR am
																		INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR em
																		ON am.ENT_ID = em.ENT_ID
																		INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER rui
																		ON em.ENT_NM = rui.TRG_TBL
																		AND rui.TY = ''RANKING''								
																		AND ATTR_NM = ''ATTR_ID'' 
																		GROUP BY 1, 2, 3 ) ATTR_TGT
										ON rui. TRG_TBL_ENT_ID = ATTR_TGT.ENT_ID
										AND rui.RUL_ID = ATTR_TGT.RUL_ID
										WHERE rui.FLTR_COND <> ''SETVAL'' 
										AND rui.TY = ''RANKING''
										GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) a
										
										LEFT OUTER JOIN (SELECT RUL_ID, COUNT(1) AS max_cnt FROM QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL GROUP BY 1) rui_max
										ON a.RUL_ID = rui_max.RUL_ID;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_TRG_DTL Inserted for ATTR_ID Column (TY = Ranking) rules - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- ***********************************  RUL_TRG_DTL - Insert for VAL Column (TY = Ranking)
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL ( 
										RUL_ID
										,SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										) SELECT 
										a.RUL_ID
										,COALESCE (rui_max.max_cnt, 0) + ROW_NUMBER () OVER(PARTITION BY a.RUL_ID ORDER BY a.RUL_ID) AS SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										FROM (SELECT 
										 rui.RUL_ID
										 ,''INSERT'' AS TRG_TY										
										,NULL AS TRG_VAL
										,ATTR_TGT.ATTR_ID AS TRG_ATTR_ID
										,NULL AS SRC_ATTR_ID
										,rui_prtn.PRTN_BY_COL_LST AS PRTN_BY_COL_LST
										,rui_prtn.ORDR_BY_COL_LST AS ORDR_BY_COL_LST
										, ''' || vEffv_Dt || ''' (date) AS EFFV_DT' || cLF
										|| ',''' || vExpr_Dt || ''' (date) AS EXPR_DT' || cLF
										|| ',''' || vUsr_Id || ''' AS USR_ID
										FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER rui
										INNER JOIN (SELECT RUL_ID, PRTN_BY_COL_LST, ORDR_BY_COL_LST FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										                            WHERE PRTN_BY_COL_LST IS NOT NULL AND ORDR_BY_COL_LST IS NOT NULL
										                            GROUP BY 1, 2, 3) rui_prtn
										ON rui.RUL_ID = rui_prtn.RUL_ID
										INNER JOIN(SELECT rui.RUL_ID, am.ATTR_ID, am.ENT_ID
																		FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR am
																		INNER JOIN QSIT_APRA2_BRL_RRP_VW.ENT_MSTR em
																		ON am.ENT_ID = em.ENT_ID
																		INNER JOIN QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER rui
																		ON em.ENT_NM = rui.TRG_TBL
																		AND rui.TY = ''RANKING''								
																		AND ATTR_NM = ''VAL'' 
																		GROUP BY 1, 2, 3 ) ATTR_TGT
										ON rui. TRG_TBL_ENT_ID = ATTR_TGT.ENT_ID
										AND rui.RUL_ID = ATTR_TGT.RUL_ID
										WHERE rui.FLTR_COND <> ''SETVAL'' 
										AND rui.TY = ''RANKING''
										GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) a
										LEFT OUTER JOIN (SELECT RUL_ID, COUNT(1) AS max_cnt FROM QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL GROUP BY 1) rui_max
										ON a.RUL_ID = rui_max.RUL_ID
										;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_TRG_DTL Inserted for VAL Column (TY = Ranking) rules - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- ***********************************  RUL_TRG_DTL - Insert for SETVAL
SET vSQL_Text = 'INSERT INTO QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL ( 
										RUL_ID
										,SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										) SELECT 
										a.RUL_ID
										,rui_max.max_cnt + ROW_NUMBER () OVER(PARTITION BY a.RUL_ID ORDER BY a.RUL_ID) AS SQNC_NBR
										,TRG_TY
										,TRG_VAL
										,TRG_ATTR_ID
										,SRC_ATTR_ID
										,PRTN_BY_COL_LST
										,ORDR_BY_COL_LST
										,EFFV_DT
										,EXPR_DT
										,USR_ID
										FROM (
										SELECT 
										 rui.RUL_ID
										,''SETVAL'' AS TRG_TY										
										,FLTR_VAL AS TRG_VAL
										,am.ATTR_ID AS TRG_ATTR_ID
										,NULL AS SRC_ATTR_ID
										,NULL AS PRTN_BY_COL_LST
										,NULL AS ORDR_BY_COL_LST
										, ''' || vEffv_Dt || ''' (date) AS EFFV_DT' || cLF
										|| ',''' || vExpr_Dt || ''' (date) AS EXPR_DT' || cLF
										|| ',''' || vUsr_Id || ''' AS USR_ID
										FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER rui
										INNER JOIN (SELECT ENT_ID, ATTR_ID, ATTR_NM FROM QSIT_APRA2_BRL_RRP_VW.ATTR_MSTR) am
										ON rui.TRG_TBL_ENT_ID = am.ENT_ID
										AND rui.FLTR_COL_NM = am.ATTR_NM
										WHERE FLTR_COND = ''SETVAL'' 
										GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) a
										LEFT OUTER JOIN (SELECT RUL_ID, COUNT(1) AS max_cnt FROM QSIT_APRA2_BRL_RRP_VW.RUL_TRG_DTL GROUP BY 1) rui_max
										ON a.RUL_ID = rui_max.RUL_ID
										;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Table RUL_TRG_DTL Inserted for SETVAL - ' || TRIM(vActivity_Ct) || ' Rows Inserted';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);



-- ***********************************  Program End

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_VALIDATE_PREMERGE
-- =============================================
-- Description: This Stored Procedure will call the other procedures for uploader activity.
-- Change log
--      [2015 02 04]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vRecordCount INTEGER DEFAULT 100;
DECLARE vActivity_Ct INTEGER DEFAULT -99;
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vFailureMessage VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(1000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'BSNSMD_UPLDR_VALIDATE_PREMERGE';


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

--UNCOMMENT IT WHEN GOOD DATA IS AVAILABLE
/*-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsg || ''' because no rows were returned from the sql.';
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;
*/

SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- LST_ID, FLTR_COND Uniqueness
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										FROM (SELECT Rul_Nm, Rul_Id, Lst_Id, Fltr_Col_Attr_Id, Fltr_Cond, COUNT(*) AS Row_Cnt
													FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
													WHERE Is_Gen <> ''Y''
													GROUP BY 1, 2, 3, 4, 5
													HAVING COUNT(*) > 1 ) a
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''LIST ID AND FILTER CONDITION HAS MORE THAN 1 RECORD''
                WHERE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.Rul_Id = a.Rul_Id
				AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.Lst_Id = a.Lst_Id
				AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.Fltr_Col_Attr_Id = a.Fltr_Col_Attr_Id
				AND QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.Fltr_Cond = a.Fltr_Cond;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate LST_ID, FLTR_COND Uniqueness Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- CC_VAL_ID - LOOKUP condition SURR KEYS
SET vSQL_Text ='UPDATE  QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''CC_VAL_ID IS NOT NULL FOR LOOKUP filter CONDITION''
                WHERE RUL_ID IN (SELECT RUL_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER WHERE FLTR_COND = ''LOOKUP'' 
                AND (CC_VAL <> '''' OR CASE WHEN TY = ''PASSTHRU'' THEN NULL ELSE CC_VAL_ID END IS NOT NULL));';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for CC_VAL_ID (lookup) Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- CC_VAL_ID all others SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''CC_VAL_ID IS NULL''
                WHERE (CC_VAL <> '''' OR CASE WHEN TY = ''PASSTHRU'' THEN 1 ELSE CC_VAL_ID END IS NULL ) 
				AND CC_VAL_ID IS NULL
				AND RUL_ID NOT IN (SELECT RUL_ID FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER WHERE FLTR_COND = ''LOOKUP'');';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for CC_VAL_ID (all others) Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- RUL_ID SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''RUL_ID IS NULL''
                WHERE RUL_ID IS NULL ;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for RUL_ID Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- SRC_ENT_ID SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''SRC_ENT_ID IS NULL''
                WHERE SRC_ENT_ID IS NULL ;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for SRC_ENT_ID Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- CC_ID SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''CC_ID IS NULL''
                WHERE CC_ID IS NULL ;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for CC_ID Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- TRG_TBL_ENT_ID SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''TRG_TBL_ENT_ID IS NULL''
                WHERE TRG_TBL_ENT_ID IS NULL ;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for TRG_TBL_ENT_ID Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- SRC_ATTR_ID SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''SRC_ATTR_ID IS NULL''
                WHERE SRC_ATTR_ID IS NULL ;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for SRC_ATTR_ID Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- TRG_ATTR_ID SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''TRG_ATTR_ID IS NULL''
                WHERE TRG_ATTR_ID IS NULL ;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for TRG_ATTR_ID Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- FLTR_COL_ATTR_ID SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''FLTR_COL_ATTR_ID IS NULL''
                WHERE CASE WHEN FLTR_COND = '''' THEN 1 ELSE FLTR_COL_ATTR_ID END IS NULL;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for FLTR_COL_ATTR_ID Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- FLTR_COND SURR KEYS
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										    ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''FLTR_COND IS NULL''
                WHERE CASE WHEN FLTR_COND = '''' THEN 1 ELSE FLTR_COND END IS NULL;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate SURR KEYS for FLTR_COND Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- FLTR_COND 
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''Invalid data in FLTR_COND''
										WHERE  FLTR_COND NOT IN (''*'', ''IN'', ''NOT IN'', ''NULL'', ''NOT NULL'', ''RANGE'', ''LOOKUP'', ''CONTAINS'', ''BEGIN WITH'', ''END WITH'', ''SETVAL'')
										AND FLTR_COL_NM <> '''';';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate data -- FLTR_COND Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- **************************************************** validate data -- FLTR_LOW_COND  & FLTR_HGH_COND
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										FROM (SELECT RUL_ID 
															FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
															WHERE FLTR_COND = ''RANGE''
															AND (
																		(FLTR_LOW_COND NOT IN (''>'',''>='') AND FLTR_HGH_COND IS NULL)
																		OR (FLTR_LOW_COND IS NULL AND FLTR_HGH_COND NOT IN (''<'', ''<=''))
																		OR (FLTR_LOW_COND IS NULL AND FLTR_HGH_COND IS NULL)
																		OR (FLTR_LOW_COND IS NULL AND FLTR_LOW_VAL IS NULL)
																		OR (FLTR_HGH_COND IS NULL AND FLTR_HGH_VAL IS NULL)
																		)) SRC
										SET PRCS_FLG = ''E'',
										           ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''Invalid data in FLTR_LOW_COND''
										WHERE  QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.RUL_ID = SRC.RUL_ID;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate data -- FLTR_LOW_COND & FLTR_HGH_COND Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- PRTY 
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										           ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''NULL in PRTY''
										WHERE  PRTY IS NULL;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate data -- PRTY Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- IS_DFLT 
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										           ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''Invalid data in IS_DFLT''
										WHERE  IS_DFLT NOT IN (''Y'', ''N'');';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate data -- IS_DFLT Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data -- TY 
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''E'',
										           ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''Invalid data in TY''
										WHERE  TY NOT IN (''REDUCING'', ''AUTO GL ADJUSTMENT'', ''PMX'', ''PASSTHRU'',''CP TYPE'',''RANKING'',''OVERRIDE'',''AUTO GL ADJUSTMENT'', ''REGULAR'');';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate data -- TY Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- **************************************************** validate data -- distinct values per Rule Id
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										FROM (SELECT RUL_ID
														FROM (SELECT RUL_ID, RUL_NM, RUL_DESC, TY, DQ_TY, SRC_SYS_CD, SRC_ENT_ID, CC_VAL_ID, PRTY
																		FROM QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER 
																		GROUP BY RUL_ID, RUL_NM, RUL_DESC, TY, DQ_TY, SRC_SYS_CD, SRC_ENT_ID, CC_VAL_ID, PRTY ) a
														GROUP BY 1
														HAVING COUNT(*) > 1) SRC
										SET PRCS_FLG = ''E'',
										           ERR_MSG = COALESCE(ERR_MSG,'' '') || '' - '' || ''Rule id attributes are not distinct''
										 WHERE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER.RUL_ID = SRC.RUL_ID;';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

IF vActivity_Ct <> 0 THEN
    SET vFailureMessage = 'One of the validation steps failed. Pls check RUL_UPLDR_INTER based on columns PRCS_FLG and ERR_MSG.';
END IF;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate data -- distinct values per Rule Id Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- **************************************************** update  PRCS_FLG = V for valid records
SET vSQL_Text ='UPDATE QSIT_APRA2_BRL_RRP_VW.RUL_UPLDR_INTER
										SET PRCS_FLG = ''V''
										WHERE  PRCS_FLG <> ''E'';';

-- Log SQL Text 
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = '/*Generated SQL is:*/' || cLF ||  vSQL_Text;
SET vDebugLvl = 5;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

EXECUTE IMMEDIATE vSQL_Text;
SET vActivity_Ct = ACTIVITY_COUNT;

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Step validate data -- TY Completed - ' || TRIM(vActivity_Ct) || ' Rows Affected';
SET vDebugLvl = 1;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);


-- **************************************************** exiting when error

IF vFailureMessage <> '' THEN
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Process - Failed';
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	SET oReturn_Code = 1;
	SET oReturn_Message = vFailureMessage;
    LEAVE MAIN;
END IF;

-- ****************************************************  exiting
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.BSNSMD_UPLDR_VALIDATE_PSTMERGE
-- =============================================
-- Description: This Stored Procedure will call the other procedures for uploader activity.
-- Change log
--      [2015 02 04]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
OUT oReturn_Code SMALLINT, /* 0: Successful; Non-Zero: Error */
OUT oReturn_Message VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vRecordCount INTEGER DEFAULT 100;
DECLARE vActivity_Ct INTEGER DEFAULT -99;
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(1000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'BSNSMD_UPLDR_VALIDATE_PSTMERGE';


-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

--UNCOMMENT IT WHEN GOOD DATA IS AVAILABLE
/*-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsg || ''' because no rows were returned from the sql.';
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;
*/

SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

-- **************************************************** validate data
SELECT COUNT(*) INTO vRecordCount FROM QSIT_APRA2_BRL_RRP_VW.BSNSMD_VALIDATION_POSTMERGE WHERE FailedRecord_Cnt <> 0;

IF vRecordCount <> 0 THEN
    SET oReturn_Code = 1;
    SET oReturn_Message = 'Failure: Pls query view QSIT_APRA2_BRL_RRP_VW.BSNSMD_VALIDATION for more details regarding which validation has failed';
    LEAVE MAIN;
END IF;

-- ****************************************************  exiting
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------

REPLACE PROCEDURE QSIT_APRA2_BRL_RRP_PGM.CreateSingleTableViews
-- =============================================
-- Description: This Stored Procedure will perform insert into BSNS metadata tables from uploader intermediate table.
-- Change log
--      [2015 02 04]: Initial version 
-- =============================================
-- Stored Procedure Parameters
(
IN Exec_In CHAR(1),
IN iSrcDatabaseName VARCHAR(255),
IN iTrgDatabaseName VARCHAR(255),
IN iTableNamePattern VARCHAR(255), -- Pass null  if want to execute for all tables
IN Is_Exact_Match CHAR(1), -- Y will look for exact match of the table name
IN iWhereClause VARCHAR(1000),  -- Pass null if no where clause. If there is any standard where condition is to be added i.e. pick only active records.
IN iViewNamePrefix VARCHAR(10), -- Pass null for no prefix
IN iViewNameSuffix VARCHAR(10), --- Pass null for no suffix
OUT oSQL_Text VARCHAR(10000), -- Output SQL
OUT oReturn_Code SMALLINT, -- 0: Successful; Non-Zero: Error 
OUT oReturn_Message VARCHAR(1000)
)
MAIN:
BEGIN
-- Declare variables
DECLARE cLF CHAR(2) DEFAULT '0A'XC;
DECLARE vTableNamePattern VARCHAR(255);
DECLARE vWhereClause VARCHAR(1000);
DECLARE vViewNamePrefix VARCHAR(10);
DECLARE vViewNameSuffix VARCHAR(10);
DECLARE vCounter INTEGER;
DECLARE vSQL_Text_ColList VARCHAR(10000) DEFAULT '';
DECLARE vSQL_Text VARCHAR(10000) DEFAULT '';
DECLARE vCntr INTEGER DEFAULT 100;
DECLARE vSQL_Code INTEGER;
DECLARE vSQL_State VARCHAR(6);
DECLARE vError_Text VARCHAR(256);
DECLARE oSubReturn_Code SMALLINT;
DECLARE oSubReturn_Message VARCHAR(1000);
DECLARE vDebugLvl SMALLINT DEFAULT 5; -- 5 = verbose
DECLARE vLogMsg VARCHAR(10000);
DECLARE vLogSPName VARCHAR(255) DEFAULT 'CreateSingleTableViews';

-- Error Handler
DECLARE EXIT HANDLER FOR SQLEXCEPTION
BEGIN
    SET vSQL_Code  = SQLCODE;
    SET vSQL_State = SQLSTATE;
    GET DIAGNOSTICS EXCEPTION 1 vError_Text = MESSAGE_TEXT;
    
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''SQLEXCEPTION'') - ' || 'SQL Exception, SQLCODE = '||TRIM(vSQL_Code)||', SQLSTATE = '||vSQL_State||', Error Msg = '||vError_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
    SET oReturn_Code = 2;
    SET oReturn_Message = vLogMsg;
END;

-- If attribute name is not returned by above query, then throw an user error and stop the process
DECLARE EXIT HANDLER FOR NOT FOUND
BEGIN	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = 'Failed (Exited at handler ''NOT FOUND'') - ' || 'Failure at next step of the step which says ''' || vLogMsg || ''' because no rows were returned from the sql.';
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	SET oReturn_Code = 1;
	SET oReturn_Message = vLogMsg;
END;

-- ***********************************  Program End
-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Started';
SET vDebugLvl = 0;
--CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);

IF iTableNamePattern IS NULL THEN 
	SET vTableNamePattern = '' ;
ELSE 
	IF Is_Exact_Match = 'Y' THEN 
		SET vTableNamePattern = iTableNamePattern ;
	ELSE 
		SET vTableNamePattern = '%' || iTableNamePattern || '%' ;
	END IF;
END IF;

IF iWhereClause IS NULL THEN 
	SET vWhereClause = '' ;
ELSE 
SET vWhereClause = iWhereClause ;
END IF;

IF iViewNamePrefix IS NULL THEN 
	SET vViewNamePrefix = '' ;
ELSE 
	SET vViewNamePrefix = iViewNamePrefix ;
END IF;

IF iViewNameSuffix IS NULL THEN 
	SET vViewNameSuffix = '' ;
ELSE 
	SET vViewNameSuffix = iViewNameSuffix ;
END IF;

L1:
FOR CSR1 AS 
	SELECT	
	TRIM (DataBaseName) AS SrcDatabaseName,
	TRIM (TABLENAME) AS SrcTableName
	FROM dbc.TablesV
	WHERE TRIM (DataBaseName) = iSrcDatabaseName
	AND TRIM(TABLENAME) LIKE vTableNamePattern 
	UNION 
	SELECT	
	TRIM (DataBaseName) AS SrcDatabaseName,
	TRIM (TABLENAME) AS SrcTableName
	FROM dbc.TablesV
	WHERE TRIM (DataBaseName) = iSrcDatabaseName
	AND TRIM(TABLENAME) = vTableNamePattern 
	ORDER BY 2
DO
	SET vCounter = 0;
	SET vSQL_Text_ColList = '';
	SET vSQL_Text = 'REPLACE VIEW ' || iTrgDatabaseName || '.' || vViewNamePrefix || CSR1.SrcTableName || vViewNameSuffix || cLF;
	SET vSQL_Text = vSQL_Text || '(' || cLF;
	L2:
	FOR	CSR2 AS 
		SELECT	
		 TRIM (DataBaseName) AS SrcDatabaseName,
		 TRIM (TABLENAME) AS SrcTableName,
		 TRIM (ColumnName) AS SrcColumnName
		 FROM dbc.ColumnsV
		 WHERE TRIM (DataBaseName) = CSR1.SrcDatabaseName
		 AND TRIM (TABLENAME) = CSR1.SrcTableName
		 ORDER BY ColumnId	
	DO
		IF vCounter = 0 THEN 
			SET vSQL_Text_ColList = vSQL_Text_ColList || ' ' || CSR2.SrcColumnName || cLF;
		ELSE
			SET vSQL_Text_ColList = vSQL_Text_ColList || ',' || CSR2.SrcColumnName || cLF;
		END IF;		
		SET vCounter = vCounter + 1;	
	END FOR L2;
	
	SET vSQL_Text = vSQL_Text || vSQL_Text_ColList;
	SET vSQL_Text = vSQL_Text || ')' || cLF;
	SET vSQL_Text = vSQL_Text || 'AS LOCKING ROW FOR ACCESS' || cLF;
	SET vSQL_Text = vSQL_Text || 'SELECT' || cLF;
	SET vSQL_Text = vSQL_Text || vSQL_Text_ColList; 
	SET vSQL_Text = vSQL_Text || 'FROM ' || CSR1.SrcDatabaseName || '.' || CSR1.SrcTableName || cLF;
	IF vWhereClause <> '' THEN
		SET vSQL_Text = vSQL_Text || vWhereClause || cLF;
	END IF;
	
	SET vSQL_Text = vSQL_Text || ';';
	
	-- Message Log portion
	SET vCntr = vCntr + 1; -- Increase step number by 1
	SET vLogMsg = vSQL_Text;
	SET oSQL_Text = vSQL_Text;
	SET vDebugLvl = 0;
	CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	
	IF Exec_In = 'Y' THEN
		EXECUTE IMMEDIATE vSQL_Text;
		-- Message Log portion
		SET vCntr = vCntr + 1; -- Increase step number by 1
		SET vLogMsg = 'View ' || CSR1.SrcDatabaseName || '.' || CSR1.SrcTableName || ' created.';
		SET vDebugLvl = 1;
		CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
	END IF;
	
END FOR L1;

-- ***********************************  Program End

-- Message Log portion
SET vCntr = vCntr + 1; -- Increase step number by 1
SET vLogMsg = 'Process - Completed';
SET vDebugLvl = 0;
--CALL QSIT_APRA2_BRL_RRP_PGM.BSNSMD_ERRORLOG (vLogSPName, vCntr, vLogMsg, vDebugLvl, oSubReturn_Code, oSubReturn_Message);
SET oReturn_Code = 0;
SET oReturn_Message = vLogMsg;

END MAIN;


--Database Comparison


REPLACE	PROCEDURE SIT07_BRL_RRP_PGM. DB_COMPARE (IN vi_frm_dbname VARCHAR (32), IN vi_to_dbname VARCHAR(32),  OUT ov_str VARCHAR(1000))
BEGIN	
 
DECLARE	v_Str_Tbl_List VARCHAR(1000);
DECLARE	v_Str_Tbl_Nm VARCHAR(1000);

-- LIST OF OBJECTS PRESENT  IN BOTH THE DATABASES
L1: 
FOR	CSR1 AS 
SEL	
TABLENAME 
FROM	DBC.COLUMNSV 
WHERE	DATABASENAME = vi_frm_dbname
AND	TABLENAME IN 
(
SEL	TABLENAME 
FROM	DBC.COLUMNSV 
WHERE	DATABASENAME =vi_to_dbname
)
GROUP BY TABLENAME ORDER BY 1

DO
----------


 SET v_Str_Tbl_List = CSR1.TABLENAME;
 
 -- CHECK IF OBJECT'S ATTRIBUTE OF 'FROM DATABASE' IS NOT PRESENT IN  'TO DATABASE'
L2: 
FOR	CSR2 AS 
  SELECT 
 TABLENAME,
 COLUMNNAME  ,
 COLUMNFORMAT  ,
 COLUMNTYPE  
FROM
DBC.COLUMNSV    
WHERE DATABASENAME = vi_frm_dbname
AND   TABLENAME = v_Str_Tbl_List
AND (COLUMNNAME, COLUMNFORMAT, COLUMNTYPE)
 NOT IN 
(
SELECT 
 COLUMNNAME  ,
 COLUMNFORMAT  ,
 COLUMNTYPE  
FROM
DBC.COLUMNSV    
WHERE DATABASENAME =vi_to_dbname
AND TABLENAME = v_Str_Tbl_List
)

DO
----------

  SET v_Str_Tbl_Nm =  CSR2.TABLENAME;
  
--LOAD THE MISMATCH INTO A WORK TABLE 
   
INSERT INTO SIT07_BRL_RRP_WK.DB_MSMTCH(vi_frm_dbname,CSR2.TABLENAME, CSR2.COLUMNNAME, CSR2.COLUMNFORMAT,CSR2.COLUMNTYPE,'Not Present in '||trim(vi_to_dbname));
DELETE FROM SIT07_BRL_RRP_WK.DB_MSMTCH WHERE TABLENAME IS NULL;
  

----------
END FOR L2;

----------
END FOR L1;

IF  v_Str_Tbl_Nm <> ''
THEN
SET ov_str=  'Mismatch found between '||vi_frm_dbname||' and '||vi_to_dbname;
ELSE
SET ov_str=  'No Mismatch found between '||vi_frm_dbname||' and '||vi_to_dbname;
END IF;

END;


