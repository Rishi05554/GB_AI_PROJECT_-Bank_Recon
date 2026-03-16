create or replace PACKAGE BODY PKG_BIP_LOADER
AS

/* ============================================================
   PRIVATE: GET_CONFIG
   Fetches a single config value from APEX_RECON_CONFIG.
   Raises -20001 if the key does not exist.
============================================================ */
FUNCTION GET_CONFIG (p_key IN VARCHAR2) RETURN VARCHAR2
IS
    v_value VARCHAR2(4000);
BEGIN
    SELECT config_value
    INTO   v_value
    FROM   apex_recon_config
    WHERE  config_key = p_key;
    RETURN v_value;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20001,
            'BIP Loader: Config key not found: ' || p_key);
END GET_CONFIG;


/* ============================================================
   PRIVATE: LOG_LOAD
   Inserts a new load log row (when p_load_id IS NULL) or
   updates an existing one.  Uses AUTONOMOUS_TRANSACTION so
   log entries persist even when the caller rolls back.
============================================================ */
PROCEDURE LOG_LOAD (
    p_load_id       IN OUT  NUMBER,
    p_report_code   IN      VARCHAR2,
    p_status        IN      VARCHAR2,
    p_rows_fetched  IN      NUMBER   DEFAULT NULL,
    p_rows_inserted IN      NUMBER   DEFAULT NULL,
    p_rows_skipped  IN      NUMBER   DEFAULT NULL,
    p_error_detail  IN      VARCHAR2 DEFAULT NULL,
    p_is_end        IN      VARCHAR2 DEFAULT 'N'
)
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    IF p_load_id IS NULL THEN
        INSERT INTO apex_bip_load_log (
            report_code, status, rows_fetched,
            rows_inserted, rows_skipped, error_detail
        ) VALUES (
            p_report_code, p_status, p_rows_fetched,
            p_rows_inserted, p_rows_skipped,
            SUBSTR(p_error_detail, 1, 4000)
        )
        RETURNING load_id INTO p_load_id;
    ELSE
        UPDATE apex_bip_load_log
        SET    status        = p_status,
               rows_fetched  = NVL(p_rows_fetched,  rows_fetched),
               rows_inserted = NVL(p_rows_inserted, rows_inserted),
               rows_skipped  = NVL(p_rows_skipped,  rows_skipped),
               error_detail  = NVL(SUBSTR(p_error_detail,1,4000), error_detail),
               load_end      = CASE p_is_end
                                   WHEN 'Y' THEN SYSTIMESTAMP
                                   ELSE NULL
                               END
        WHERE  load_id = p_load_id;
    END IF;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN NULL;  -- never let logging break the main flow
END LOG_LOAD;


/* ============================================================
   PRIVATE: CALL_BIP_REPORT
   Calls a BIP report via SOAP, base64-decodes the response,
   and returns the decoded HTML as a CLOB.
============================================================ */
FUNCTION CALL_BIP_REPORT (p_report_path IN VARCHAR2) RETURN CLOB
IS
    v_endpoint      VARCHAR2(500);
    v_action        VARCHAR2(500);
    v_username      VARCHAR2(200);
    v_password      VARCHAR2(200);
    v_envelope      CLOB;
    v_xml           XMLTYPE;
    v_response_clob CLOB;
    v_decoded_clob  CLOB;
    v_decoded_raw   RAW(32767);
    v_decoded_chunk VARCHAR2(32767);
    v_status_code   VARCHAR2(100);
    v_offset        INTEGER := 1;
    v_chunk_size    INTEGER := 4000;
    v_clob_length   INTEGER;
    v_pos_start     INTEGER;
    v_pos_end       INTEGER;
BEGIN
    v_endpoint := GET_CONFIG('BIP_ENDPOINT');
    v_action   := GET_CONFIG('BIP_ACTION');
    v_username := GET_CONFIG('BIP_USERNAME');
    v_password := GET_CONFIG('BIP_PASSWORD');

    DBMS_LOB.CREATETEMPORARY(v_envelope, TRUE);
    DBMS_LOB.APPEND(v_envelope,
        '<soapenv:Envelope '
        || 'xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        || 'xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">'
        || '<soapenv:Header/><soapenv:Body><pub:runReport><pub:reportRequest>'
        || '<pub:attributeFormat>html</pub:attributeFormat>'
        || '<pub:reportAbsolutePath>' || p_report_path || '</pub:reportAbsolutePath>'
        || '</pub:reportRequest>'
        || '<pub:userID>'   || v_username || '</pub:userID>'
        || '<pub:password>' || v_password || '</pub:password>'
        || '</pub:runReport></soapenv:Body></soapenv:Envelope>'
    );

    v_xml         := APEX_WEB_SERVICE.MAKE_REQUEST(
                         p_url      => v_endpoint,
                         p_action   => v_action,
                         p_envelope => v_envelope);
    v_status_code := APEX_WEB_SERVICE.G_STATUS_CODE;

    IF v_status_code != '200' THEN
        RAISE_APPLICATION_ERROR(-20002,
            'BIP call failed. HTTP status: ' || v_status_code);
    END IF;

    v_response_clob := v_xml.getClobVal();
    v_pos_start     := INSTR(v_response_clob, '<reportBytes>') + LENGTH('<reportBytes>');
    v_pos_end       := INSTR(v_response_clob, '</reportBytes>');
    v_response_clob := SUBSTR(v_response_clob, v_pos_start, v_pos_end - v_pos_start);

    DBMS_LOB.CREATETEMPORARY(v_decoded_clob, TRUE);
    v_clob_length := LENGTH(v_response_clob);

    WHILE v_offset <= v_clob_length LOOP
        v_decoded_chunk := SUBSTR(v_response_clob, v_offset, v_chunk_size);
        BEGIN
            v_decoded_raw   := UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(v_decoded_chunk));
            v_decoded_chunk := UTL_RAW.CAST_TO_VARCHAR2(v_decoded_raw);
        EXCEPTION
            WHEN OTHERS THEN
                RAISE_APPLICATION_ERROR(-20003,
                    'Base64 decode failed at offset ' || v_offset || ': ' || SQLERRM);
        END;
        DBMS_LOB.WRITEAPPEND(v_decoded_clob, LENGTH(v_decoded_chunk), v_decoded_chunk);
        v_offset := v_offset + v_chunk_size;
    END LOOP;

    DBMS_LOB.FREETEMPORARY(v_envelope);
    RETURN v_decoded_clob;
EXCEPTION
    WHEN OTHERS THEN
        IF v_envelope IS NOT NULL THEN
            DBMS_LOB.FREETEMPORARY(v_envelope);
        END IF;
        RAISE;
END CALL_BIP_REPORT;


/* ============================================================
   PRIVATE: EXTRACT_TABLE_HTML
   Extracts the first <table>...</table> block from the BIP
   HTML output and performs HTML entity cleanup so the result
   can be parsed as XMLTYPE.
============================================================ */
FUNCTION EXTRACT_TABLE_HTML (p_html IN CLOB) RETURN CLOB
IS
    v_table_start  INTEGER;
    v_table_end    INTEGER;
    v_result       CLOB;
BEGIN
    v_table_start := INSTR(p_html, '<table', 1, 1);
    v_table_end   := INSTR(p_html, '</table>', v_table_start) + LENGTH('</table>');

    SELECT SUBSTR(p_html, v_table_start, v_table_end - v_table_start)
    INTO   v_result
    FROM   DUAL;

    v_result := REPLACE(v_result, '&amp;',  '&');
    v_result := REPLACE(v_result, '&lt;',   '<');
    v_result := REPLACE(v_result, '&gt;',   '>');
    v_result := REPLACE(v_result, '&quot;', '"');
    v_result := REPLACE(v_result, '&nbsp;', '');

    RETURN v_result;
END EXTRACT_TABLE_HTML;


/* ============================================================
   PUBLIC: LOAD_STMT_HEADERS
   Target : XXEMR_BANK_STATEMENT_HEADERS
   Version: v1 – no changes; all 33 columns verified correct.

   BIP column order:
     td[1]  STATEMENT_HEADER_ID      td[18] INBOUND_FILE_ID
     td[2]  BALANCE_CHECK            td[19] LOADED_DATE
     td[3]  STATEMENT_NUMBER         td[20] LAST_UPDATE_DATE
     td[4]  BANK_ACCOUNT_ID          td[21] LAST_UPDATED_BY
     td[5]  BANK_ACCOUNT_NAME        td[22] CREATION_DATE
     td[6]  BANK_ACCOUNT_NUM         td[23] CREATED_BY
     td[7]  BANK_DESCRIPTION         td[24] OPBD_AMOUNT
     td[8]  SHORT_BANK_ACCOUNT_NAME  td[25] OPBD_DATE
     td[9]  BANK_NAME                td[26] CLBD_AMOUNT
     td[10] STATEMENT_DATE           td[27] CLBD_DATE
     td[11] STMT_FROM_DATE           td[28] MONTH_START_DATE
     td[12] STMT_TO_DATE             td[29] REPORT_RUN_DATE (ignored)
     td[13] RECON_STATUS_CODE        td[30] BANK_CURRENCY_CODE
     td[14] AUTOREC_PROCESS_CODE     td[31] ESCROW_ACCOUNT
     td[15] AUTOREC_PROCESS_ID       td[32] BANK_ID
     td[16] STATEMENT_ENTRY_TYPE     td[33] IBAN_NUMBER
     td[17] STATEMENT_TYPE
============================================================ */
PROCEDURE LOAD_STMT_HEADERS
IS
    v_html          CLOB;
    v_table_html    CLOB;
    v_load_id       NUMBER  := NULL;
    v_rows_fetched  NUMBER  := 0;
    v_rows_inserted NUMBER  := 0;
BEGIN
    LOG_LOAD(v_load_id, 'STMT_HEADERS', 'RUNNING');

    v_html       := CALL_BIP_REPORT(GET_CONFIG('REPORT_PATH_STMT_HEADERS'));
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    INSERT INTO XXEMR_BANK_STATEMENT_HEADERS (
        statement_header_id,       -- td[1]
        balance_check,             -- td[2]
        statement_number,          -- td[3]
        bank_account_id,           -- td[4]
        bank_account_name,         -- td[5]
        bank_account_num,          -- td[6]
        bank_description,          -- td[7]
        short_bank_account_name,   -- td[8]
        bank_name,                 -- td[9]
        statement_date,            -- td[10]
        stmt_from_date,            -- td[11]
        stmt_to_date,              -- td[12]
        recon_status_code,         -- td[13]
        autorec_process_code,      -- td[14]
        autorec_process_id,        -- td[15]
        statement_entry_type,      -- td[16]
        statement_type,            -- td[17]
        inbound_file_id,           -- td[18]
        loaded_date,               -- td[19]
        last_update_date,          -- td[20]
        last_updated_by,           -- td[21]
        creation_date,             -- td[22]
        created_by,                -- td[23]
        opbd_amount,               -- td[24]
        opbd_date,                 -- td[25]
        clbd_amount,               -- td[26]
        clbd_date,                 -- td[27]
        month_start_date,          -- td[28]
        -- td[29] REPORT_RUN_DATE  (not loaded into APEX)
        bank_currency_code,        -- td[30]
        escrow_account,            -- td[31]
        bank_id,                   -- td[32]
        iban_number                -- td[33]
    )
    SELECT
        TO_NUMBER(X.c1  DEFAULT NULL ON CONVERSION ERROR),
        ROUND(TO_NUMBER(X.c2 DEFAULT NULL ON CONVERSION ERROR), 18),
        X.c3,
        TO_NUMBER(X.c4  DEFAULT NULL ON CONVERSION ERROR),
        X.c5,
        CASE WHEN REGEXP_LIKE(TRIM(X.c6), '^[0-9]+$')
             THEN TO_NUMBER(X.c6)
        END,
        X.c7,
        X.c8,
        X.c9,
        TO_DATE(SUBSTR(X.c10,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_DATE(SUBSTR(X.c11,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_DATE(SUBSTR(X.c12,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        X.c13,
        X.c14,
        TO_NUMBER(X.c15 DEFAULT NULL ON CONVERSION ERROR),
        X.c16,
        X.c17,
        X.c18,
        CASE WHEN X.c19 IS NOT NULL AND X.c19 != ''
             THEN TO_TIMESTAMP(SUBSTR(X.c19,1,19), 'YYYY-MM-DD"T"HH24:MI:SS')
        END,
        CASE WHEN X.c20 IS NOT NULL AND X.c20 != ''
             THEN TO_TIMESTAMP(SUBSTR(X.c20,1,19), 'YYYY-MM-DD"T"HH24:MI:SS')
        END,
        X.c21,
        CASE WHEN X.c22 IS NOT NULL AND X.c22 != ''
             THEN TO_TIMESTAMP(SUBSTR(X.c22,1,19), 'YYYY-MM-DD"T"HH24:MI:SS')
        END,
        X.c23,
        TO_NUMBER(X.c24 DEFAULT NULL ON CONVERSION ERROR),
        TO_DATE(SUBSTR(X.c25,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_NUMBER(X.c26 DEFAULT NULL ON CONVERSION ERROR),
        TO_DATE(SUBSTR(X.c27,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_DATE(SUBSTR(X.c28,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        X.c30,
        X.c31,
        TO_NUMBER(X.c32 DEFAULT NULL ON CONVERSION ERROR),
        X.c33
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',   -- STATEMENT_HEADER_ID
            c2  VARCHAR2(100)  PATH 'string(td[2])',   -- BALANCE_CHECK
            c3  VARCHAR2(200)  PATH 'string(td[3])',   -- STATEMENT_NUMBER
            c4  VARCHAR2(100)  PATH 'string(td[4])',   -- BANK_ACCOUNT_ID
            c5  VARCHAR2(500)  PATH 'string(td[5])',   -- BANK_ACCOUNT_NAME
            c6  VARCHAR2(200)  PATH 'string(td[6])',   -- BANK_ACCOUNT_NUM
            c7  VARCHAR2(500)  PATH 'string(td[7])',   -- BANK_DESCRIPTION
            c8  VARCHAR2(200)  PATH 'string(td[8])',   -- SHORT_BANK_ACCOUNT_NAME
            c9  VARCHAR2(500)  PATH 'string(td[9])',   -- BANK_NAME
            c10 VARCHAR2(100)  PATH 'string(td[10])',  -- STATEMENT_DATE
            c11 VARCHAR2(100)  PATH 'string(td[11])',  -- STMT_FROM_DATE
            c12 VARCHAR2(100)  PATH 'string(td[12])',  -- STMT_TO_DATE
            c13 VARCHAR2(100)  PATH 'string(td[13])',  -- RECON_STATUS_CODE
            c14 VARCHAR2(100)  PATH 'string(td[14])',  -- AUTOREC_PROCESS_CODE
            c15 VARCHAR2(100)  PATH 'string(td[15])',  -- AUTOREC_PROCESS_ID
            c16 VARCHAR2(100)  PATH 'string(td[16])',  -- STATEMENT_ENTRY_TYPE
            c17 VARCHAR2(100)  PATH 'string(td[17])',  -- STATEMENT_TYPE
            c18 VARCHAR2(100)  PATH 'string(td[18])',  -- INBOUND_FILE_ID
            c19 VARCHAR2(100)  PATH 'string(td[19])',  -- LOADED_DATE
            c20 VARCHAR2(100)  PATH 'string(td[20])',  -- LAST_UPDATE_DATE
            c21 VARCHAR2(200)  PATH 'string(td[21])',  -- LAST_UPDATED_BY
            c22 VARCHAR2(100)  PATH 'string(td[22])',  -- CREATION_DATE
            c23 VARCHAR2(200)  PATH 'string(td[23])',  -- CREATED_BY
            c24 VARCHAR2(100)  PATH 'string(td[24])',  -- OPBD_AMOUNT
            c25 VARCHAR2(100)  PATH 'string(td[25])',  -- OPBD_DATE
            c26 VARCHAR2(100)  PATH 'string(td[26])',  -- CLBD_AMOUNT
            c27 VARCHAR2(100)  PATH 'string(td[27])',  -- CLBD_DATE
            c28 VARCHAR2(100)  PATH 'string(td[28])',  -- MONTH_START_DATE
            c29 VARCHAR2(100)  PATH 'string(td[29])',  -- REPORT_RUN_DATE (read but not inserted)
            c30 VARCHAR2(50)   PATH 'string(td[30])',  -- BANK_CURRENCY_CODE
            c31 VARCHAR2(50)   PATH 'string(td[31])',  -- ESCROW_ACCOUNT
            c32 VARCHAR2(100)  PATH 'string(td[32])',  -- BANK_ID
            c33 VARCHAR2(200)  PATH 'string(td[33])'   -- IBAN_NUMBER
    ) X
    WHERE REGEXP_LIKE(TRIM(X.c1), '^[0-9]+$')
      AND NOT EXISTS (
            SELECT 1
            FROM   XXEMR_BANK_STATEMENT_HEADERS H
            WHERE  H.statement_header_id =
                   TO_NUMBER(X.c1 DEFAULT NULL ON CONVERSION ERROR)
      );

    v_rows_inserted := SQL%ROWCOUNT;

    SELECT COUNT(*) INTO v_rows_fetched
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS c1 VARCHAR2(100) PATH 'string(td[1])'
    )
    WHERE REGEXP_LIKE(TRIM(c1), '^[0-9]+$');

    COMMIT;
    LOG_LOAD(v_load_id, 'STMT_HEADERS', 'COMPLETED',
        v_rows_fetched, v_rows_inserted,
        v_rows_fetched - v_rows_inserted, NULL, 'Y');
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        LOG_LOAD(v_load_id, 'STMT_HEADERS', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END LOAD_STMT_HEADERS;


/* ============================================================
   PUBLIC: LOAD_STMT_LINES
   Target : XXEMR_BANK_STATEMENT_LINES
   Version: v3 – all 28 BIP columns mapped; 5 new cols added;
            10 wrong positions corrected from v1.

   BIP column order:
     td[1]  LOADED_DATE              td[15] EXCHANGE_RATE_DATE
     td[2]  STATEMENT_HEADER_ID      td[16] EXCHANGE_RATE_TYPE
     td[3]  STATEMENT_LINE_ID        td[17] EXTERNAL_TRANSACTION_ID (NEW)
     td[4]  LINE_NUMBER              td[18] DESCRIPTION             (NEW)
     td[5]  TRX_TYPE                 td[19] RECON_REFERENCE -> reference_num
     td[6]  FLOW_INDICATOR           td[20] CHECK_NUMBER
     td[7]  RECON_STATUS             td[21] EXCEPTION_FLAG
     td[8]  AMOUNT                   td[22] LAST_UPDATE_DATE
     td[9]  REVERSAL_IND_FLAG        td[23] LAST_UPDATED_BY
     td[10] BOOKING_DATE             td[24] CREATION_DATE
     td[11] VALUE_DATE               td[25] CREATED_BY
     td[12] TRX_AMOUNT (NEW)         td[26] BANK_ID
     td[13] TRX_CURR_CODE            td[27] TRX_CODE               (NEW)
     td[14] EXCHANGE_RATE            td[28] TRX_DESCRIPTION        (NEW)

   Post-INSERT: BANK_ACCOUNT_ID back-filled from
   XXEMR_BANK_STATEMENT_HEADERS (not present in BIP Lines report).
============================================================ */
PROCEDURE LOAD_STMT_LINES
IS
    v_html          CLOB;
    v_table_html    CLOB;
    v_load_id       NUMBER  := NULL;
    v_rows_fetched  NUMBER  := 0;
    v_rows_inserted NUMBER  := 0;
BEGIN
    LOG_LOAD(v_load_id, 'STMT_LINES', 'RUNNING');

    v_html       := CALL_BIP_REPORT(GET_CONFIG('REPORT_PATH_STMT_LINES'));
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    -- Step 1: stamp bip_last_seen_date on rows already in APEX
    UPDATE XXEMR_BANK_STATEMENT_LINES L
    SET    L.bip_last_seen_date = TRUNC(SYSDATE)
    WHERE  L.statement_line_id IN (
        SELECT TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
        FROM   XMLTABLE(
                   '/table/tr[position() > 1]'
                   PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
                   COLUMNS c3 VARCHAR2(100) PATH 'string(td[3])'
               ) X
        WHERE  REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
    );

    -- Step 2: insert brand-new rows only
    INSERT INTO XXEMR_BANK_STATEMENT_LINES (
        statement_header_id,        -- td[2]
        statement_line_id,          -- td[3]
        bank_account_id,            -- NULL; back-filled in Step 3
        line_number,                -- td[4]
        trx_type,                   -- td[5]
        flow_indicator,             -- td[6]
        recon_status,               -- td[7]
        amount,                     -- td[8]
        reversal_ind_flag,          -- td[9]
        statement_date,             -- derived from td[10] booking_date
        booking_date,               -- td[10]
        value_date,                 -- td[11]
        trx_amount,                 -- td[12]  NEW
        currency_code,              -- td[13]  TRX_CURR_CODE
        exchange_rate,              -- td[14]
        exchange_rate_date,         -- td[15]
        exchange_rate_type,         -- td[16]
        external_txn_id,            -- td[17]  NEW
        description,                -- td[18]  NEW
        reference_num,              -- td[19]  RECON_REFERENCE
        check_number,               -- td[20]
        exception_flag,             -- td[21]
        last_update_date,           -- td[22]
        last_updated_by,            -- td[23]
        creation_date,              -- td[24]
        created_by,                 -- td[25]
        bank_id,                    -- td[26]
        trx_code,                   -- td[27]  NEW
        trx_description,            -- td[28]  NEW
        process_date,               -- derived from td[10]
        period_name,                -- derived from td[10]
        bip_last_seen_date,
        external_flag,
        pw_keyword_check_done,
        full_keyword_check_done,
        pw_check_done,
        month_end_check_done,
        dashboard_flag,
        approval_status,
        match_flag
    )
    SELECT
        TO_NUMBER(X.c2  DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c3  DEFAULT NULL ON CONVERSION ERROR),
        NULL,                                                        -- bank_account_id
        TO_NUMBER(X.c4  DEFAULT NULL ON CONVERSION ERROR),
        X.c5,
        X.c6,
        X.c7,
        TO_NUMBER(X.c8  DEFAULT NULL ON CONVERSION ERROR),
        X.c9,
        TO_DATE(SUBSTR(X.c10,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- statement_date
        TO_DATE(SUBSTR(X.c10,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- booking_date
        TO_DATE(SUBSTR(X.c11,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- value_date
        TO_NUMBER(X.c12 DEFAULT NULL ON CONVERSION ERROR),                            -- trx_amount     NEW
        X.c13,                                                                        -- currency_code
        TO_NUMBER(X.c14 DEFAULT NULL ON CONVERSION ERROR),                            -- exchange_rate
        TO_DATE(SUBSTR(X.c15,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- exchange_rate_date
        X.c16,                                                                        -- exchange_rate_type
        X.c17,                                                                        -- external_txn_id NEW
        X.c18,                                                                        -- description     NEW
        X.c19,                                                                        -- reference_num
        X.c20,                                                                        -- check_number
        X.c21,                                                                        -- exception_flag
        CASE WHEN X.c22 IS NOT NULL AND X.c22 != ''
             THEN TO_TIMESTAMP(SUBSTR(X.c22,1,19), 'YYYY-MM-DD"T"HH24:MI:SS')
        END,                                                                          -- last_update_date
        X.c23,                                                                        -- last_updated_by
        CASE WHEN X.c24 IS NOT NULL AND X.c24 != ''
             THEN TO_TIMESTAMP(SUBSTR(X.c24,1,19), 'YYYY-MM-DD"T"HH24:MI:SS')
        END,                                                                          -- creation_date
        X.c25,                                                                        -- created_by
        TO_NUMBER(X.c26 DEFAULT NULL ON CONVERSION ERROR),                            -- bank_id
        X.c27,                                                                        -- trx_code        NEW
        X.c28,                                                                        -- trx_description NEW
        TO_DATE(SUBSTR(X.c10,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- process_date
        TO_CHAR(
            TO_DATE(SUBSTR(X.c10,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
            'Mon-YY'
        ),                                                                            -- period_name
        TRUNC(SYSDATE),                                                               -- bip_last_seen_date
        'N', 'N', 'N', 'N', 'N', 'N', NULL, 'N'
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS
            c2  VARCHAR2(100)  PATH 'string(td[2])',   -- STATEMENT_HEADER_ID
            c3  VARCHAR2(100)  PATH 'string(td[3])',   -- STATEMENT_LINE_ID
            c4  VARCHAR2(100)  PATH 'string(td[4])',   -- LINE_NUMBER
            c5  VARCHAR2(200)  PATH 'string(td[5])',   -- TRX_TYPE
            c6  VARCHAR2(10)   PATH 'string(td[6])',   -- FLOW_INDICATOR
            c7  VARCHAR2(100)  PATH 'string(td[7])',   -- RECON_STATUS
            c8  VARCHAR2(200)  PATH 'string(td[8])',   -- AMOUNT
            c9  VARCHAR2(10)   PATH 'string(td[9])',   -- REVERSAL_IND_FLAG
            c10 VARCHAR2(100)  PATH 'string(td[10])',  -- BOOKING_DATE
            c11 VARCHAR2(100)  PATH 'string(td[11])',  -- VALUE_DATE
            c12 VARCHAR2(200)  PATH 'string(td[12])',  -- TRX_AMOUNT          NEW
            c13 VARCHAR2(50)   PATH 'string(td[13])',  -- TRX_CURR_CODE
            c14 VARCHAR2(100)  PATH 'string(td[14])',  -- EXCHANGE_RATE
            c15 VARCHAR2(100)  PATH 'string(td[15])',  -- EXCHANGE_RATE_DATE
            c16 VARCHAR2(100)  PATH 'string(td[16])',  -- EXCHANGE_RATE_TYPE
            c17 VARCHAR2(100)  PATH 'string(td[17])',  -- EXTERNAL_TRANSACTION_ID  NEW
            c18 VARCHAR2(500)  PATH 'string(td[18])',  -- DESCRIPTION              NEW
            c19 VARCHAR2(200)  PATH 'string(td[19])',  -- RECON_REFERENCE
            c20 VARCHAR2(200)  PATH 'string(td[20])',  -- CHECK_NUMBER
            c21 VARCHAR2(10)   PATH 'string(td[21])',  -- EXCEPTION_FLAG
            c22 VARCHAR2(100)  PATH 'string(td[22])',  -- LAST_UPDATE_DATE
            c23 VARCHAR2(200)  PATH 'string(td[23])',  -- LAST_UPDATED_BY
            c24 VARCHAR2(100)  PATH 'string(td[24])',  -- CREATION_DATE
            c25 VARCHAR2(200)  PATH 'string(td[25])',  -- CREATED_BY
            c26 VARCHAR2(100)  PATH 'string(td[26])',  -- BANK_ID
            c27 VARCHAR2(100)  PATH 'string(td[27])',  -- TRX_CODE                 NEW
            c28 VARCHAR2(500)  PATH 'string(td[28])'   -- TRX_DESCRIPTION          NEW
    ) X
    WHERE REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
      AND NOT EXISTS (
            SELECT 1
            FROM   XXEMR_BANK_STATEMENT_LINES L
            WHERE  L.statement_line_id =
                   TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
      );

    v_rows_inserted := SQL%ROWCOUNT;

    -- Step 3: back-fill BANK_ACCOUNT_ID for rows inserted in this run
    UPDATE XXEMR_BANK_STATEMENT_LINES L
    SET    L.bank_account_id = (
               SELECT H.bank_account_id
               FROM   XXEMR_BANK_STATEMENT_HEADERS H
               WHERE  H.statement_header_id = L.statement_header_id
           )
    WHERE  L.bank_account_id IS NULL
      AND  L.bip_last_seen_date = TRUNC(SYSDATE);

    SELECT COUNT(*) INTO v_rows_fetched
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS c3 VARCHAR2(100) PATH 'string(td[3])'
    )
    WHERE REGEXP_LIKE(TRIM(c3), '^[0-9]+$');

    COMMIT;
    LOG_LOAD(v_load_id, 'STMT_LINES', 'COMPLETED',
        v_rows_fetched, v_rows_inserted,
        v_rows_fetched - v_rows_inserted, NULL, 'Y');
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        LOG_LOAD(v_load_id, 'STMT_LINES', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END LOAD_STMT_LINES;


/* ============================================================
   PUBLIC: LOAD_EXT_TRANSACTIONS
   Target : XXEMR_EXTERNAL_TRANSACTIONS
   Version: v3 – all 31 BIP columns mapped; 15 new cols added;
            4 wrong positions corrected from v1.

   BIP column order:
     td[1]  EXTERNAL_TRANSACTION_ID  td[17] TRANSACTION_TYPE
     td[2]  RECON_HISTORY_ID  (NEW)  td[18] STATUS -> recon_status
     td[3]  BANK_ACCOUNT_ID          td[19] DESCRIPTION        (NEW)
     td[4]  BANK_ACCOUNT_NAME (NEW)  td[20] SOURCE             (NEW)
     td[5]  BANK_ACCOUNT_NUM  (NEW)  td[21] CLEARED_DATE       (NEW)
     td[6]  BANK_CURRENCY            td[22] LOADED_DATE        (NEW)
     td[7]  BANK_DESCRIPTION (skip)  td[23] LAST_UPDATE_DATE   (NEW)
     td[8]  IBAN              (NEW)  td[24] LAST_UPDATED_BY    (NEW)
     td[9]  SHORT_BANK_ACCT   (skip) td[25] CREATION_DATE      (NEW)
     td[10] BUSINESS_UNIT_ID  (skip) td[26] CREATED_BY         (NEW)
     td[11] LEGAL_ENTITY_ID   (skip) td[27] REFERENCE_TEXT -> reference_num
     td[12] TRANSACTION_ID           td[28] LOB                (NEW)
     td[13] STATEMENT_LINE_ID        td[29] (gap/empty)
     td[14] TRANSACTION_DATE         td[30] TRANSACTION_TYPE_CODE
     td[15] VALUE_DATE               td[31] BANK_ID            (NEW)
     td[16] AMOUNT

   FUSION_EXT_TXN_ID mirrors EXT_TXN_ID (Option A – no migration needed).
============================================================ */
PROCEDURE LOAD_EXT_TRANSACTIONS
IS
    v_html          CLOB;
    v_table_html    CLOB;
    v_load_id       NUMBER  := NULL;
    v_rows_fetched  NUMBER  := 0;
    v_rows_inserted NUMBER  := 0;
BEGIN
    LOG_LOAD(v_load_id, 'EXT_TXN', 'RUNNING');

    v_html       := CALL_BIP_REPORT(GET_CONFIG('REPORT_PATH_EXT_TXN'));
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    INSERT INTO XXEMR_EXTERNAL_TRANSACTIONS (
        ext_txn_id,              -- td[1]  EXTERNAL_TRANSACTION_ID
        fusion_ext_txn_id,       -- td[1]  mirror of ext_txn_id      NEW
        recon_history_id,        -- td[2]  RECON_HISTORY_ID           NEW
        bank_account_id,         -- td[3]  BANK_ACCOUNT_ID
        bank_account_name,       -- td[4]  BANK_ACCOUNT_NAME          NEW
        bank_account_num,        -- td[5]  BANK_ACCOUNT_NUM           NEW
        currency_code,           -- td[6]  BANK_CURRENCY
        iban,                    -- td[8]  IBAN                       NEW
        transaction_id,          -- td[12] TRANSACTION_ID
        statement_line_id,       -- td[13] STATEMENT_LINE_ID
        transaction_date,        -- td[14] TRANSACTION_DATE
        value_date,              -- td[15] VALUE_DATE
        amount,                  -- td[16] AMOUNT
        transaction_type,        -- td[17] TRANSACTION_TYPE
        recon_status,            -- td[18] STATUS
        description,             -- td[19] DESCRIPTION                NEW
        source,                  -- td[20] SOURCE                     NEW
        cleared_date,            -- td[21] CLEARED_DATE               NEW
        loaded_date,             -- td[22] LOADED_DATE                NEW
        reference_num,           -- td[27] REFERENCE_TEXT
        transaction_type_code,   -- td[30] TRANSACTION_TYPE_CODE
        bank_id                  -- td[31] BANK_ID                    NEW
    )
    SELECT
        X.c1,                                                              -- ext_txn_id
        X.c1,                                                              -- fusion_ext_txn_id (mirror)
        TO_NUMBER(X.c2  DEFAULT NULL ON CONVERSION ERROR),                 -- recon_history_id
        X.c3,                                                              -- bank_account_id
        X.c4,                                                              -- bank_account_name
        X.c5,                                                              -- bank_account_num
        X.c6,                                                              -- currency_code
        X.c8,                                                              -- iban
        TO_NUMBER(X.c12 DEFAULT NULL ON CONVERSION ERROR),                 -- transaction_id
        TO_NUMBER(X.c13 DEFAULT NULL ON CONVERSION ERROR),                 -- statement_line_id
        TO_DATE(SUBSTR(X.c14,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_DATE(SUBSTR(X.c15,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_NUMBER(X.c16 DEFAULT NULL ON CONVERSION ERROR),                 -- amount
        X.c17,                                                             -- transaction_type
        X.c18,                                                             -- recon_status
        X.c19,                                                             -- description
        X.c20,                                                             -- source
        TO_DATE(SUBSTR(X.c21,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- cleared_date
        TO_DATE(SUBSTR(X.c22,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- loaded_date
        X.c27,                                                             -- reference_num
        X.c30,                                                             -- transaction_type_code
        TO_NUMBER(X.c31 DEFAULT NULL ON CONVERSION ERROR)                  -- bank_id
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',   -- EXTERNAL_TRANSACTION_ID
            c2  VARCHAR2(100)  PATH 'string(td[2])',   -- RECON_HISTORY_ID    NEW
            c3  VARCHAR2(50)   PATH 'string(td[3])',   -- BANK_ACCOUNT_ID
            c4  VARCHAR2(360)  PATH 'string(td[4])',   -- BANK_ACCOUNT_NAME   NEW
            c5  VARCHAR2(100)  PATH 'string(td[5])',   -- BANK_ACCOUNT_NUM    NEW
            c6  VARCHAR2(50)   PATH 'string(td[6])',   -- BANK_CURRENCY
            c8  VARCHAR2(200)  PATH 'string(td[8])',   -- IBAN                NEW
            c12 VARCHAR2(100)  PATH 'string(td[12])',  -- TRANSACTION_ID
            c13 VARCHAR2(100)  PATH 'string(td[13])',  -- STATEMENT_LINE_ID
            c14 VARCHAR2(100)  PATH 'string(td[14])',  -- TRANSACTION_DATE
            c15 VARCHAR2(100)  PATH 'string(td[15])',  -- VALUE_DATE
            c16 VARCHAR2(200)  PATH 'string(td[16])',  -- AMOUNT
            c17 VARCHAR2(200)  PATH 'string(td[17])',  -- TRANSACTION_TYPE
            c18 VARCHAR2(100)  PATH 'string(td[18])',  -- STATUS -> recon_status
            c19 VARCHAR2(500)  PATH 'string(td[19])',  -- DESCRIPTION         NEW
            c20 VARCHAR2(100)  PATH 'string(td[20])',  -- SOURCE              NEW
            c21 VARCHAR2(100)  PATH 'string(td[21])',  -- CLEARED_DATE        NEW
            c22 VARCHAR2(100)  PATH 'string(td[22])',  -- LOADED_DATE         NEW
            c23 VARCHAR2(100)  PATH 'string(td[23])',  -- LAST_UPDATE_DATE    NEW
            c25 VARCHAR2(100)  PATH 'string(td[25])',  -- CREATION_DATE       NEW
            c26 VARCHAR2(200)  PATH 'string(td[26])',  -- CREATED_BY          NEW
            c27 VARCHAR2(500)  PATH 'string(td[27])',  -- REFERENCE_TEXT
            c28 VARCHAR2(200)  PATH 'string(td[28])',  -- LOB                 NEW
            c30 VARCHAR2(200)  PATH 'string(td[30])',  -- TRANSACTION_TYPE_CODE
            c31 VARCHAR2(100)  PATH 'string(td[31])'   -- BANK_ID             NEW
    ) X
    WHERE  TRIM(X.c1) IS NOT NULL
      AND  NOT EXISTS (
               SELECT 1
               FROM   XXEMR_EXTERNAL_TRANSACTIONS T
               WHERE  T.ext_txn_id = X.c1
           );

    v_rows_inserted := SQL%ROWCOUNT;

    SELECT COUNT(*) INTO v_rows_fetched
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS c1 VARCHAR2(100) PATH 'string(td[1])'
    )
    WHERE TRIM(c1) IS NOT NULL;

    COMMIT;
    LOG_LOAD(v_load_id, 'EXT_TXN', 'COMPLETED',
        v_rows_fetched, v_rows_inserted,
        v_rows_fetched - v_rows_inserted, NULL, 'Y');
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        LOG_LOAD(v_load_id, 'EXT_TXN', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END LOAD_EXT_TRANSACTIONS;


/* ============================================================
   PUBLIC: LOAD_AR_RECEIPTS
   Target : XXEMR_AR_CASH_RECEIPTS
   Version: v3 – all 32 BIP columns mapped; 11 new cols added;
            gl_date, iban and audit col positions fixed from v1.

   BIP column order:
     td[1]  LOADED_DATE              td[17] REMITTANCE_BANK_ACCOUNT_ID
     td[2]  RECEIPT_NUMBER           td[18] RECEIPT_METHOD_ID
     td[3]  CASH_RECEIPT_ID          td[19] ORG_ID
     td[4]  COLLECTOR_ID             td[20] LEGAL_ENTITY_ID
     td[5]  RECEIPT_BATCH_ID         td[21] CODE_COMBINATION_ID
     td[6]  REMITTANCE_BATCH_ID      td[22] STRUCTURED_PAYMENT_REFERENCE (NEW)
     td[7]  RECON_FLAG               td[23] GL_DATE
     td[8]  AMOUNT                   td[24] BANK_ACCOUNT_NAME  (NEW)
     td[9]  TAX_AMOUNT               td[25] BANK_ACCOUNT_NUM   (NEW)
     td[10] SET_OF_BOOKS_ID          td[26] BANK_CURRENCY      (NEW)
     td[11] CURRENCY_CODE            td[27] BANK_DESCRIPTION   (NEW)
     td[12] RECEIVABLES_TRX_ID       td[28] IBAN
     td[13] STATUS                   td[29] SHORT_BANK_ACCOUNT_NAME (NEW)
     td[14] TYPE                     td[30] BANK_NAME          (NEW)
     td[15] RECEIPT_DATE             td[31] CUSTOMER_NAME      (NEW)
     td[16] RECEIPT_UPLOAD (skipped) td[32] BANK_ID            (NEW)

   LAST_UPDATED_BY / CREATED_BY: not in BIP report;
   defaulted to BIP_USERNAME config value.
============================================================ */
PROCEDURE LOAD_AR_RECEIPTS
IS
    v_html          CLOB;
    v_table_html    CLOB;
    v_load_id       NUMBER  := NULL;
    v_rows_fetched  NUMBER  := 0;
    v_rows_inserted NUMBER  := 0;
    v_bip_username  VARCHAR2(200);
BEGIN
    LOG_LOAD(v_load_id, 'AR_RECEIPTS', 'RUNNING');

    -- Fetch BIP service username for audit cols (not present in this BIP report)
    v_bip_username := GET_CONFIG('BIP_USERNAME');

    v_html       := CALL_BIP_REPORT(GET_CONFIG('REPORT_PATH_AR_RECEIPTS'));
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    INSERT INTO XXEMR_AR_CASH_RECEIPTS (
        loaded_date,                          -- td[1]
        receipt_number,                       -- td[2]
        cash_receipt_id,                      -- td[3]
        collector_id,                         -- td[4]
        receipt_batch_id,                     -- td[5]
        remittance_batch_id,                  -- td[6]
        recon_flag,                           -- td[7]
        amount,                               -- td[8]
        tax_amount,                           -- td[9]
        set_of_books_id,                      -- td[10]
        currency_code,                        -- td[11]
        receivables_trx_id,                   -- td[12]
        status,                               -- td[13]
        type,                                 -- td[14]
        receipt_date,                         -- td[15]
        -- td[16] RECEIPT_UPLOAD not in APEX table; skipped
        remittance_bank_account_id,           -- td[17]
        receipt_method_id,                    -- td[18]
        org_id,                               -- td[19]
        legal_entity_id,                      -- td[20]
        code_combination_id,                  -- td[21]
        structured_payment_reference,         -- td[22]  NEW
        gl_date,                              -- td[23]
        bank_account_name,                    -- td[24]  NEW
        bank_account_num,                     -- td[25]  NEW
        bank_currency,                        -- td[26]  NEW
        bank_description,                     -- td[27]  NEW
        iban,                                 -- td[28]
        short_bank_account_name,              -- td[29]  NEW
        bank_name,                            -- td[30]  NEW
        customer_name,                        -- td[31]  NEW
        bank_id,                              -- td[32]  NEW
        last_update_date,                     -- not in BIP; SYSDATE
        last_updated_by,                      -- not in BIP; BIP_USERNAME  NEW
        creation_date,                        -- not in BIP; SYSDATE
        created_by                            -- not in BIP; BIP_USERNAME  NEW
    )
    SELECT
        TO_DATE(SUBSTR(X.c1,1,10)  DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        X.c2,
        TO_NUMBER(X.c3  DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c4  DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c5  DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c6  DEFAULT NULL ON CONVERSION ERROR),
        X.c7,
        TO_NUMBER(X.c8  DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c9  DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c10 DEFAULT NULL ON CONVERSION ERROR),
        X.c11,
        TO_NUMBER(X.c12 DEFAULT NULL ON CONVERSION ERROR),
        X.c13,
        X.c14,
        TO_DATE(SUBSTR(X.c15,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_NUMBER(X.c17 DEFAULT NULL ON CONVERSION ERROR),                             -- remittance_bank_account_id
        TO_NUMBER(X.c18 DEFAULT NULL ON CONVERSION ERROR),                             -- receipt_method_id
        TO_NUMBER(X.c19 DEFAULT NULL ON CONVERSION ERROR),                             -- org_id
        TO_NUMBER(X.c20 DEFAULT NULL ON CONVERSION ERROR),                             -- legal_entity_id
        TO_NUMBER(X.c21 DEFAULT NULL ON CONVERSION ERROR),                             -- code_combination_id
        X.c22,                                                                         -- structured_payment_reference NEW
        TO_DATE(SUBSTR(X.c23,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),   -- gl_date
        X.c24,                                                                         -- bank_account_name   NEW
        X.c25,                                                                         -- bank_account_num    NEW
        X.c26,                                                                         -- bank_currency       NEW
        X.c27,                                                                         -- bank_description    NEW
        X.c28,                                                                         -- iban
        X.c29,                                                                         -- short_bank_account_name NEW
        X.c30,                                                                         -- bank_name           NEW
        X.c31,                                                                         -- customer_name       NEW
        TO_NUMBER(X.c32 DEFAULT NULL ON CONVERSION ERROR),                             -- bank_id             NEW
        TRUNC(SYSDATE),                                                                -- last_update_date
        v_bip_username,                                                                -- last_updated_by     NEW
        TRUNC(SYSDATE),                                                                -- creation_date
        v_bip_username                                                                 -- created_by          NEW
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',   -- LOADED_DATE
            c2  VARCHAR2(200)  PATH 'string(td[2])',   -- RECEIPT_NUMBER
            c3  VARCHAR2(100)  PATH 'string(td[3])',   -- CASH_RECEIPT_ID
            c4  VARCHAR2(100)  PATH 'string(td[4])',   -- COLLECTOR_ID
            c5  VARCHAR2(100)  PATH 'string(td[5])',   -- RECEIPT_BATCH_ID
            c6  VARCHAR2(100)  PATH 'string(td[6])',   -- REMITTANCE_BATCH_ID
            c7  VARCHAR2(10)   PATH 'string(td[7])',   -- RECON_FLAG
            c8  VARCHAR2(200)  PATH 'string(td[8])',   -- AMOUNT
            c9  VARCHAR2(200)  PATH 'string(td[9])',   -- TAX_AMOUNT
            c10 VARCHAR2(100)  PATH 'string(td[10])',  -- SET_OF_BOOKS_ID
            c11 VARCHAR2(50)   PATH 'string(td[11])',  -- CURRENCY_CODE
            c12 VARCHAR2(100)  PATH 'string(td[12])',  -- RECEIVABLES_TRX_ID
            c13 VARCHAR2(100)  PATH 'string(td[13])',  -- STATUS
            c14 VARCHAR2(100)  PATH 'string(td[14])',  -- TYPE
            c15 VARCHAR2(100)  PATH 'string(td[15])',  -- RECEIPT_DATE
            -- c16 = td[16] RECEIPT_UPLOAD (skipped)
            c17 VARCHAR2(100)  PATH 'string(td[17])',  -- REMITTANCE_BANK_ACCOUNT_ID
            c18 VARCHAR2(100)  PATH 'string(td[18])',  -- RECEIPT_METHOD_ID
            c19 VARCHAR2(100)  PATH 'string(td[19])',  -- ORG_ID
            c20 VARCHAR2(100)  PATH 'string(td[20])',  -- LEGAL_ENTITY_ID
            c21 VARCHAR2(100)  PATH 'string(td[21])',  -- CODE_COMBINATION_ID
            c22 VARCHAR2(500)  PATH 'string(td[22])',  -- STRUCTURED_PAYMENT_REFERENCE NEW
            c23 VARCHAR2(100)  PATH 'string(td[23])',  -- GL_DATE
            c24 VARCHAR2(360)  PATH 'string(td[24])',  -- BANK_ACCOUNT_NAME   NEW
            c25 VARCHAR2(100)  PATH 'string(td[25])',  -- BANK_ACCOUNT_NUM    NEW
            c26 VARCHAR2(50)   PATH 'string(td[26])',  -- BANK_CURRENCY       NEW
            c27 VARCHAR2(500)  PATH 'string(td[27])',  -- BANK_DESCRIPTION    NEW
            c28 VARCHAR2(200)  PATH 'string(td[28])',  -- IBAN
            c29 VARCHAR2(200)  PATH 'string(td[29])',  -- SHORT_BANK_ACCOUNT_NAME NEW
            c30 VARCHAR2(360)  PATH 'string(td[30])',  -- BANK_NAME           NEW
            c31 VARCHAR2(500)  PATH 'string(td[31])',  -- CUSTOMER_NAME       NEW
            c32 VARCHAR2(100)  PATH 'string(td[32])'   -- BANK_ID             NEW
    ) X
    WHERE  REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
      AND  NOT EXISTS (
               SELECT 1
               FROM   XXEMR_AR_CASH_RECEIPTS R
               WHERE  R.cash_receipt_id =
                      TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
           );

    v_rows_inserted := SQL%ROWCOUNT;

    SELECT COUNT(*) INTO v_rows_fetched
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS c3 VARCHAR2(100) PATH 'string(td[3])'
    )
    WHERE REGEXP_LIKE(TRIM(c3), '^[0-9]+$');

    COMMIT;
    LOG_LOAD(v_load_id, 'AR_RECEIPTS', 'COMPLETED',
        v_rows_fetched, v_rows_inserted,
        v_rows_fetched - v_rows_inserted, NULL, 'Y');
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        LOG_LOAD(v_load_id, 'AR_RECEIPTS', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END LOAD_AR_RECEIPTS;


/* ============================================================
   PUBLIC: LOAD_BANK_DETAILS
   Target : XXEMR_BANK_DETAILS
   Version: v3 – Mode changed to DELETE + full re-insert every
            run so that updated BIP data (including new
            BANK_ACCT_USE_ID rows for the same BANK_ACCOUNT_ID)
            is always reflected. SEGMENT1-9 intentionally NULL.

   BIP column order:
     td[1]  BANK_ACCOUNT_ID
     td[2]  BANK_NAME
     td[3]  BANK_ACCOUNT_NAME
     td[4]  BANK_ACCOUNT_NUM
     td[5]  BANK_ACCT_USE_ID
     td[6]  ASSET_CODE_COMBINATION_ID
     td[7]  SEGMENT1          (read but NOT inserted – left NULL)
     td[8]  SEGMENT2          (read but NOT inserted – left NULL)
     td[9]  SEGMENT3          (read but NOT inserted – left NULL)
     td[10] SEGMENT4          (read but NOT inserted – left NULL)
     td[11] SEGMENT5          (read but NOT inserted – left NULL)
     td[12] SEGMENT6          (read but NOT inserted – left NULL)
     td[13] SEGMENT7          (read but NOT inserted – left NULL)
     td[14] SEGMENT8          (read but NOT inserted – left NULL)
     td[15] SEGMENT9          (read but NOT inserted – left NULL)
     td[16] CONCATENATED_SEGMENTS
     td[17] OFFSET_ACCOUNT_COMBINATION
     td[18] LEGAL_ENTITY_NAME
     td[19] LEGAL_ENTITY_ID
     td[20] BUSINESS_UNIT_ID   (not loaded)
     td[21] BUSINESS_UNIT_NAME (not loaded)

   BANK_DETAIL_ID is an identity column – not in INSERT list.
   LAST_UPDATE_DATE / CREATION_DATE default to TRUNC(SYSDATE).
   LAST_UPDATED_BY / CREATED_BY: fallback to BIP_USERNAME config.

   Load sequence:
     Step 1 – COUNT rows in BIP (v_rows_fetched)
     Step 2 – DELETE all existing rows from XXEMR_BANK_DETAILS
     Step 3 – INSERT all rows from BIP
     Step 4 – COMMIT
============================================================ */
PROCEDURE LOAD_BANK_DETAILS
IS
    v_html          CLOB;
    v_table_html    CLOB;
    v_load_id       NUMBER  := NULL;
    v_rows_fetched  NUMBER  := 0;
    v_rows_inserted NUMBER  := 0;
    v_bip_username  VARCHAR2(200);
BEGIN
    LOG_LOAD(v_load_id, 'BANK_DETAILS', 'RUNNING');

    -- Cache BIP username for audit columns (not present in this BIP report)
    v_bip_username := GET_CONFIG('BIP_USERNAME');

    v_html       := CALL_BIP_REPORT(
                        '/Custom/Financials/Bank Reconciliation/Reports/'
                        || 'EMR_Bank_Details_Report.xdo');
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    -- Step 1: Count total valid rows coming from BIP
    SELECT COUNT(*) INTO v_rows_fetched
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS c1 VARCHAR2(100) PATH 'string(td[1])'
    )
    WHERE REGEXP_LIKE(TRIM(c1), '^[0-9]+$');

    -- Step 2: Delete all existing rows – full refresh every run
    DELETE FROM XXEMR_BANK_DETAILS;

    -- Step 3: Insert all rows from BIP
    INSERT INTO XXEMR_BANK_DETAILS (
        bank_account_id,              -- td[1]
        bank_name,                    -- td[2]
        bank_account_name,            -- td[3]
        bank_account_num,             -- td[4]
        bank_acct_use_id,             -- td[5]
        asset_code_combination_id,    -- td[6]
        -- SEGMENT1-9 (td[7]-td[15]) intentionally NOT loaded; columns stay NULL
        concatenated_segments,        -- td[16]
        offset_account_combination,   -- td[17]
        legal_entity_name,            -- td[18]
        legal_entity_id,              -- td[19]
        -- td[20] BUSINESS_UNIT_ID    (not in APEX table; not loaded)
        -- td[21] BUSINESS_UNIT_NAME  (not in APEX table; not loaded)
        last_update_date,             -- TRUNC(SYSDATE)
        last_updated_by,              -- BIP_USERNAME
        creation_date,                -- TRUNC(SYSDATE)
        created_by                    -- BIP_USERNAME
    )
    SELECT
        TO_NUMBER(X.c1  DEFAULT NULL ON CONVERSION ERROR),  -- bank_account_id
        X.c2,                                               -- bank_name
        X.c3,                                               -- bank_account_name
        X.c4,                                               -- bank_account_num
        TO_NUMBER(X.c5  DEFAULT NULL ON CONVERSION ERROR),  -- bank_acct_use_id
        TO_NUMBER(X.c6  DEFAULT NULL ON CONVERSION ERROR),  -- asset_code_combination_id
        -- c7-c15 (SEGMENT1-9) declared in XMLTABLE but NOT referenced; stay NULL
        X.c16,                                              -- concatenated_segments
        X.c17,                                              -- offset_account_combination
        X.c18,                                              -- legal_entity_name
        TO_NUMBER(X.c19 DEFAULT NULL ON CONVERSION ERROR),  -- legal_entity_id
        TRUNC(SYSDATE),                                     -- last_update_date
        v_bip_username,                                     -- last_updated_by
        TRUNC(SYSDATE),                                     -- creation_date
        v_bip_username                                      -- created_by
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',   -- BANK_ACCOUNT_ID
            c2  VARCHAR2(360)  PATH 'string(td[2])',   -- BANK_NAME
            c3  VARCHAR2(100)  PATH 'string(td[3])',   -- BANK_ACCOUNT_NAME
            c4  VARCHAR2(100)  PATH 'string(td[4])',   -- BANK_ACCOUNT_NUM
            c5  VARCHAR2(100)  PATH 'string(td[5])',   -- BANK_ACCT_USE_ID
            c6  VARCHAR2(100)  PATH 'string(td[6])',   -- ASSET_CODE_COMBINATION_ID
            c7  VARCHAR2(50)   PATH 'string(td[7])',   -- SEGMENT1  (read; not inserted)
            c8  VARCHAR2(50)   PATH 'string(td[8])',   -- SEGMENT2  (read; not inserted)
            c9  VARCHAR2(50)   PATH 'string(td[9])',   -- SEGMENT3  (read; not inserted)
            c10 VARCHAR2(50)   PATH 'string(td[10])',  -- SEGMENT4  (read; not inserted)
            c11 VARCHAR2(50)   PATH 'string(td[11])',  -- SEGMENT5  (read; not inserted)
            c12 VARCHAR2(50)   PATH 'string(td[12])',  -- SEGMENT6  (read; not inserted)
            c13 VARCHAR2(50)   PATH 'string(td[13])',  -- SEGMENT7  (read; not inserted)
            c14 VARCHAR2(50)   PATH 'string(td[14])',  -- SEGMENT8  (read; not inserted)
            c15 VARCHAR2(50)   PATH 'string(td[15])',  -- SEGMENT9  (read; not inserted)
            c16 VARCHAR2(500)  PATH 'string(td[16])',  -- CONCATENATED_SEGMENTS
            c17 VARCHAR2(500)  PATH 'string(td[17])',  -- OFFSET_ACCOUNT_COMBINATION
            c18 VARCHAR2(240)  PATH 'string(td[18])',  -- LEGAL_ENTITY_NAME
            c19 VARCHAR2(100)  PATH 'string(td[19])',  -- LEGAL_ENTITY_ID
            c20 VARCHAR2(100)  PATH 'string(td[20])',  -- BUSINESS_UNIT_ID   (not inserted)
            c21 VARCHAR2(240)  PATH 'string(td[21])'   -- BUSINESS_UNIT_NAME (not inserted)
    ) X
    WHERE REGEXP_LIKE(TRIM(X.c1), '^[0-9]+$');

    v_rows_inserted := SQL%ROWCOUNT;

    -- Step 4: Commit both the DELETE and INSERT together
    COMMIT;

    LOG_LOAD(v_load_id, 'BANK_DETAILS', 'COMPLETED',
        v_rows_fetched, v_rows_inserted,
        v_rows_fetched - v_rows_inserted, NULL, 'Y');
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
EXCEPTION
    WHEN OTHERS THEN
        -- Rolls back both the DELETE and INSERT atomically
        ROLLBACK;
        LOG_LOAD(v_load_id, 'BANK_DETAILS', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END LOAD_BANK_DETAILS;


/* ============================================================
   PUBLIC: LOAD_ALL_REPORTS
   Calls all five loaders in sequence.  Each loader runs inside
   its own BEGIN/EXCEPTION block so a single failure does not
   abort the remaining loaders.  A summary error is raised at
   the end if any individual loader failed.
============================================================ */
PROCEDURE LOAD_ALL_REPORTS
IS
    v_overall_load_id  NUMBER        := NULL;
    v_errors           NUMBER        := 0;
    v_error_summary    VARCHAR2(4000) := NULL;
BEGIN
    LOG_LOAD(v_overall_load_id, 'ALL_REPORTS', 'RUNNING');

    BEGIN
        LOAD_STMT_HEADERS;
    EXCEPTION WHEN OTHERS THEN
        v_errors        := v_errors + 1;
        v_error_summary := v_error_summary || 'HEADERS: '
            || SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 500) || ' | ';
    END;

    BEGIN
        LOAD_STMT_LINES;
    EXCEPTION WHEN OTHERS THEN
        v_errors        := v_errors + 1;
        v_error_summary := v_error_summary || 'LINES: '
            || SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 500) || ' | ';
    END;

    BEGIN
        LOAD_EXT_TRANSACTIONS;
    EXCEPTION WHEN OTHERS THEN
        v_errors        := v_errors + 1;
        v_error_summary := v_error_summary || 'EXT_TXN: '
            || SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 500) || ' | ';
    END;

    BEGIN
        LOAD_AR_RECEIPTS;
    EXCEPTION WHEN OTHERS THEN
        v_errors        := v_errors + 1;
        v_error_summary := v_error_summary || 'AR_RECEIPTS: '
            || SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 500) || ' | ';
    END;

    BEGIN
        LOAD_BANK_DETAILS;
    EXCEPTION WHEN OTHERS THEN
        v_errors        := v_errors + 1;
        v_error_summary := v_error_summary || 'BANK_DETAILS: '
            || SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 500) || ' | ';
    END;

    IF v_errors = 0 THEN
        LOG_LOAD(v_overall_load_id, 'ALL_REPORTS', 'COMPLETED',
            NULL, NULL, NULL, NULL, 'Y');
    ELSE
        LOG_LOAD(v_overall_load_id, 'ALL_REPORTS', 'PARTIAL_FAILURE',
            NULL, NULL, NULL,
            v_errors || ' report(s) failed: '
                || SUBSTR(v_error_summary, 1, 3500), 'Y');
        RAISE_APPLICATION_ERROR(-20010,
            'BIP Loader completed with ' || v_errors
            || ' error(s). Check APEX_BIP_LOAD_LOG for details.');
    END IF;
END LOAD_ALL_REPORTS;
------------------------------------------------------------
PROCEDURE SYNC_RECON_STATUS_FROM_BIP
IS
    v_html              CLOB;
    v_table_html        CLOB;
    v_load_id           NUMBER  := NULL;
 
    -- Counts for logging
    v_sent_count        NUMBER  := 0;
    v_reconciled_count  NUMBER  := 0;
    v_rejected_count    NUMBER  := 0;
    v_unchanged_count   NUMBER  := 0;
BEGIN
    LOG_LOAD(v_load_id, 'RECON_SYNC', 'RUNNING');
 
    -- ----------------------------------------------------------------
    -- STEP 1: Early exit — nothing to do if no SENT rows exist
    -- ----------------------------------------------------------------
    SELECT COUNT(DISTINCT statement_line_id)
      INTO v_sent_count
      FROM xxemr_recon_fbdi_lines
     WHERE send_status = 'SENT';
 
    IF v_sent_count = 0 THEN
        LOG_LOAD(v_load_id, 'RECON_SYNC', 'COMPLETED',
            0, 0, 0,
            'No SENT rows in XXEMR_RECON_FBDI_LINES. Nothing to sync.', 'Y');
        RETURN;
    END IF;
 
    -- ----------------------------------------------------------------
    -- STEP 2: Call BIP — one call, returns all statement lines
    -- ----------------------------------------------------------------
    v_html       := CALL_BIP_REPORT(GET_CONFIG('REPORT_PATH_STMT_LINES'));
    v_table_html := EXTRACT_TABLE_HTML(v_html);
 
    -- ----------------------------------------------------------------
    -- STEP 3: UPDATE statement lines where BIP says REC but APEX says UNR
    --
    -- Scoped to: lines that exist in XXEMR_RECON_FBDI_LINES with
    -- send_status = 'SENT' — i.e. lines WE submitted via FBDI.
    -- Lines reconciled by other means are untouched.
    -- ----------------------------------------------------------------
    UPDATE xxemr_bank_statement_lines apex_line
       SET apex_line.recon_status      = 'REC',
           apex_line.match_flag        = 'Y',
           apex_line.bip_last_seen_date = TRUNC(SYSDATE),
           apex_line.last_updated      = SYSTIMESTAMP
     WHERE apex_line.recon_status     != 'REC'
       AND apex_line.statement_line_id IN (
               -- Must be a line WE submitted via FBDI
               SELECT f.statement_line_id
                 FROM xxemr_recon_fbdi_lines f
                WHERE f.send_status      = 'SENT'
                  AND f.source_code      = 'BS'  -- one row per group, not duplicates
           )
       AND apex_line.statement_line_id IN (
               -- BIP confirms it is now REC in Fusion
               SELECT TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
                 FROM XMLTABLE(
                          '/table/tr[position() > 1]'
                          PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
                          COLUMNS
                              c3 VARCHAR2(100) PATH 'string(td[3])',  -- STATEMENT_LINE_ID
                              c8 VARCHAR2(20)  PATH 'string(td[8])'   -- RECON_STATUS
                      ) X
                WHERE TRIM(X.c8) = 'REC'
                  AND REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
           );
 
    v_reconciled_count := SQL%ROWCOUNT;
 
    -- ----------------------------------------------------------------
    -- STEP 4: Mark confirmed lines as RECONCILED in the FBDI table
    -- ----------------------------------------------------------------
    UPDATE xxemr_recon_fbdi_lines f
       SET f.send_status      = 'RECONCILED',
           f.reconciled_date  = SYSTIMESTAMP,
           f.last_update_date = SYSTIMESTAMP,
           f.last_updated_by  = 'BIP_SYNC'
     WHERE f.send_status = 'SENT'
       AND f.statement_line_id IN (
               -- Lines we just confirmed reconciled in step 3
               SELECT statement_line_id
                 FROM xxemr_bank_statement_lines
                WHERE recon_status       = 'REC'
                  AND bip_last_seen_date = TRUNC(SYSDATE)
           );
 
    -- ----------------------------------------------------------------
    -- STEP 5: Mark lines still UNR in BIP as FUSION_REJECTED
    --
    -- These are lines we submitted (SENT) but BIP still shows UNR
    -- after the ESS run — Fusion rejected them (tolerance, invalid etc.)
    -- BIP_LAST_SEEN_DATE = TRUNC(SYSDATE) guard confirms BIP ran fresh.
    -- ----------------------------------------------------------------
    UPDATE xxemr_recon_fbdi_lines f
       SET f.send_status      = 'FUSION_REJECTED',
           f.last_error       = 'BIP sync: line still UNRECONCILED in Fusion after FBDI load. '
                                || 'Check Fusion CE for tolerance or validity issues.',
           f.last_update_date = SYSTIMESTAMP,
           f.last_updated_by  = 'BIP_SYNC'
     WHERE f.send_status      = 'SENT'
       AND f.source_code      = 'BS'
       AND f.statement_line_id IN (
               -- BIP shows UNR — Fusion did not reconcile it
               SELECT TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
                 FROM XMLTABLE(
                          '/table/tr[position() > 1]'
                          PASSING XMLTYPE(REPLACE(v_table_html, '&', '&amp;'))
                          COLUMNS
                              c3 VARCHAR2(100) PATH 'string(td[3])',  -- STATEMENT_LINE_ID
                              c8 VARCHAR2(20)  PATH 'string(td[8])'   -- RECON_STATUS
                      ) X
                WHERE TRIM(X.c8) = 'UNR'
                  AND REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
           );
 
    v_rejected_count := SQL%ROWCOUNT;
 
    -- Lines still SENT after both updates = not yet visible in BIP
    -- (ESS may still be running — they will be picked up on the next sync run)
    SELECT COUNT(DISTINCT statement_line_id)
      INTO v_unchanged_count
      FROM xxemr_recon_fbdi_lines
     WHERE send_status  = 'SENT'
       AND source_code  = 'BS';
 
    COMMIT;
 
    LOG_LOAD(v_load_id, 'RECON_SYNC', 'COMPLETED',
        v_sent_count,
        v_reconciled_count,
        v_rejected_count,
        'Reconciled: '     || v_reconciled_count
        || ' | Rejected: ' || v_rejected_count
        || ' | Still SENT (ESS pending): ' || v_unchanged_count,
        'Y');
 
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
 
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        IF v_html IS NOT NULL THEN
            DBMS_LOB.FREETEMPORARY(v_html);
        END IF;
        IF v_table_html IS NOT NULL THEN
            DBMS_LOB.FREETEMPORARY(v_table_html);
        END IF;
        LOG_LOAD(v_load_id, 'RECON_SYNC', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END SYNC_RECON_STATUS_FROM_BIP;

END PKG_BIP_LOADER;
/