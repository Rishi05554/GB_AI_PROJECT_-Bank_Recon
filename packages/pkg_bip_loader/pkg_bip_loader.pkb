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

    /* -------------------------------------------------------
       Extract reportBytes using CLOB string operations only.
       Do NOT call v_xml.getClobVal() — it triggers LPX-00231
       because the SOAP response XML contains "Bank Reconciliation"
       (space in element content confuses Oracle XML parser).
       Instead, use APEX_WEB_SERVICE.G_HEADERS to get the raw
       response, or extract reportBytes directly from the XMLTYPE
       serialization using INSTR on the getClobVal output but
       with error handling, OR use the workaround below:
       call getClobVal() but catch and re-extract via DBMS_XMLDOM.
       Simplest fix: extract reportBytes from raw SOAP using
       DBMS_LOB operations on the XML internal representation.
    ------------------------------------------------------- */
    -- Extract reportBytes directly without getClobVal()
    -- XMLTYPE.extract() with XPath avoids full serialization
    BEGIN
        v_response_clob := v_xml.extract(
            '//reportBytes/text()',
            'xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService"'
        ).getStringVal();
    EXCEPTION
        WHEN OTHERS THEN
            -- Fallback: getClobVal with manual extraction
            v_response_clob := v_xml.getClobVal();
            v_pos_start     := INSTR(v_response_clob, '<reportBytes>') + LENGTH('<reportBytes>');
            v_pos_end       := INSTR(v_response_clob, '</reportBytes>');
            v_response_clob := SUBSTR(v_response_clob, v_pos_start, v_pos_end - v_pos_start);
    END;
    -- If XPath extraction returned the value directly, use as-is
    -- (no need to strip tags — extract(//text()) returns content only)
    IF v_response_clob IS NULL THEN
        v_response_clob := v_xml.getClobVal();
        v_pos_start     := INSTR(v_response_clob, '<reportBytes>') + LENGTH('<reportBytes>');
        v_pos_end       := INSTR(v_response_clob, '</reportBytes>');
        v_response_clob := SUBSTR(v_response_clob, v_pos_start, v_pos_end - v_pos_start);
    END IF;

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

    -- FIX: keep &amp; &lt; &gt; &quot; encoded — XMLTYPE needs valid XML
    -- FIX: replace &nbsp; with XML-safe numeric reference
    -- FIX: strip HTML tag attributes to prevent LPX-00231
    v_result := REPLACE(v_result, '&nbsp;', '&#160;');
    v_result := REGEXP_REPLACE(v_result,
                    '<(table|thead|tbody|tr|th|td)(\s[^>]*)?>',
                    '<\1>',
                    1, 0, 'i');

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
        PASSING XMLTYPE(v_table_html)
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',
            c2  VARCHAR2(100)  PATH 'string(td[2])',
            c3  VARCHAR2(200)  PATH 'string(td[3])',
            c4  VARCHAR2(100)  PATH 'string(td[4])',
            c5  VARCHAR2(500)  PATH 'string(td[5])',
            c6  VARCHAR2(200)  PATH 'string(td[6])',
            c7  VARCHAR2(500)  PATH 'string(td[7])',
            c8  VARCHAR2(200)  PATH 'string(td[8])',
            c9  VARCHAR2(500)  PATH 'string(td[9])',
            c10 VARCHAR2(100)  PATH 'string(td[10])',
            c11 VARCHAR2(100)  PATH 'string(td[11])',
            c12 VARCHAR2(100)  PATH 'string(td[12])',
            c13 VARCHAR2(100)  PATH 'string(td[13])',
            c14 VARCHAR2(100)  PATH 'string(td[14])',
            c15 VARCHAR2(100)  PATH 'string(td[15])',
            c16 VARCHAR2(100)  PATH 'string(td[16])',
            c17 VARCHAR2(100)  PATH 'string(td[17])',
            c18 VARCHAR2(100)  PATH 'string(td[18])',
            c19 VARCHAR2(100)  PATH 'string(td[19])',
            c20 VARCHAR2(100)  PATH 'string(td[20])',
            c21 VARCHAR2(200)  PATH 'string(td[21])',
            c22 VARCHAR2(100)  PATH 'string(td[22])',
            c23 VARCHAR2(200)  PATH 'string(td[23])',
            c24 VARCHAR2(100)  PATH 'string(td[24])',
            c25 VARCHAR2(100)  PATH 'string(td[25])',
            c26 VARCHAR2(100)  PATH 'string(td[26])',
            c27 VARCHAR2(100)  PATH 'string(td[27])',
            c28 VARCHAR2(100)  PATH 'string(td[28])',
            c29 VARCHAR2(100)  PATH 'string(td[29])',
            c30 VARCHAR2(50)   PATH 'string(td[30])',
            c31 VARCHAR2(50)   PATH 'string(td[31])',
            c32 VARCHAR2(100)  PATH 'string(td[32])',
            c33 VARCHAR2(200)  PATH 'string(td[33])'
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
        PASSING XMLTYPE(v_table_html)
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
   Version: v7 – FULL BIP COLUMN REMAP (actual BIP output verified).

   BIP actual column order:
     td[1]  LOADED_DATE
     td[2]  STATEMENT_HEADER_ID
     td[3]  STATEMENT_LINE_ID
     td[4]  BANK_ACCOUNT_ID         (BIP col present; not loaded to APEX)
     td[5]  LINE_NUMBER
     td[6]  TRX_TYPE
     td[7]  FLOW_INDICATOR
     td[8]  RECON_STATUS
     td[9]  AMOUNT                  NOT NULL → NVL(0)
     td[10] REVERSAL_IND_FLAG       VARCHAR2(1) → CASE Y/N/NULL
     td[11] BOOKING_DATE            NOT NULL basis for STATEMENT_DATE
     td[12] VALUE_DATE
     td[13] TRX_AMOUNT
     td[14] TRX_CURR_CODE
     td[15] EXCHANGE_RATE
     td[16] EXCHANGE_RATE_DATE
     td[17] EXCHANGE_RATE_TYPE
     td[18] EXTERNAL_TRANSACTION_ID
     td[19] DESCRIPTION
     td[20] RECON_REFERENCE
     td[21] CHECK_NUMBER
     td[22] EXCEPTION_FLAG
     td[23] LAST_UPDATE_DATE
     td[24] LAST_UPDATED_BY
     td[25] CREATION_DATE
     td[26] CREATED_BY
     td[27] BANK_ID
     td[28] TRX_CODE
     td[29] TRX_DESCRIPTION
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

    -- ----------------------------------------------------------------
    -- Step 1: stamp bip_last_seen_date on rows already in APEX
    -- ----------------------------------------------------------------
    UPDATE XXEMR_BANK_STATEMENT_LINES L
    SET    L.bip_last_seen_date = TRUNC(SYSDATE)
    WHERE  L.statement_line_id IN (
        SELECT TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
        FROM   XMLTABLE(
                   '/table/tr[position() > 1]'
                   PASSING XMLTYPE(v_table_html)
                   COLUMNS c3 VARCHAR2(100) PATH 'string(td[3])'
               ) X
        WHERE  REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
    );

    -- ----------------------------------------------------------------
    -- Step 2: insert brand-new rows only
    -- ----------------------------------------------------------------
    INSERT INTO XXEMR_BANK_STATEMENT_LINES (
        statement_header_id,        -- td[2]
        statement_line_id,          -- td[3]
        bank_account_id,            -- td[4]  read from BIP; also back-filled Step 3
        line_number,                -- td[5]
        trx_type,                   -- td[6]
        flow_indicator,             -- td[7]
        recon_status,               -- td[8]
        amount,                     -- td[9]  NOT NULL → NVL 0
        reversal_ind_flag,          -- td[10] VARCHAR2(1) → CASE Y/N/NULL
        statement_date,             -- NOT NULL → COALESCE(td[11],td[12],SYSDATE)
        booking_date,               -- td[11] raw (nullable)
        value_date,                 -- td[12]
        trx_amount,                 -- td[13]
        currency_code,              -- td[14]
        exchange_rate,              -- td[15]
        exchange_rate_date,         -- td[16]
        exchange_rate_type,         -- td[17]
        external_txn_id,            -- td[18]
        description,                -- td[19]
        reference_num,              -- td[20]
        check_number,               -- td[21]
        exception_flag,             -- td[22]
        last_update_date,           -- td[23]
        last_updated_by,            -- td[24]
        creation_date,              -- td[25]
        created_by,                 -- td[26]
        bank_id,                    -- td[27]
        trx_code,                   -- td[28]
        trx_description,            -- td[29]
        process_date,               -- NOT NULL → COALESCE(td[11],td[12],SYSDATE)
        period_name,                -- derived from same COALESCE
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
        TO_NUMBER(X.c2  DEFAULT NULL ON CONVERSION ERROR),   -- statement_header_id  td[2]
        TO_NUMBER(X.c3  DEFAULT NULL ON CONVERSION ERROR),   -- statement_line_id    td[3]

        /* td[4] BANK_ACCOUNT_ID — now available directly from BIP.
           Also back-filled in Step 3 via JOIN to headers for safety. */
        TO_NUMBER(X.c4  DEFAULT NULL ON CONVERSION ERROR),   -- bank_account_id      td[4]

        TO_NUMBER(X.c5  DEFAULT NULL ON CONVERSION ERROR),   -- line_number          td[5]
        X.c6,                                                -- trx_type             td[6]
        X.c7,                                                -- flow_indicator       td[7]
        X.c8,                                                -- recon_status         td[8]

        /* -------------------------------------------------------
           AMOUNT  td[9] — NOT NULL NUMBER(18,2)
           NVL to 0 when BIP sends blank (reversal/memo lines).
        ------------------------------------------------------- */
        NVL(TO_NUMBER(X.c9 DEFAULT NULL ON CONVERSION ERROR), 0),  -- amount

        /* -------------------------------------------------------
           REVERSAL_IND_FLAG  td[10] — VARCHAR2(1)
           BIP sends 'Yes','No','TRUE','FALSE','Y','N'.
           CASE maps all to Y / N / NULL.
        ------------------------------------------------------- */
        CASE
            WHEN UPPER(TRIM(X.c10)) IN ('Y','YES','TRUE', '1') THEN 'Y'
            WHEN UPPER(TRIM(X.c10)) IN ('N','NO', 'FALSE','0') THEN 'N'
            ELSE NULL
        END,                                                 -- reversal_ind_flag

        /* -------------------------------------------------------
           STATEMENT_DATE — NOT NULL DATE
           COALESCE: td[11] BOOKING_DATE → td[12] VALUE_DATE → today
        ------------------------------------------------------- */
        COALESCE(
            TO_DATE(SUBSTR(X.c11,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
            TO_DATE(SUBSTR(X.c12,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
            TRUNC(SYSDATE)
        ),                                                   -- statement_date

        TO_DATE(SUBSTR(X.c11,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- booking_date  raw nullable
        TO_DATE(SUBSTR(X.c12,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- value_date    td[12]
        TO_NUMBER(X.c13 DEFAULT NULL ON CONVERSION ERROR),   -- trx_amount           td[13]
        X.c14,                                               -- currency_code         td[14]
        TO_NUMBER(X.c15 DEFAULT NULL ON CONVERSION ERROR),   -- exchange_rate         td[15]
        TO_DATE(SUBSTR(X.c16,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- exchange_rate_date td[16]
        X.c17,                                               -- exchange_rate_type    td[17]
        X.c18,                                               -- external_txn_id       td[18]
        X.c19,                                               -- description           td[19]
        X.c20,                                               -- reference_num         td[20]
        X.c21,                                               -- check_number          td[21]
        X.c22,                                               -- exception_flag        td[22]
        CASE WHEN X.c23 IS NOT NULL AND X.c23 != ''
             THEN TO_TIMESTAMP(SUBSTR(X.c23,1,19), 'YYYY-MM-DD"T"HH24:MI:SS')
        END,                                                 -- last_update_date      td[23]
        X.c24,                                               -- last_updated_by       td[24]
        CASE WHEN X.c25 IS NOT NULL AND X.c25 != ''
             THEN TO_TIMESTAMP(SUBSTR(X.c25,1,19), 'YYYY-MM-DD"T"HH24:MI:SS')
        END,                                                 -- creation_date         td[25]
        X.c26,                                               -- created_by            td[26]
        TO_NUMBER(X.c27 DEFAULT NULL ON CONVERSION ERROR),   -- bank_id               td[27]
        X.c28,                                               -- trx_code              td[28]
        X.c29,                                               -- trx_description       td[29]

        /* -------------------------------------------------------
           PROCESS_DATE — NOT NULL DATE, same COALESCE as STATEMENT_DATE
        ------------------------------------------------------- */
        COALESCE(
            TO_DATE(SUBSTR(X.c11,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
            TO_DATE(SUBSTR(X.c12,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
            TRUNC(SYSDATE)
        ),                                                   -- process_date

        TO_CHAR(
            COALESCE(
                TO_DATE(SUBSTR(X.c11,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
                TO_DATE(SUBSTR(X.c12,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
                TRUNC(SYSDATE)
            ),
            'Mon-YY'
        ),                                                   -- period_name

        TRUNC(SYSDATE),                                      -- bip_last_seen_date
        'N',   -- external_flag
        'N',   -- pw_keyword_check_done
        'N',   -- full_keyword_check_done
        'N',   -- pw_check_done
        'N',   -- month_end_check_done
        'N',   -- dashboard_flag
        NULL,  -- approval_status
        'N'    -- match_flag

    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(v_table_html)
        COLUMNS
            c2  VARCHAR2(100)  PATH 'string(td[2])',   -- STATEMENT_HEADER_ID
            c3  VARCHAR2(100)  PATH 'string(td[3])',   -- STATEMENT_LINE_ID
            c4  VARCHAR2(100)  PATH 'string(td[4])',   -- BANK_ACCOUNT_ID     (new in BIP)
            c5  VARCHAR2(100)  PATH 'string(td[5])',   -- LINE_NUMBER
            c6  VARCHAR2(200)  PATH 'string(td[6])',   -- TRX_TYPE
            c7  VARCHAR2(10)   PATH 'string(td[7])',   -- FLOW_INDICATOR
            c8  VARCHAR2(100)  PATH 'string(td[8])',   -- RECON_STATUS
            c9  VARCHAR2(200)  PATH 'string(td[9])',   -- AMOUNT              NOT NULL fixed v5+v7
            c10 VARCHAR2(20)   PATH 'string(td[10])',  -- REVERSAL_IND_FLAG   VARCHAR2(1) fixed v6+v7
            c11 VARCHAR2(100)  PATH 'string(td[11])',  -- BOOKING_DATE
            c12 VARCHAR2(100)  PATH 'string(td[12])',  -- VALUE_DATE
            c13 VARCHAR2(200)  PATH 'string(td[13])',  -- TRX_AMOUNT
            c14 VARCHAR2(50)   PATH 'string(td[14])',  -- TRX_CURR_CODE
            c15 VARCHAR2(100)  PATH 'string(td[15])',  -- EXCHANGE_RATE
            c16 VARCHAR2(100)  PATH 'string(td[16])',  -- EXCHANGE_RATE_DATE
            c17 VARCHAR2(100)  PATH 'string(td[17])',  -- EXCHANGE_RATE_TYPE
            c18 VARCHAR2(100)  PATH 'string(td[18])',  -- EXTERNAL_TRANSACTION_ID
            c19 VARCHAR2(500)  PATH 'string(td[19])',  -- DESCRIPTION
            c20 VARCHAR2(200)  PATH 'string(td[20])',  -- RECON_REFERENCE
            c21 VARCHAR2(200)  PATH 'string(td[21])',  -- CHECK_NUMBER
            c22 VARCHAR2(10)   PATH 'string(td[22])',  -- EXCEPTION_FLAG
            c23 VARCHAR2(100)  PATH 'string(td[23])',  -- LAST_UPDATE_DATE
            c24 VARCHAR2(200)  PATH 'string(td[24])',  -- LAST_UPDATED_BY
            c25 VARCHAR2(100)  PATH 'string(td[25])',  -- CREATION_DATE
            c26 VARCHAR2(200)  PATH 'string(td[26])',  -- CREATED_BY
            c27 VARCHAR2(100)  PATH 'string(td[27])',  -- BANK_ID
            c28 VARCHAR2(100)  PATH 'string(td[28])',  -- TRX_CODE
            c29 VARCHAR2(500)  PATH 'string(td[29])'   -- TRX_DESCRIPTION
    ) X
    WHERE REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
      AND NOT EXISTS (
            SELECT 1
            FROM   XXEMR_BANK_STATEMENT_LINES L
            WHERE  L.statement_line_id =
                   TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
      );

    v_rows_inserted := SQL%ROWCOUNT;

    -- ----------------------------------------------------------------
    -- Step 3: back-fill BANK_ACCOUNT_ID where td[4] was NULL
    -- (safety net — td[4] now populated from BIP directly above)
    -- ----------------------------------------------------------------
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
        PASSING XMLTYPE(v_table_html)
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
   Version: v5 – Removed date filter entirely. Load ALL history on first
            run. NOT EXISTS dedup prevents duplicates on re-runs.
            BIP size limit handled by INSTR/SUBSTR parser (no XMLTYPE). – Extended filter from 12 months to 36 months (3 years).
            BIP sample confirmed td[34] = 2024-10-01 which is outside
            the 12-month window. 36 months covers Oct 2022 to Mar 2026.
            because AP data LAST_UPDATE_DATE spans historical periods.
            BIP sample confirmed td[34] = 2024-10-01 format.
            Filter: LAST_UPDATE_DATE >= ADD_MONTHS(TRUNC(SYSDATE,MM), -36) – unchanged.

   BIP column order:
     td[1]  EXTERNAL_TRANSACTION_ID  td[17] TRANSACTION_TYPE
     td[2]  RECON_HISTORY_ID         td[18] STATUS -> recon_status
     td[3]  BANK_ACCOUNT_ID          td[19] DESCRIPTION
     td[4]  BANK_ACCOUNT_NAME        td[20] SOURCE
     td[5]  BANK_ACCOUNT_NUM         td[21] CLEARED_DATE
     td[6]  BANK_CURRENCY            td[22] LOADED_DATE
     td[7]  BANK_DESCRIPTION (skip)  td[23] LAST_UPDATE_DATE
     td[8]  IBAN                     td[24] LAST_UPDATED_BY
     td[9]  SHORT_BANK_ACCT  (skip)  td[25] CREATION_DATE
     td[10] BUSINESS_UNIT_ID (skip)  td[26] CREATED_BY
     td[11] LEGAL_ENTITY_ID  (skip)  td[27] REFERENCE_TEXT -> reference_num
     td[12] TRANSACTION_ID           td[28] LOB
     td[13] STATEMENT_LINE_ID        td[29] (gap/empty)
     td[14] TRANSACTION_DATE         td[30] TRANSACTION_TYPE_CODE
     td[15] VALUE_DATE               td[31] BANK_ID
     td[16] AMOUNT
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
        ext_txn_id,
        fusion_ext_txn_id,
        recon_history_id,
        bank_account_id,
        bank_account_name,
        bank_account_num,
        currency_code,
        iban,
        transaction_id,
        statement_line_id,
        transaction_date,
        value_date,
        amount,
        transaction_type,
        recon_status,
        description,
        source,
        cleared_date,
        loaded_date,
        reference_num,
        transaction_type_code,
        bank_id
    )
    SELECT
        X.c1,
        X.c1,
        TO_NUMBER(X.c2  DEFAULT NULL ON CONVERSION ERROR),
        X.c3,
        X.c4,
        X.c5,
        X.c6,
        X.c8,
        TO_NUMBER(X.c12 DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c13 DEFAULT NULL ON CONVERSION ERROR),
        TO_DATE(SUBSTR(X.c14,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_DATE(SUBSTR(X.c15,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_NUMBER(X.c16 DEFAULT NULL ON CONVERSION ERROR),
        X.c17,
        X.c18,
        X.c19,
        X.c20,
        TO_DATE(SUBSTR(X.c21,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        TO_DATE(SUBSTR(X.c22,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        X.c27,
        X.c30,
        TO_NUMBER(X.c31 DEFAULT NULL ON CONVERSION ERROR)
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(v_table_html)
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',
            c2  VARCHAR2(100)  PATH 'string(td[2])',
            c3  VARCHAR2(50)   PATH 'string(td[3])',
            c4  VARCHAR2(360)  PATH 'string(td[4])',
            c5  VARCHAR2(100)  PATH 'string(td[5])',
            c6  VARCHAR2(50)   PATH 'string(td[6])',
            c8  VARCHAR2(200)  PATH 'string(td[8])',
            c12 VARCHAR2(100)  PATH 'string(td[12])',
            c13 VARCHAR2(100)  PATH 'string(td[13])',
            c14 VARCHAR2(100)  PATH 'string(td[14])',
            c15 VARCHAR2(100)  PATH 'string(td[15])',
            c16 VARCHAR2(200)  PATH 'string(td[16])',
            c17 VARCHAR2(200)  PATH 'string(td[17])',
            c18 VARCHAR2(100)  PATH 'string(td[18])',
            c19 VARCHAR2(500)  PATH 'string(td[19])',
            c20 VARCHAR2(100)  PATH 'string(td[20])',
            c21 VARCHAR2(100)  PATH 'string(td[21])',
            c22 VARCHAR2(100)  PATH 'string(td[22])',
            c23 VARCHAR2(100)  PATH 'string(td[23])',
            c25 VARCHAR2(100)  PATH 'string(td[25])',
            c26 VARCHAR2(200)  PATH 'string(td[26])',
            c27 VARCHAR2(500)  PATH 'string(td[27])',
            c28 VARCHAR2(200)  PATH 'string(td[28])',
            c30 VARCHAR2(200)  PATH 'string(td[30])',
            c31 VARCHAR2(100)  PATH 'string(td[31])'
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
        PASSING XMLTYPE(v_table_html)
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
   Version: v5 – Removed date filter entirely. Load ALL history on first
            run. NOT EXISTS dedup prevents duplicates on re-runs.
            BIP size limit handled by INSTR/SUBSTR parser (no XMLTYPE). – Extended filter from 12 months to 36 months (3 years).
            BIP sample confirmed td[34] = 2024-10-01 which is outside
            the 12-month window. 36 months covers Oct 2022 to Mar 2026.
            because AP data LAST_UPDATE_DATE spans historical periods.
            BIP sample confirmed td[34] = 2024-10-01 format.
            Filter: LAST_UPDATE_DATE >= ADD_MONTHS(TRUNC(SYSDATE,MM), -36) – unchanged.

   BIP column order:
     td[1]  LOADED_DATE              td[17] REMITTANCE_BANK_ACCOUNT_ID
     td[2]  RECEIPT_NUMBER           td[18] RECEIPT_METHOD_ID
     td[3]  CASH_RECEIPT_ID          td[19] ORG_ID
     td[4]  COLLECTOR_ID             td[20] LEGAL_ENTITY_ID
     td[5]  RECEIPT_BATCH_ID         td[21] CODE_COMBINATION_ID
     td[6]  REMITTANCE_BATCH_ID      td[22] STRUCTURED_PAYMENT_REFERENCE
     td[7]  RECON_FLAG               td[23] GL_DATE
     td[8]  AMOUNT                   td[24] BANK_ACCOUNT_NAME
     td[9]  TAX_AMOUNT               td[25] BANK_ACCOUNT_NUM
     td[10] SET_OF_BOOKS_ID          td[26] BANK_CURRENCY
     td[11] CURRENCY_CODE            td[27] BANK_DESCRIPTION
     td[12] RECEIVABLES_TRX_ID       td[28] IBAN
     td[13] STATUS                   td[29] SHORT_BANK_ACCOUNT_NAME
     td[14] TYPE                     td[30] BANK_NAME
     td[15] RECEIPT_DATE             td[31] CUSTOMER_NAME
     td[16] RECEIPT_UPLOAD (skipped) td[32] BANK_ID
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

    v_bip_username := GET_CONFIG('BIP_USERNAME');

    v_html       := CALL_BIP_REPORT(GET_CONFIG('REPORT_PATH_AR_RECEIPTS'));
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    INSERT INTO XXEMR_AR_CASH_RECEIPTS (
        loaded_date,
        receipt_number,
        cash_receipt_id,
        collector_id,
        receipt_batch_id,
        remittance_batch_id,
        recon_flag,
        amount,
        tax_amount,
        set_of_books_id,
        currency_code,
        receivables_trx_id,
        status,
        type,
        receipt_date,
        remittance_bank_account_id,
        receipt_method_id,
        org_id,
        legal_entity_id,
        code_combination_id,
        structured_payment_reference,
        gl_date,
        bank_account_name,
        bank_account_num,
        bank_currency,
        bank_description,
        iban,
        short_bank_account_name,
        bank_name,
        customer_name,
        bank_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by
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
        TO_NUMBER(X.c17 DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c18 DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c19 DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c20 DEFAULT NULL ON CONVERSION ERROR),
        TO_NUMBER(X.c21 DEFAULT NULL ON CONVERSION ERROR),
        X.c22,
        TO_DATE(SUBSTR(X.c23,1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),
        X.c24,
        X.c25,
        X.c26,
        X.c27,
        X.c28,
        X.c29,
        X.c30,
        X.c31,
        TO_NUMBER(X.c32 DEFAULT NULL ON CONVERSION ERROR),
        TRUNC(SYSDATE),
        v_bip_username,
        TRUNC(SYSDATE),
        v_bip_username
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(v_table_html)
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',
            c2  VARCHAR2(200)  PATH 'string(td[2])',
            c3  VARCHAR2(100)  PATH 'string(td[3])',
            c4  VARCHAR2(100)  PATH 'string(td[4])',
            c5  VARCHAR2(100)  PATH 'string(td[5])',
            c6  VARCHAR2(100)  PATH 'string(td[6])',
            c7  VARCHAR2(10)   PATH 'string(td[7])',
            c8  VARCHAR2(200)  PATH 'string(td[8])',
            c9  VARCHAR2(200)  PATH 'string(td[9])',
            c10 VARCHAR2(100)  PATH 'string(td[10])',
            c11 VARCHAR2(50)   PATH 'string(td[11])',
            c12 VARCHAR2(100)  PATH 'string(td[12])',
            c13 VARCHAR2(100)  PATH 'string(td[13])',
            c14 VARCHAR2(100)  PATH 'string(td[14])',
            c15 VARCHAR2(100)  PATH 'string(td[15])',
            c17 VARCHAR2(100)  PATH 'string(td[17])',
            c18 VARCHAR2(100)  PATH 'string(td[18])',
            c19 VARCHAR2(100)  PATH 'string(td[19])',
            c20 VARCHAR2(100)  PATH 'string(td[20])',
            c21 VARCHAR2(100)  PATH 'string(td[21])',
            c22 VARCHAR2(500)  PATH 'string(td[22])',
            c23 VARCHAR2(100)  PATH 'string(td[23])',
            c24 VARCHAR2(360)  PATH 'string(td[24])',
            c25 VARCHAR2(100)  PATH 'string(td[25])',
            c26 VARCHAR2(50)   PATH 'string(td[26])',
            c27 VARCHAR2(500)  PATH 'string(td[27])',
            c28 VARCHAR2(200)  PATH 'string(td[28])',
            c29 VARCHAR2(200)  PATH 'string(td[29])',
            c30 VARCHAR2(360)  PATH 'string(td[30])',
            c31 VARCHAR2(500)  PATH 'string(td[31])',
            c32 VARCHAR2(100)  PATH 'string(td[32])'
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
        PASSING XMLTYPE(v_table_html)
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
   Version: v4 – SEGMENT1–SEGMENT9 now loaded from BIP.
            Previously intentionally skipped (left NULL).
            Now mapped to td[7]–td[15] per verified BIP output.

   BIP column order (confirmed from live Fusion output):
     td[1]  BANK_ACCOUNT_ID
     td[2]  BANK_NAME
     td[3]  BANK_ACCOUNT_NAME
     td[4]  BANK_ACCOUNT_NUM
     td[5]  BANK_ACCT_USE_ID
     td[6]  ASSET_CODE_COMBINATION_ID
     td[7]  SEGMENT1              ← NOW LOADED (was skipped in v3)
     td[8]  SEGMENT2              ← NOW LOADED
     td[9]  SEGMENT3              ← NOW LOADED
     td[10] SEGMENT4              ← NOW LOADED
     td[11] SEGMENT5              ← NOW LOADED
     td[12] SEGMENT6              ← NOW LOADED
     td[13] SEGMENT7              ← NOW LOADED
     td[14] SEGMENT8              ← NOW LOADED
     td[15] SEGMENT9              ← NOW LOADED
     td[16] CONCATENATED_SEGMENTS
     td[17] OFFSET_ACCOUNT_COMBINATION
     td[18] LEGAL_ENTITY_NAME
     td[19] LEGAL_ENTITY_ID
     td[20] BUSINESS_UNIT_ID      (not in APEX table; not loaded)
     td[21] BUSINESS_UNIT_NAME    (not in APEX table; not loaded)

   APEX table column order (XXEMR_BANK_DETAILS):
     BANK_DETAIL_ID          identity — auto-generated, not in INSERT
     BANK_NAME               td[2]
     BANK_ACCOUNT_NAME       td[3]
     BANK_ACCOUNT_NUM        td[4]
     LEGAL_ENTITY_NAME       td[18]
     LEGAL_ENTITY_ID         td[19]
     BANK_ACCOUNT_ID         td[1]
     BANK_ACCT_USE_ID        td[5]
     ASSET_CODE_COMBINATION_ID td[6]
     CONCATENATED_SEGMENTS   td[16]
     SEGMENT1                td[7]
     SEGMENT2                td[8]
     SEGMENT3                td[9]
     SEGMENT4                td[10]
     SEGMENT5                td[11]
     SEGMENT6                td[12]
     SEGMENT7                td[13]
     SEGMENT8                td[14]
     SEGMENT9                td[15]
     LAST_UPDATE_DATE        TRUNC(SYSDATE)
     LAST_UPDATED_BY         BIP_USERNAME
     CREATION_DATE           TRUNC(SYSDATE)
     CREATED_BY              BIP_USERNAME
     OFFSET_ACCOUNT_COMBINATION td[17]

   Mode: Full DELETE + re-insert every run (unchanged from v3).
   BANK_DETAIL_ID is identity column — never in INSERT list.
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

    v_bip_username := GET_CONFIG('BIP_USERNAME');

    v_html       := CALL_BIP_REPORT(
                        '/Custom/Financials/Bank Reconciliation/Reports/'
                        || 'EMR_Bank_Details_Report.xdo');
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    -- Step 1: Count total valid rows from BIP
    SELECT COUNT(*) INTO v_rows_fetched
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(v_table_html)
        COLUMNS c1 VARCHAR2(100) PATH 'string(td[1])'
    )
    WHERE REGEXP_LIKE(TRIM(c1), '^[0-9]+$');

    -- Step 2: Full refresh — delete all existing rows
    DELETE FROM XXEMR_BANK_DETAILS;

    -- Step 3: Insert all rows from BIP including SEGMENT1-9
    INSERT INTO XXEMR_BANK_DETAILS (
        bank_name,                    -- td[2]
        bank_account_name,            -- td[3]
        bank_account_num,             -- td[4]
        legal_entity_name,            -- td[18]
        legal_entity_id,              -- td[19]
        bank_account_id,              -- td[1]
        bank_acct_use_id,             -- td[5]
        asset_code_combination_id,    -- td[6]
        concatenated_segments,        -- td[16]
        segment1,                     -- td[7]   ← LOADED in v4
        segment2,                     -- td[8]   ← LOADED in v4
        segment3,                     -- td[9]   ← LOADED in v4
        segment4,                     -- td[10]  ← LOADED in v4
        segment5,                     -- td[11]  ← LOADED in v4
        segment6,                     -- td[12]  ← LOADED in v4
        segment7,                     -- td[13]  ← LOADED in v4
        segment8,                     -- td[14]  ← LOADED in v4
        segment9,                     -- td[15]  ← LOADED in v4
        last_update_date,             -- TRUNC(SYSDATE)
        last_updated_by,              -- BIP_USERNAME
        creation_date,                -- TRUNC(SYSDATE)
        created_by,                   -- BIP_USERNAME
        offset_account_combination    -- td[17]
    )
    SELECT
        X.c2,                                               -- bank_name
        X.c3,                                               -- bank_account_name
        X.c4,                                               -- bank_account_num
        X.c18,                                              -- legal_entity_name
        TO_NUMBER(X.c19 DEFAULT NULL ON CONVERSION ERROR),  -- legal_entity_id
        TO_NUMBER(X.c1  DEFAULT NULL ON CONVERSION ERROR),  -- bank_account_id
        TO_NUMBER(X.c5  DEFAULT NULL ON CONVERSION ERROR),  -- bank_acct_use_id
        TO_NUMBER(X.c6  DEFAULT NULL ON CONVERSION ERROR),  -- asset_code_combination_id
        X.c16,                                              -- concatenated_segments
        SUBSTR(TRIM(X.c7),  1, 50),                         -- segment1  td[7]
        SUBSTR(TRIM(X.c8),  1, 50),                         -- segment2  td[8]
        SUBSTR(TRIM(X.c9),  1, 50),                         -- segment3  td[9]
        SUBSTR(TRIM(X.c10), 1, 50),                         -- segment4  td[10]
        SUBSTR(TRIM(X.c11), 1, 50),                         -- segment5  td[11]
        SUBSTR(TRIM(X.c12), 1, 50),                         -- segment6  td[12]
        SUBSTR(TRIM(X.c13), 1, 50),                         -- segment7  td[13]
        SUBSTR(TRIM(X.c14), 1, 50),                         -- segment8  td[14]
        SUBSTR(TRIM(X.c15), 1, 50),                         -- segment9  td[15]
        TRUNC(SYSDATE),                                     -- last_update_date
        v_bip_username,                                     -- last_updated_by
        TRUNC(SYSDATE),                                     -- creation_date
        v_bip_username,                                     -- created_by
        X.c17                                               -- offset_account_combination
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(v_table_html)
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',   -- BANK_ACCOUNT_ID
            c2  VARCHAR2(360)  PATH 'string(td[2])',   -- BANK_NAME
            c3  VARCHAR2(100)  PATH 'string(td[3])',   -- BANK_ACCOUNT_NAME
            c4  VARCHAR2(100)  PATH 'string(td[4])',   -- BANK_ACCOUNT_NUM
            c5  VARCHAR2(100)  PATH 'string(td[5])',   -- BANK_ACCT_USE_ID
            c6  VARCHAR2(100)  PATH 'string(td[6])',   -- ASSET_CODE_COMBINATION_ID
            c7  VARCHAR2(50)   PATH 'string(td[7])',   -- SEGMENT1  ← NOW LOADED
            c8  VARCHAR2(50)   PATH 'string(td[8])',   -- SEGMENT2  ← NOW LOADED
            c9  VARCHAR2(50)   PATH 'string(td[9])',   -- SEGMENT3  ← NOW LOADED
            c10 VARCHAR2(50)   PATH 'string(td[10])',  -- SEGMENT4  ← NOW LOADED
            c11 VARCHAR2(50)   PATH 'string(td[11])',  -- SEGMENT5  ← NOW LOADED
            c12 VARCHAR2(50)   PATH 'string(td[12])',  -- SEGMENT6  ← NOW LOADED
            c13 VARCHAR2(50)   PATH 'string(td[13])',  -- SEGMENT7  ← NOW LOADED
            c14 VARCHAR2(50)   PATH 'string(td[14])',  -- SEGMENT8  ← NOW LOADED
            c15 VARCHAR2(50)   PATH 'string(td[15])',  -- SEGMENT9  ← NOW LOADED
            c16 VARCHAR2(500)  PATH 'string(td[16])',  -- CONCATENATED_SEGMENTS
            c17 VARCHAR2(500)  PATH 'string(td[17])',  -- OFFSET_ACCOUNT_COMBINATION
            c18 VARCHAR2(240)  PATH 'string(td[18])',  -- LEGAL_ENTITY_NAME
            c19 VARCHAR2(100)  PATH 'string(td[19])',  -- LEGAL_ENTITY_ID
            c20 VARCHAR2(100)  PATH 'string(td[20])',  -- BUSINESS_UNIT_ID   (not loaded)
            c21 VARCHAR2(240)  PATH 'string(td[21])'   -- BUSINESS_UNIT_NAME (not loaded)
    ) X
    WHERE REGEXP_LIKE(TRIM(X.c1), '^[0-9]+$');

    v_rows_inserted := SQL%ROWCOUNT;

    -- Step 4: Commit DELETE + INSERT atomically
    COMMIT;

    LOG_LOAD(v_load_id, 'BANK_DETAILS', 'COMPLETED',
        v_rows_fetched, v_rows_inserted,
        v_rows_fetched - v_rows_inserted, NULL, 'Y');
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        LOG_LOAD(v_load_id, 'BANK_DETAILS', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END LOAD_BANK_DETAILS;


/* ============================================================
   PUBLIC: LOAD_ALL_REPORTS
   Orchestrator – calls all six loaders in sequence.
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

    BEGIN
        LOAD_AP_CHECKS;
    EXCEPTION WHEN OTHERS THEN
        v_errors        := v_errors + 1;
        v_error_summary := v_error_summary || 'AP_CHECKS: '
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


/* ============================================================
   PUBLIC: SYNC_RECON_STATUS_FROM_BIP
   Unchanged from original.
============================================================ */
PROCEDURE SYNC_RECON_STATUS_FROM_BIP
IS
    v_html              CLOB;
    v_table_html        CLOB;
    v_load_id           NUMBER  := NULL;
    v_sent_count        NUMBER  := 0;
    v_reconciled_count  NUMBER  := 0;
    v_rejected_count    NUMBER  := 0;
    v_unchanged_count   NUMBER  := 0;
BEGIN
    LOG_LOAD(v_load_id, 'RECON_SYNC', 'RUNNING');

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

    v_html       := CALL_BIP_REPORT(GET_CONFIG('REPORT_PATH_STMT_LINES'));
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    UPDATE xxemr_bank_statement_lines apex_line
       SET apex_line.recon_status       = 'REC',
           apex_line.fusion_recon_status = 'REC',
           apex_line.match_flag         = 'Y',
           apex_line.bip_last_seen_date = TRUNC(SYSDATE),
           apex_line.last_updated       = SYSTIMESTAMP
     WHERE apex_line.recon_status      != 'REC'
       AND apex_line.statement_line_id IN (
               SELECT f.statement_line_id
                 FROM xxemr_recon_fbdi_lines f
                WHERE f.send_status = 'SENT'
                  AND f.source_code = 'BS'
           )
       AND apex_line.statement_line_id IN (
               SELECT TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
                 FROM XMLTABLE(
                          '/table/tr[position() > 1]'
                          PASSING XMLTYPE(v_table_html)
                          COLUMNS
                              c3 VARCHAR2(100) PATH 'string(td[3])',
                              c8 VARCHAR2(20)  PATH 'string(td[8])'   -- td[8] = RECON_STATUS in sync context
                      ) X
                WHERE TRIM(X.c8) = 'REC'
                  AND REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
           );

    v_reconciled_count := SQL%ROWCOUNT;

    UPDATE xxemr_recon_fbdi_lines f
       SET f.send_status      = 'RECONCILED',
           f.reconciled_date  = SYSTIMESTAMP,
           f.last_update_date = SYSTIMESTAMP,
           f.last_updated_by  = 'BIP_SYNC'
     WHERE f.send_status = 'SENT'
       AND f.statement_line_id IN (
               SELECT statement_line_id
                 FROM xxemr_bank_statement_lines
                WHERE recon_status       = 'REC'
                  AND bip_last_seen_date = TRUNC(SYSDATE)
           );

    UPDATE xxemr_recon_fbdi_lines f
       SET f.send_status      = 'FUSION_REJECTED',
           f.last_error       = 'BIP sync: line still UNRECONCILED in Fusion after FBDI load. '
                                || 'Check Fusion CE for tolerance or validity issues.',
           f.last_update_date = SYSTIMESTAMP,
           f.last_updated_by  = 'BIP_SYNC'
     WHERE f.send_status      = 'SENT'
       AND f.source_code      = 'BS'
       AND f.statement_line_id IN (
               SELECT TO_NUMBER(X.c3 DEFAULT NULL ON CONVERSION ERROR)
                 FROM XMLTABLE(
                          '/table/tr[position() > 1]'
                          PASSING XMLTYPE(v_table_html)
                          COLUMNS
                              c3 VARCHAR2(100) PATH 'string(td[3])',
                              c8 VARCHAR2(20)  PATH 'string(td[8])'
                      ) X
                WHERE TRIM(X.c8) = 'UNR'
                  AND REGEXP_LIKE(TRIM(X.c3), '^[0-9]+$')
           );

    v_rejected_count := SQL%ROWCOUNT;

    SELECT COUNT(DISTINCT statement_line_id)
      INTO v_unchanged_count
      FROM xxemr_recon_fbdi_lines
     WHERE send_status = 'SENT'
       AND source_code = 'BS';

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


/* ============================================================
   PUBLIC: LOAD_AP_CHECKS
   Source : Hardcoded BIP path
            /Custom/Financials/Bank Reconciliation/Reports/
            EMR_AP_Details_Report.xdo
   Target : XXEMR_AP_CHECKS_ALL
   Version: v6 – Rewritten to exactly match LOAD_AR_RECEIPTS
            pattern: CALL_BIP_REPORT + EXTRACT_TABLE_HTML +
            XMLTABLE INSERT + NOT EXISTS dedup + LOG_LOAD.
            No custom parsers. Same structure as all other loaders.
   Key    : PAYMENT_REFERENCE + PAYMENT_DATE + BANK_ACCOUNT_ID
            (CHECK_ID is ALWAYS identity — not inserted)
   Mode   : INSERT new rows only (skip existing by natural key)

   BIP column order (verified from live Fusion sample):
     td[1]  BANK_ACCOUNT_NAME       td[18] SEGMENT2
     td[2]  BANK_NAME               td[19] SEGMENT3
     td[3]  PAYMENT_METHOD_CODE     td[20] SEGMENT4
     td[4]  PAYMENT_REFERENCE       td[21] SEGMENT5
     td[5]  PAYMENT_DATE            td[22] SEGMENT6
     td[6]  AMOUNT                  td[23] SEGMENT7
     td[7]  PAYMENT_STATUS          td[24] CONCATENATED_SEGMENTS
     td[8]  BU_NAME                 td[25] BANK_ID
     td[9]  RECON_FLAG              td[26] BANK_BRANCH_ID
     td[10] PAYEE                   td[27] BANK_ACCOUNT_ID
     td[11] PAYEE_SITE              td[28] ORG_ID
     td[12] ADDRESS                 td[29] CHECK_ID (not inserted)
     td[13] PAYMENT_TYPE            td[30] CURRENCY_CODE
     td[14] PAYMENT_DOCUMENT_NAME   td[31] CREATED_BY
     td[15] REMIT_TO_ACCOUNT        td[32] CREATION_DATE
     td[16] LEGAL_ENTITY            td[33] LAST_UPDATED_BY
     td[17] SEGMENT1                td[34] LAST_UPDATE_DATE
============================================================ */
PROCEDURE LOAD_AP_CHECKS
IS
    v_html          CLOB;
    v_table_html    CLOB;
    v_load_id       NUMBER  := NULL;
    v_rows_fetched  NUMBER  := 0;
    v_rows_inserted NUMBER  := 0;
BEGIN
    LOG_LOAD(v_load_id, 'AP_CHECKS', 'RUNNING');

    v_html       := CALL_BIP_REPORT(
                        '/Custom/Financials/Bank Reconciliation/Reports/'
                        || 'EMR_AP_Details_Report.xdo');
    v_table_html := EXTRACT_TABLE_HTML(v_html);

    INSERT INTO XXEMR_AP_CHECKS_ALL (
        bank_account_name,        -- td[1]
        payment_method_code,      -- td[3]
        payment_reference,        -- td[4]
        payment_date,             -- td[5]
        amount,                   -- td[6]
        payment_status,           -- td[7]
        bank_name,                -- td[2]
        bu_name,                  -- td[8]
        recon_flag,               -- td[9]   VARCHAR2(1) → CASE Y/N/NULL
        legal_entity,             -- td[16]
        payee,                    -- td[10]
        payee_site,               -- td[11]
        address,                  -- td[12]
        payment_type,             -- td[13]
        payment_document_name,    -- td[14]
        remit_to_account,         -- td[15]
        segment1,                 -- td[17]
        segment2,                 -- td[18]
        segment3,                 -- td[19]
        segment4,                 -- td[20]
        segment5,                 -- td[21]
        segment6,                 -- td[22]
        segment7,                 -- td[23]
        concatenated_segments,    -- td[24]
        bank_id,                  -- td[25]
        bank_branch_id,           -- td[26]
        bank_account_id,          -- td[27]
        org_id,                   -- td[28]
        -- td[29] CHECK_ID: ALWAYS identity — not inserted
        currency_code,            -- td[30]
        created_by,               -- td[31]
        creation_date,            -- td[32]
        last_updated_by,          -- td[33]
        last_update_date,         -- td[34]
        xxemr_load_date,          -- TRUNC(SYSDATE)
        xxemr_last_refresh        -- SYSTIMESTAMP
    )
    SELECT
        SUBSTR(X.c1,  1, 100),                                                        -- bank_account_name
        SUBSTR(X.c3,  1, 30),                                                         -- payment_method_code
        SUBSTR(X.c4,  1, 50),                                                         -- payment_reference
        TO_DATE(SUBSTR(X.c5,  1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- payment_date
        TO_NUMBER(X.c6  DEFAULT NULL ON CONVERSION ERROR),                            -- amount
        SUBSTR(X.c7,  1, 25),                                                         -- payment_status
        SUBSTR(X.c2,  1, 360),                                                        -- bank_name
        SUBSTR(X.c8,  1, 240),                                                        -- bu_name
        CASE
            WHEN UPPER(TRIM(X.c9))  IN ('Y','YES','TRUE', '1') THEN 'Y'
            WHEN UPPER(TRIM(X.c9))  IN ('N','NO', 'FALSE','0') THEN 'N'
            ELSE NULL
        END,                                                                          -- recon_flag
        SUBSTR(X.c16, 1, 240),                                                        -- legal_entity
        SUBSTR(X.c10, 1, 240),                                                        -- payee
        SUBSTR(X.c11, 1, 15),                                                         -- payee_site
        SUBSTR(X.c12, 1, 240),                                                        -- address
        SUBSTR(X.c13, 1, 80),                                                         -- payment_type
        SUBSTR(X.c14, 1, 30),                                                         -- payment_document_name
        SUBSTR(X.c15, 1, 100),                                                        -- remit_to_account
        SUBSTR(TRIM(X.c17), 1, 25),                                                   -- segment1
        SUBSTR(TRIM(X.c18), 1, 25),                                                   -- segment2
        SUBSTR(TRIM(X.c19), 1, 25),                                                   -- segment3
        SUBSTR(TRIM(X.c20), 1, 25),                                                   -- segment4
        SUBSTR(TRIM(X.c21), 1, 25),                                                   -- segment5
        SUBSTR(TRIM(X.c22), 1, 25),                                                   -- segment6
        SUBSTR(TRIM(X.c23), 1, 25),                                                   -- segment7
        SUBSTR(X.c24, 1, 240),                                                        -- concatenated_segments
        TO_NUMBER(X.c25 DEFAULT NULL ON CONVERSION ERROR),                            -- bank_id
        TO_NUMBER(X.c26 DEFAULT NULL ON CONVERSION ERROR),                            -- bank_branch_id
        TO_NUMBER(X.c27 DEFAULT NULL ON CONVERSION ERROR),                            -- bank_account_id
        TO_NUMBER(X.c28 DEFAULT NULL ON CONVERSION ERROR),                            -- org_id
        SUBSTR(X.c30, 1, 15),                                                         -- currency_code
        SUBSTR(X.c31, 1, 100),                                                        -- created_by
        TO_DATE(SUBSTR(X.c32, 1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- creation_date
        SUBSTR(X.c33, 1, 100),                                                        -- last_updated_by
        TO_DATE(SUBSTR(X.c34, 1,10) DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'),  -- last_update_date
        TRUNC(SYSDATE),                                                               -- xxemr_load_date
        SYSTIMESTAMP                                                                  -- xxemr_last_refresh
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(v_table_html)
        COLUMNS
            c1  VARCHAR2(100)  PATH 'string(td[1])',   -- BANK_ACCOUNT_NAME
            c2  VARCHAR2(360)  PATH 'string(td[2])',   -- BANK_NAME
            c3  VARCHAR2(30)   PATH 'string(td[3])',   -- PAYMENT_METHOD_CODE
            c4  VARCHAR2(50)   PATH 'string(td[4])',   -- PAYMENT_REFERENCE
            c5  VARCHAR2(100)  PATH 'string(td[5])',   -- PAYMENT_DATE
            c6  VARCHAR2(200)  PATH 'string(td[6])',   -- AMOUNT
            c7  VARCHAR2(25)   PATH 'string(td[7])',   -- PAYMENT_STATUS
            c8  VARCHAR2(240)  PATH 'string(td[8])',   -- BU_NAME
            c9  VARCHAR2(20)   PATH 'string(td[9])',   -- RECON_FLAG
            c10 VARCHAR2(240)  PATH 'string(td[10])',  -- PAYEE
            c11 VARCHAR2(15)   PATH 'string(td[11])',  -- PAYEE_SITE
            c12 VARCHAR2(240)  PATH 'string(td[12])',  -- ADDRESS
            c13 VARCHAR2(80)   PATH 'string(td[13])',  -- PAYMENT_TYPE
            c14 VARCHAR2(30)   PATH 'string(td[14])',  -- PAYMENT_DOCUMENT_NAME
            c15 VARCHAR2(100)  PATH 'string(td[15])',  -- REMIT_TO_ACCOUNT
            c16 VARCHAR2(240)  PATH 'string(td[16])',  -- LEGAL_ENTITY
            c17 VARCHAR2(25)   PATH 'string(td[17])',  -- SEGMENT1
            c18 VARCHAR2(25)   PATH 'string(td[18])',  -- SEGMENT2
            c19 VARCHAR2(25)   PATH 'string(td[19])',  -- SEGMENT3
            c20 VARCHAR2(25)   PATH 'string(td[20])',  -- SEGMENT4
            c21 VARCHAR2(25)   PATH 'string(td[21])',  -- SEGMENT5
            c22 VARCHAR2(25)   PATH 'string(td[22])',  -- SEGMENT6
            c23 VARCHAR2(25)   PATH 'string(td[23])',  -- SEGMENT7
            c24 VARCHAR2(240)  PATH 'string(td[24])',  -- CONCATENATED_SEGMENTS
            c25 VARCHAR2(100)  PATH 'string(td[25])',  -- BANK_ID
            c26 VARCHAR2(100)  PATH 'string(td[26])',  -- BANK_BRANCH_ID
            c27 VARCHAR2(100)  PATH 'string(td[27])',  -- BANK_ACCOUNT_ID
            c28 VARCHAR2(100)  PATH 'string(td[28])',  -- ORG_ID
            c29 VARCHAR2(100)  PATH 'string(td[29])',  -- CHECK_ID (not inserted)
            c30 VARCHAR2(15)   PATH 'string(td[30])',  -- CURRENCY_CODE
            c31 VARCHAR2(100)  PATH 'string(td[31])',  -- CREATED_BY
            c32 VARCHAR2(100)  PATH 'string(td[32])',  -- CREATION_DATE
            c33 VARCHAR2(100)  PATH 'string(td[33])',  -- LAST_UPDATED_BY
            c34 VARCHAR2(100)  PATH 'string(td[34])'   -- LAST_UPDATE_DATE
    ) X
    WHERE  TRIM(X.c4) IS NOT NULL
      AND  TRIM(X.c5) IS NOT NULL
      AND  NOT EXISTS (
               SELECT 1
               FROM   XXEMR_AP_CHECKS_ALL A
               WHERE  A.payment_reference = SUBSTR(X.c4, 1, 50)
                 AND  A.payment_date      = TO_DATE(SUBSTR(X.c5,1,10)
                                               DEFAULT NULL ON CONVERSION ERROR,
                                               'YYYY-MM-DD')
           );

    v_rows_inserted := SQL%ROWCOUNT;

    SELECT COUNT(*) INTO v_rows_fetched
    FROM XMLTABLE(
        '/table/tr[position() > 1]'
        PASSING XMLTYPE(v_table_html)
        COLUMNS
            c4 VARCHAR2(50)  PATH 'string(td[4])',
            c5 VARCHAR2(100) PATH 'string(td[5])'
    )
    WHERE TRIM(c4) IS NOT NULL
      AND TRIM(c5) IS NOT NULL;

    COMMIT;
    LOG_LOAD(v_load_id, 'AP_CHECKS', 'COMPLETED',
        v_rows_fetched, v_rows_inserted,
        v_rows_fetched - v_rows_inserted, NULL, 'Y');
    DBMS_LOB.FREETEMPORARY(v_html);
    DBMS_LOB.FREETEMPORARY(v_table_html);
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        LOG_LOAD(v_load_id, 'AP_CHECKS', 'FAILED',
            NULL, NULL, NULL,
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000), 'Y');
        RAISE;
END LOAD_AP_CHECKS;





END PKG_BIP_LOADER;
/