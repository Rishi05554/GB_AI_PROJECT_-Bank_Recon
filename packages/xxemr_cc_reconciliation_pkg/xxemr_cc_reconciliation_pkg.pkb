CREATE OR REPLACE PACKAGE BODY XXEMR_CC_RECONCILIATION_PKG AS
 
 
 
-- ----------------------------------------------------------------
-- PRIVATE: log_cc
-- ----------------------------------------------------------------
PROCEDURE log_cc (p_msg IN VARCHAR2) IS
BEGIN
    DBMS_OUTPUT.PUT_LINE(
        TO_CHAR(SYSTIMESTAMP,'HH24:MI:SS') || ' | CC | ' || p_msg);
END log_cc;
 
 
-- ================================================================
-- STEP 1: xxemr_resolve_cc_bank
-- Resolves bank_account_id from BANK_ACCOUNT_NUMBER via
-- XXEMR_BANK_DETAILS. Also cleans receipt numbers.
-- Resets match_status to PENDING (idempotent re-run).
-- ================================================================
PROCEDURE xxemr_resolve_cc_bank (
    p_upload_id   IN NUMBER,
    p_created_by  IN VARCHAR2 DEFAULT 'SYSTEM'
)
IS
    l_invalid NUMBER := 0;
BEGIN
    log_cc('xxemr_resolve_cc_bank START. upload_id=' || p_upload_id);
 
    -- Reset for idempotent re-run
    UPDATE xxemr_cc_fund_transfer
       SET bank_account_id = NULL,
           cash_receipt_id = NULL,
           ar_amount       = NULL,
           match_status    = 'PENDING',
           match_error     = NULL
     WHERE upload_id = p_upload_id;
 
    -- Resolve bank_account_id from BANK_ACCOUNT_NUMBER via XXEMR_BANK_DETAILS.
    -- RECEIPT_NUMBER is already correct in the table (scientific notation
    -- is an Excel display issue only — the stored value is the full number).
    UPDATE xxemr_cc_fund_transfer ft
       SET ft.bank_account_id = (
               SELECT d.bank_account_id
                 FROM xxemr_bank_details d
                WHERE REPLACE(d.bank_account_num,' ','')
                      = REPLACE(ft.bank_account_number,' ','')
                  AND ROWNUM = 1
           ),
           ft.match_status = CASE
                                 WHEN ft.amount IS NULL THEN 'INVALID'
                                 ELSE 'PENDING'
                             END,
           ft.match_error  = CASE
                                 WHEN ft.amount IS NULL THEN 'Amount is null'
                                 ELSE NULL
                             END
     WHERE ft.upload_id = p_upload_id;
 
    -- Flag rows where bank_account_id could not be resolved
    UPDATE xxemr_cc_fund_transfer
       SET match_status = 'INVALID',
           match_error  = 'Bank account number not found in XXEMR_BANK_DETAILS: '
                          || bank_account_number
     WHERE upload_id      = p_upload_id
       AND match_status   = 'PENDING'
       AND bank_account_id IS NULL;
 
    SELECT COUNT(*) INTO l_invalid
      FROM xxemr_cc_fund_transfer
     WHERE upload_id   = p_upload_id
       AND match_status = 'INVALID';
 
    COMMIT;
    log_cc('xxemr_resolve_cc_bank DONE. Invalid=' || l_invalid);
 
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        log_cc('FATAL: xxemr_resolve_cc_bank: ' || SQLERRM);
        RAISE;
END xxemr_resolve_cc_bank;
 
 
-- ================================================================
-- STEP 2: xxemr_validate_cc_receipts
-- For each PENDING row: look up receipt_number in
-- XXEMR_AR_CASH_RECEIPTS. Mark MATCHED or UNMATCHED.
-- Populates cash_receipt_id and ar_amount on matched rows.
-- ================================================================
PROCEDURE xxemr_validate_cc_receipts (
    p_upload_id   IN NUMBER,
    p_created_by  IN VARCHAR2 DEFAULT 'SYSTEM'
)
IS
    l_matched   NUMBER;
    l_unmatched NUMBER;
BEGIN
    log_cc('xxemr_validate_cc_receipts START. upload_id=' || p_upload_id);
 
    -- Match receipt_number + amount + bank_account_id against AR receipts.
    -- All three must match to handle duplicate receipt numbers (e.g. MB/VISA/016
    -- which exists across multiple banks/amounts).
    UPDATE xxemr_cc_fund_transfer ft
       SET (ft.cash_receipt_id,
            ft.ar_amount,
            ft.match_status,
            ft.match_error) = (
               SELECT r.cash_receipt_id,
                      r.amount,
                      'MATCHED',
                      NULL
                 FROM xxemr_ar_cash_receipts r
                WHERE r.receipt_number             = ft.receipt_number
                  AND r.amount                     = ft.amount
                  AND r.remittance_bank_account_id = ft.bank_account_id
                  AND ROWNUM = 1
           )
     WHERE ft.upload_id    = p_upload_id
       AND ft.match_status = 'PENDING';
 
    -- Still PENDING after update = receipt not found → UNMATCHED
    UPDATE xxemr_cc_fund_transfer
       SET match_status = 'UNMATCHED',
           match_error  = 'Receipt not found in XXEMR_AR_CASH_RECEIPTS: '
                          || receipt_number
     WHERE upload_id    = p_upload_id
       AND match_status = 'PENDING';
 
    SELECT COUNT(*) INTO l_matched
      FROM xxemr_cc_fund_transfer
     WHERE upload_id = p_upload_id AND match_status = 'MATCHED';
 
    SELECT COUNT(*) INTO l_unmatched
      FROM xxemr_cc_fund_transfer
     WHERE upload_id = p_upload_id AND match_status = 'UNMATCHED';
 
    COMMIT;
    log_cc('xxemr_validate_cc_receipts DONE.'
        || ' Matched='   || l_matched
        || ' Unmatched=' || l_unmatched
        || ' (unmatched rows flagged, proceeding with matched only)');
 
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        log_cc('FATAL: xxemr_validate_cc_receipts: ' || SQLERRM);
        RAISE;
END xxemr_validate_cc_receipts;
 
 
-- ================================================================
-- STEP 3: xxemr_match_cc_statement
-- Groups MATCHED rows by bank_account_id.
-- SUM(ar_amount) → finds CC statement line with that exact amount.
-- Inserts one row into XXEMR_CC_MATCH_GROUPS per bank group.
-- ================================================================
PROCEDURE xxemr_match_cc_statement (
    p_upload_id   IN NUMBER,
    p_created_by  IN VARCHAR2 DEFAULT 'SYSTEM'
)
IS
    l_statement_line_id NUMBER;
    l_match_group_id    NUMBER;
BEGIN
    log_cc('xxemr_match_cc_statement START. upload_id=' || p_upload_id);
 
    -- Clear existing match groups for this upload (idempotent)
    DELETE FROM xxemr_cc_match_groups
     WHERE upload_id = p_upload_id;
 
    -- One group per bank_account_id in this upload
    FOR g IN (
        SELECT bank_account_id,
               MAX(bank_account_number)                            AS bank_account_number,
               SUM(CASE WHEN match_status='MATCHED'
                        THEN NVL(ar_amount, amount) ELSE 0 END)   AS matched_sum,
               COUNT(CASE WHEN match_status='MATCHED' THEN 1 END) AS matched_cnt,
               COUNT(CASE WHEN match_status='UNMATCHED' THEN 1 END) AS unmatched_cnt
          FROM xxemr_cc_fund_transfer
         WHERE upload_id      = p_upload_id
           AND match_status  IN ('MATCHED','UNMATCHED')
           AND bank_account_id IS NOT NULL
         GROUP BY bank_account_id
    ) LOOP
 
        -- Find CC statement line: same bank, exact amount, unreconciled,
        -- description contains a CC or fund transfer keyword.
        -- Simple keyword check for now — keywords maintained in
        -- XXEMR_KEYWORD_MAPPING with is_credit_card='Y' or is_fund_transfer='Y'.
        BEGIN
            SELECT statement_line_id
              INTO l_statement_line_id
              FROM xxemr_bank_statement_lines s
             WHERE s.bank_account_id = g.bank_account_id
               AND s.amount          = g.matched_sum
               -- NOTE: recon_status filter removed for testing.
               -- After testing add: AND NVL(s.recon_status,'UNR') = 'UNR'
               AND EXISTS (
                       SELECT 1
                         FROM xxemr_keyword_mapping k
                        WHERE k.enabled_flag = 'Y'
                          AND (k.is_credit_card = 'Y' OR k.is_fund_transfer = 'Y')
                          AND INSTR(UPPER(s.description), UPPER(k.keywords)) > 0
                   )
               AND ROWNUM = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN l_statement_line_id := NULL;
        END;
 
        INSERT INTO xxemr_cc_match_groups (
            cc_match_group_id,
            upload_id,
            bank_account_id,
            bank_account_number,
            statement_line_id,
            statement_amount,
            matched_receipt_count,
            matched_amount,
            unmatched_count,
            match_status,
            match_error,
            created_by,
            creation_date,
            last_updated_by,
            last_update_date
        ) VALUES (
            xxemr_cc_match_grp_seq.NEXTVAL,
            p_upload_id,
            g.bank_account_id,
            g.bank_account_number,
            l_statement_line_id,
            g.matched_sum,
            g.matched_cnt,
            g.matched_sum,
            g.unmatched_cnt,
            CASE
                WHEN l_statement_line_id IS NULL THEN 'STMT_NOT_FOUND'
                WHEN g.unmatched_cnt     > 0     THEN 'AMOUNT_MISMATCH'
                ELSE                                  'AMOUNT_MATCH'
            END,
            CASE
                WHEN l_statement_line_id IS NULL
                THEN 'No unreconciled CC/FT statement line found with amount='
                     || g.matched_sum
                     || ' for bank_account_id=' || g.bank_account_id
                     || '. Check: (1) statement line exists with exact amount, '
                     || '(2) description matches a CC/FT keyword, '
                     || '(3) line is not already reconciled.'
                WHEN g.unmatched_cnt > 0
                THEN g.unmatched_cnt
                     || ' receipt(s) not found in AR — sum may differ from statement'
                ELSE NULL
            END,
            p_created_by, SYSTIMESTAMP, p_created_by, SYSTIMESTAMP
        )
        RETURNING cc_match_group_id INTO l_match_group_id;
 
        log_cc('Bank=' || g.bank_account_id
            || ' | Sum='        || g.matched_sum
            || ' | Matched='    || g.matched_cnt
            || ' | Unmatched='  || g.unmatched_cnt
            || ' | StmtLine='   || NVL(TO_CHAR(l_statement_line_id),'NOT FOUND')
            || ' | Status='     || CASE
                                       WHEN l_statement_line_id IS NULL THEN 'STMT_NOT_FOUND'
                                       WHEN g.unmatched_cnt > 0 THEN 'AMOUNT_MISMATCH'
                                       ELSE 'AMOUNT_MATCH'
                                   END);
    END LOOP;
 
    COMMIT;
    log_cc('xxemr_match_cc_statement DONE.');
 
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        log_cc('FATAL: xxemr_match_cc_statement: ' || SQLERRM);
        RAISE;
END xxemr_match_cc_statement;
 
 
-- ================================================================
-- STEP 4: xxemr_build_cc_fbdi
-- For each AMOUNT_MATCH group builds XXEMR_CC_FBDI_LINES with
-- all payload fields pre-populated so the dispatcher needs
-- zero extra joins when building the OIC JSON.
--
-- 1 BS row  (is_neg_line=Y):
--   source_id       = statement_line_id
--   recon_reference = statement_number  (from header)
--   amount          = statement_amount  (positive — OIC treats as negative)
--   line_number     = 1
--
-- N AR rows (is_neg_line=N):
--   source_id       = cash_receipt_id
--   recon_reference = receipt_number
--   receipt_number  = receipt_number
--   amount          = ar_amount
--   booking_date    = transfer_date
--   line_number     = 2..N
-- ================================================================
PROCEDURE xxemr_build_cc_fbdi (
    p_upload_id   IN NUMBER,
    p_created_by  IN VARCHAR2 DEFAULT 'SYSTEM'
)
IS
    -- Header-level fields fetched once per group
    l_statement_number    VARCHAR2(200);
    l_cc_recon_ref        VARCHAR2(200);  -- unique ref per group: CC-<upload>-<group>-<stmt_num>
    l_reference_num       VARCHAR2(200);  -- original statement reference_num for negLine reconRef
    l_bank_acct_int_id    VARCHAR2(100);
    l_statement_date      DATE;
    l_stmt_from_date      DATE;
    l_stmt_to_date        DATE;
    l_currency_code       VARCHAR2(10);
    l_transaction_code    VARCHAR2(50);
    l_transaction_type    VARCHAR2(100);
    l_line_num            NUMBER;
    l_group_count         NUMBER := 0;
BEGIN
    log_cc('xxemr_build_cc_fbdi START. upload_id=' || p_upload_id);
 
    -- Clear existing FBDI lines for this upload (idempotent)
    DELETE FROM xxemr_cc_fbdi_lines
     WHERE upload_id = p_upload_id;
 
    FOR g IN (
        SELECT mg.cc_match_group_id,
               mg.bank_account_id,
               mg.bank_account_number,
               mg.statement_line_id,
               mg.statement_amount
          FROM xxemr_cc_match_groups mg
         WHERE mg.upload_id    = p_upload_id
           AND mg.match_status = 'AMOUNT_MATCH'
    ) LOOP
 
        -- ── Fetch all header-level payload fields in one query ──
        -- Join: stmt_line → header (for statement_number, dates)
        --       stmt_line → bank_details (for bank_acct_internal_id = numeric PK)
        BEGIN
            SELECT h.statement_number,
                   bd.bank_account_num,   -- Fusion internal ID = bank_account_num
                   h.statement_date,
                   h.stmt_from_date,
                   h.stmt_to_date,
                   s.currency_code,
                   s.trx_code,
                   s.trx_description,  -- use description not trx_type for transactionType field
                   s.reference_num     -- original recon reference from Fusion statement line
              INTO l_statement_number,
                   l_bank_acct_int_id,
                   l_statement_date,
                   l_stmt_from_date,
                   l_stmt_to_date,
                   l_currency_code,
                   l_transaction_code,
                   l_transaction_type,
                   l_reference_num
              FROM xxemr_bank_statement_lines   s
              JOIN xxemr_bank_statement_headers h
                ON h.statement_header_id = s.statement_header_id
              JOIN xxemr_bank_details           bd
                ON bd.bank_account_id    = s.bank_account_id
               AND ROWNUM = 1
             WHERE s.statement_line_id = g.statement_line_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                log_cc('WARNING: header data not found for stmt_line='
                    || g.statement_line_id || '. Skipping group.');
                CONTINUE;
        END;
 
        -- Build unique CC recon reference per group
        -- Format: CC-<upload_id>-<bank_account_num>-<DDMMYYYY>
        -- SYSDATE ensures uniqueness on every run in Fusion CE_EXTERNAL_RECON_INT
        l_cc_recon_ref := 'CC-' || p_upload_id
                          || '-' || g.bank_account_number
                          || '-' || TO_CHAR(SYSDATE, 'DDMMYYYY');
 
        -- ── BS row (negLine) ──
        -- line_number = 1, is_neg_line = Y
        -- recon_reference = l_cc_recon_ref (unique per group, not raw statement_number)
        -- amount stored positive — OIC constructs it as negative in the payload
        INSERT INTO xxemr_cc_fbdi_lines (
            cc_fbdi_line_id,       cc_match_group_id,    upload_id,
            statement_number,      bank_acct_internal_id,
            statement_date,        statement_from_date,  statement_to_date,
            currency_code,         transaction_code,     transaction_type,
            source_code,           source_id,            bank_account_id,
            line_number,           is_neg_line,
            recon_reference,       receipt_number,
            amount,                booking_date,
            send_status,           send_attempts,
            created_by,            creation_date,
            last_updated_by,       last_update_date
        ) VALUES (
            xxemr_cc_fbdi_seq.NEXTVAL, g.cc_match_group_id, p_upload_id,
            l_statement_number,    l_bank_acct_int_id,
            l_statement_date,      l_stmt_from_date,     l_stmt_to_date,
            l_currency_code,       l_transaction_code,   l_transaction_type,
            'BS',                  g.statement_line_id,  g.bank_account_id,
            1,                     'Y',
            l_reference_num,       NULL,  -- original statement reference_num as negLine reconRef
            g.statement_amount,    NULL,
            'PENDING',             0,
            p_created_by,          SYSTIMESTAMP,
            p_created_by,          SYSTIMESTAMP
        );
 
        -- ── AR rows (lines[]) ──
        -- line_number = 2..N, is_neg_line = N
        -- recon_reference = receipt_number (per payload spec)
        -- booking_date    = transfer_date from fund transfer table
        l_line_num := 2;
        FOR r IN (
            SELECT ft.cash_receipt_id,
                   ft.receipt_number,
                   NVL(ft.ar_amount, ft.amount) AS receipt_amount
              FROM xxemr_cc_fund_transfer ft
             WHERE ft.upload_id       = p_upload_id
               AND ft.bank_account_id = g.bank_account_id
               AND ft.match_status    = 'MATCHED'
               AND ft.cash_receipt_id IS NOT NULL
             ORDER BY ft.transfer_id
        ) LOOP
            INSERT INTO xxemr_cc_fbdi_lines (
                cc_fbdi_line_id,       cc_match_group_id,    upload_id,
                statement_number,      bank_acct_internal_id,
                statement_date,        statement_from_date,  statement_to_date,
                currency_code,         transaction_code,     transaction_type,
                source_code,           source_id,            bank_account_id,
                line_number,           is_neg_line,
                recon_reference,       receipt_number,
                amount,                booking_date,
                send_status,           send_attempts,
                created_by,            creation_date,
                last_updated_by,       last_update_date
            ) VALUES (
                xxemr_cc_fbdi_seq.NEXTVAL, g.cc_match_group_id, p_upload_id,
                l_statement_number,    l_bank_acct_int_id,
                l_statement_date,      l_stmt_from_date,     l_stmt_to_date,
                l_currency_code,       l_transaction_code,   l_transaction_type,
                'AR',                  r.cash_receipt_id,    g.bank_account_id,
                l_line_num,            'N',
                r.receipt_number,      r.receipt_number,
                r.receipt_amount,      l_statement_date,  -- use statement_date (must be within statement date range)
                'PENDING',             0,
                p_created_by,          SYSTIMESTAMP,
                p_created_by,          SYSTIMESTAMP
            );
            l_line_num := l_line_num + 1;
        END LOOP;
 
        log_cc('FBDI built: group=' || g.cc_match_group_id
            || ' | stmt_num=' || l_statement_number
            || ' | stmt_line=' || g.statement_line_id
            || ' | receipt_rows=' || (l_line_num - 2));
 
        l_group_count := l_group_count + 1;
    END LOOP;
 
    COMMIT;
    log_cc('xxemr_build_cc_fbdi DONE. Groups built=' || l_group_count);
 
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        log_cc('FATAL: xxemr_build_cc_fbdi: ' || SQLERRM);
        RAISE;
END xxemr_build_cc_fbdi;
 
 
-- ================================================================
-- STEP 5: xxemr_dispatch_cc_to_oic
-- Reads PENDING rows from XXEMR_CC_FBDI_LINES grouped by
-- cc_match_group_id and builds the exact OIC JSON payload:
--
-- {
--   "statementNumber"      : "...",
--   "bankAccountInternalId": "...",
--   "statementDate"        : "MM/DD/YYYY",
--   "statementFromDate"    : "MM/DD/YYYY",
--   "statementToDate"      : "MM/DD/YYYY",
--   "currencyCode"         : "AED",
--   "transactionCode"      : "...",
--   "transactionType"      : "...",
--   "negLine" : { "lineNumber":1, "reconRef":"...", "amount":50000 },
--   "lines"   : [ { "lineNumber":2, "reconRef":"...",
--                   "receiptNumber":"...", "amount":10000,
--                   "bookingDate":"MM/DD/YYYY" }, ... ]
-- }
--
-- One JSON object per cc_match_group_id (one per bank group).
-- Sent individually — no batching across groups since each
-- has its own statementNumber header.
-- ================================================================
PROCEDURE xxemr_dispatch_cc_to_oic (
    p_upload_id    IN NUMBER   DEFAULT NULL,
    p_batch_size   IN NUMBER   DEFAULT 100,
    p_max_attempts IN NUMBER   DEFAULT 2
)
IS
    v_endpoint      VARCHAR2(500);
    v_username      VARCHAR2(200);
    v_password      VARCHAR2(200);
    l_payload       CLOB;
    l_api_response  VARCHAR2(4000);
    l_api_status    NUMBER;
    l_sent_count    NUMBER := 0;
    l_failed_count  NUMBER := 0;
    l_pending_count NUMBER := 0;
    l_first_line    BOOLEAN;
 
    TYPE t_num_list IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    l_id_list  t_num_list;
    l_id_count PLS_INTEGER := 0;
 
    FUNCTION fmt_date (p_date IN DATE) RETURN VARCHAR2 IS
    BEGIN
        RETURN TO_CHAR(p_date, 'MM/DD/YYYY');
    END;
 
    FUNCTION json_str (p_val IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN REPLACE(REPLACE(NVL(p_val,''),'\\','\\\\'),'"','\\"');
    END;
 
BEGIN
    log_cc('xxemr_dispatch_cc_to_oic START. upload_id='
        || NVL(TO_CHAR(p_upload_id),'ALL'));
 
    SELECT COUNT(DISTINCT cc_match_group_id)
      INTO l_pending_count
      FROM xxemr_cc_fbdi_lines
     WHERE send_status   IN ('PENDING','FAILED')
       AND send_attempts  < p_max_attempts
       AND (p_upload_id IS NULL OR upload_id = p_upload_id);
 
    IF l_pending_count = 0 THEN
        log_cc('No PENDING CC FBDI rows. Exiting.');
        RETURN;
    END IF;
 
    log_cc('Groups to dispatch: ' || l_pending_count);
 
    SELECT config_value INTO v_endpoint FROM apex_recon_config WHERE config_key = 'OIC_CC_ENDPOINT';
    SELECT config_value INTO v_username FROM apex_recon_config WHERE config_key = 'OIC_API_USERNAME';
    SELECT config_value INTO v_password FROM apex_recon_config WHERE config_key = 'OIC_API_PASSWORD';
 
    -- ── One API call per cc_match_group_id ──
    FOR g IN (
        SELECT DISTINCT cc_match_group_id
          FROM xxemr_cc_fbdi_lines
         WHERE send_status   IN ('PENDING','FAILED')
           AND send_attempts  < p_max_attempts
           AND (p_upload_id IS NULL OR upload_id = p_upload_id)
         ORDER BY cc_match_group_id
    ) LOOP
 
        l_id_count := 0;
        l_id_list.DELETE;
        DBMS_LOB.CREATETEMPORARY(l_payload, TRUE);
 
        -- ── Fetch the negLine row (is_neg_line = Y) ──
        DECLARE
            l_neg   xxemr_cc_fbdi_lines%ROWTYPE;
        BEGIN
            SELECT * INTO l_neg
              FROM xxemr_cc_fbdi_lines
             WHERE cc_match_group_id = g.cc_match_group_id
               AND is_neg_line       = 'Y'
               AND ROWNUM = 1;
 
            -- Build top-level header + negLine
            DBMS_LOB.APPEND(l_payload, TO_CLOB(
                '{' ||
                '"statementNumber":"'       || json_str(l_neg.recon_reference)       || '",' ||
                '"bankAccountInternalId":"'  || json_str(l_neg.bank_acct_internal_id) || '",' ||
                '"statementDate":"'          || fmt_date(l_neg.statement_date)         || '",' ||
                '"statementFromDate":"'      || fmt_date(l_neg.statement_from_date)    || '",' ||
                '"statementToDate":"'        || fmt_date(l_neg.statement_to_date)      || '",' ||
                '"currencyCode":"'           || json_str(l_neg.currency_code)          || '",' ||
                '"transactionCode":"'        || json_str(l_neg.transaction_code)       || '",' ||
                '"transactionType":"'        || json_str(l_neg.transaction_type)       || '",' ||
                '"negLine":{' ||
                    '"lineNumber":1,' ||
                    '"reconRef":"'   || json_str(l_neg.recon_reference) || '",' ||
                    '"amount":'       || TO_CHAR(l_neg.amount) ||
                '},' ||
                '"lines":[' ));
 
            -- Track negLine ID for status update
            l_id_count            := l_id_count + 1;
            l_id_list(l_id_count) := l_neg.cc_fbdi_line_id;
 
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                log_cc('WARNING: no negLine for group=' || g.cc_match_group_id || '. Skipping.');
                IF DBMS_LOB.ISTEMPORARY(l_payload) = 1 THEN
                    DBMS_LOB.FREETEMPORARY(l_payload);
                END IF;
                CONTINUE;
        END;
 
        -- ── Build lines[] array (AR rows, is_neg_line = N) ──
        l_first_line := TRUE;
        FOR r IN (
            SELECT cc_fbdi_line_id,
                   line_number,
                   recon_reference,
                   receipt_number,
                   amount,
                   booking_date
              FROM xxemr_cc_fbdi_lines
             WHERE cc_match_group_id = g.cc_match_group_id
               AND is_neg_line       = 'N'
               AND send_status      IN ('PENDING','FAILED')
             ORDER BY line_number
        ) LOOP
            IF NOT l_first_line THEN
                DBMS_LOB.APPEND(l_payload, TO_CLOB(','));
            END IF;
            l_first_line := FALSE;
 
            DBMS_LOB.APPEND(l_payload, TO_CLOB(
                '{' ||
                '"lineNumber":'      || TO_CHAR(r.line_number)            || ',' ||
                '"reconRef":"'       || json_str(r.recon_reference)       || '",' ||
                '"receiptNumber":"'  || json_str(r.receipt_number)        || '",' ||
                '"amount":'          || TO_CHAR(r.amount)                  || ',' ||
                '"bookingDate":"'    || fmt_date(r.booking_date)           || '"}' ));
 
            l_id_count            := l_id_count + 1;
            l_id_list(l_id_count) := r.cc_fbdi_line_id;
        END LOOP;
 
        -- Close lines[] and root object
        DBMS_LOB.APPEND(l_payload, TO_CLOB(']}'));
 
        log_cc('Dispatching group=' || g.cc_match_group_id
            || ' | lines=' || l_id_count);
 
        -- ── POST to OIC ──
        BEGIN
            apex_web_service.g_request_headers.DELETE;
            apex_web_service.g_request_headers(1).name  := 'Content-Type';
            apex_web_service.g_request_headers(1).value := 'application/json';
 
            l_api_response := SUBSTR(
                apex_web_service.make_rest_request(
                    p_url         => v_endpoint,
                    p_http_method => 'POST',
                    p_username    => v_username,
                    p_password    => v_password,
                    p_body        => l_payload
                ), 1, 4000);
            l_api_status := apex_web_service.g_status_code;
        EXCEPTION
            WHEN OTHERS THEN
                l_api_status   := -1;
                l_api_response := SUBSTR('REST exception: ' || SQLERRM, 1, 4000);
        END;
 
        log_cc('HTTP ' || NVL(TO_CHAR(l_api_status),'NULL'));
 
        -- ── Update send status ──
        IF l_api_status IN (200, 201, 202) THEN
            FORALL i IN 1..l_id_count
                UPDATE xxemr_cc_fbdi_lines
                   SET send_status      = 'SENT',
                       send_attempts    = send_attempts + 1,
                       sent_date        = SYSTIMESTAMP,
                       oic_response     = l_api_response,
                       last_error       = NULL,
                       last_updated_by  = 'CC_DISPATCHER',
                       last_update_date = SYSTIMESTAMP
                 WHERE cc_fbdi_line_id  = l_id_list(i);
 
            UPDATE xxemr_cc_match_groups
               SET match_status        = 'SUBMITTED',
                   fbdi_submitted_flag = 'Y',
                   fbdi_submitted_date = SYSTIMESTAMP,
                   last_updated_by     = 'CC_DISPATCHER',
                   last_update_date    = SYSTIMESTAMP
             WHERE cc_match_group_id   = g.cc_match_group_id;
 
            l_sent_count := l_sent_count + 1;
            COMMIT;
 
        ELSE
            FORALL i IN 1..l_id_count
                UPDATE xxemr_cc_fbdi_lines
                   SET send_attempts    = send_attempts + 1,
                       last_error       = l_api_response,
                       last_updated_by  = 'CC_DISPATCHER',
                       last_update_date = SYSTIMESTAMP
                 WHERE cc_fbdi_line_id  = l_id_list(i);
 
            FORALL i IN 1..l_id_count
                UPDATE xxemr_cc_fbdi_lines
                   SET send_status = CASE
                                         WHEN send_attempts >= p_max_attempts
                                         THEN 'ERROR'
                                         ELSE 'FAILED'
                                     END
                 WHERE cc_fbdi_line_id = l_id_list(i);
 
            l_failed_count := l_failed_count + 1;
            COMMIT;
            log_cc('Group FAILED. HTTP=' || NVL(TO_CHAR(l_api_status),'NULL'));
        END IF;
 
        IF DBMS_LOB.ISTEMPORARY(l_payload) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_payload);
        END IF;
 
    END LOOP;
 
    log_cc('xxemr_dispatch_cc_to_oic DONE.'
        || ' Sent='   || l_sent_count
        || ' Failed=' || l_failed_count);
 
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_LOB.ISTEMPORARY(l_payload) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_payload);
        END IF;
        ROLLBACK;
        log_cc('FATAL: xxemr_dispatch_cc_to_oic: ' || SQLERRM);
        RAISE;
END xxemr_dispatch_cc_to_oic;
 
 
-- ================================================================
-- STEP 6: xxemr_run_cc_keyword_check  (independent utility)
-- Scans statement line descriptions. Sets cc_flag /
-- fund_transfer_flag using XXEMR_KEYWORD_MAPPING.
-- ================================================================
PROCEDURE xxemr_run_cc_keyword_check (
    p_bank_account_id IN NUMBER   DEFAULT NULL,
    p_created_by      IN VARCHAR2 DEFAULT 'SYSTEM'
)
IS
    l_cc_updated NUMBER;
    l_ft_updated NUMBER;
BEGIN
    log_cc('xxemr_run_cc_keyword_check START. bank='
        || NVL(TO_CHAR(p_bank_account_id),'ALL'));
 
    -- CC keywords
    UPDATE xxemr_bank_statement_lines s
       SET s.cc_flag       = 'Y',
           s.cc_check_done = 'Y',
           s.last_updated  = SYSTIMESTAMP
     WHERE NVL(s.cc_flag,'N')  = 'N'
       AND (p_bank_account_id IS NULL OR s.bank_account_id = p_bank_account_id)
       AND EXISTS (
               SELECT 1 FROM xxemr_keyword_mapping k
                WHERE k.enabled_flag   = 'Y'
                  AND k.is_credit_card = 'Y'
                  AND INSTR(UPPER(s.description), UPPER(k.keywords)) > 0
           );
 
    l_cc_updated := SQL%ROWCOUNT;
 
    -- Fund transfer keywords (identify only — no matching flow yet)
    UPDATE xxemr_bank_statement_lines s
       SET s.fund_transfer_flag = 'Y',
           s.cc_check_done      = 'Y',
           s.last_updated       = SYSTIMESTAMP
     WHERE NVL(s.fund_transfer_flag,'N') = 'N'
       AND (p_bank_account_id IS NULL OR s.bank_account_id = p_bank_account_id)
       AND EXISTS (
               SELECT 1 FROM xxemr_keyword_mapping k
                WHERE k.enabled_flag     = 'Y'
                  AND k.is_fund_transfer = 'Y'
                  AND INSTR(UPPER(s.description), UPPER(k.keywords)) > 0
           );
 
    l_ft_updated := SQL%ROWCOUNT;
 
    COMMIT;
    log_cc('xxemr_run_cc_keyword_check DONE.'
        || ' CC flagged='            || l_cc_updated
        || ' Fund transfer flagged=' || l_ft_updated);
 
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        log_cc('FATAL: xxemr_run_cc_keyword_check: ' || SQLERRM);
        RAISE;
END xxemr_run_cc_keyword_check;
 
 
-- ================================================================
-- MASTER: xxemr_process_cc_upload
-- Runs steps 1-5 in sequence for a given upload_id.
-- Call this from APEX after the Excel file has been loaded
-- into XXEMR_CC_FUND_TRANSFER.
-- ================================================================
PROCEDURE xxemr_process_cc_upload (
    p_upload_id   IN NUMBER,
    p_created_by  IN VARCHAR2 DEFAULT 'SYSTEM'
)
IS
BEGIN
    log_cc('========================================');
    log_cc('xxemr_process_cc_upload START. upload_id=' || p_upload_id);
    log_cc('========================================');
 
    xxemr_resolve_cc_bank(p_upload_id, p_created_by);
    xxemr_validate_cc_receipts(p_upload_id, p_created_by);
    xxemr_match_cc_statement(p_upload_id, p_created_by);
    xxemr_build_cc_fbdi(p_upload_id, p_created_by);
    xxemr_dispatch_cc_to_oic(p_upload_id);
 
    log_cc('========================================');
    log_cc('xxemr_process_cc_upload DONE.');
    log_cc('========================================');
 
EXCEPTION
    WHEN OTHERS THEN
        log_cc('FATAL: xxemr_process_cc_upload: ' || SQLERRM
            || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
END xxemr_process_cc_upload;
 
 
END XXEMR_CC_RECONCILIATION_PKG;