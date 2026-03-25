create or replace PROCEDURE xxemr_dispatch_fbdi_to_oic (
    p_batch_size   IN NUMBER DEFAULT 100,
    p_max_attempts IN NUMBER DEFAULT 2
) AS
 
-- ----------------------------------------------------------------
-- TYPE: indexed table of NUMBER used to collect fbdi_line_ids
-- for the current batch so they can be bulk-updated after the POST.
-- ----------------------------------------------------------------
    TYPE t_num_list IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
 
-- ----------------------------------------------------------------
-- CURSOR 1: Distinct recon_reference groups that still need to be
-- sent.  FIFO order so oldest groups are dispatched first.
-- Excludes permanently dead rows (send_attempts >= max) and SENT.
-- ----------------------------------------------------------------
    CURSOR c_refs IS
        SELECT   recon_reference,
                 MIN(creation_date) AS first_created
          FROM   xxemr_recon_fbdi_lines
         WHERE   send_status   IN ('PENDING', 'FAILED')
           AND   send_attempts  < p_max_attempts
         GROUP BY recon_reference
         ORDER BY MIN(creation_date);
 
-- ----------------------------------------------------------------
-- CURSOR 2: All sendable rows for one recon_reference.
-- BS (statement line) row is always first so OIC sees it first.
-- ----------------------------------------------------------------
    CURSOR c_lines (p_ref IN VARCHAR2) IS
        SELECT fbdi_line_id,
               source_id,
               source_code,
               recon_reference,
               bank_account_id
          FROM xxemr_recon_fbdi_lines
         WHERE recon_reference = p_ref
           AND send_status     IN ('PENDING', 'FAILED')
           AND send_attempts    < p_max_attempts
         ORDER BY CASE WHEN source_code = 'BS' THEN 0 ELSE 1 END,
                  fbdi_line_id;
 
-- ----------------------------------------------------------------
-- CONFIGURATION
-- ----------------------------------------------------------------
    v_endpoint    VARCHAR2(500);
    v_username    VARCHAR2(200);
    v_password    VARCHAR2(200);
 
-- ----------------------------------------------------------------
-- BATCH STATE  (reset after every flush)
-- ----------------------------------------------------------------
    l_payload      CLOB;              -- JSON body for current batch
    l_batch_count  PLS_INTEGER := 0;  -- groups accumulated so far
    l_id_count     PLS_INTEGER := 0;  -- fbdi_line_ids collected
    l_id_list      t_num_list;        -- fbdi_line_ids for bulk UPDATE
    l_first_group  BOOLEAN     := TRUE;
    l_first_line   BOOLEAN;
 
-- ----------------------------------------------------------------
-- RUN-LEVEL SUMMARY COUNTERS
-- ----------------------------------------------------------------
    l_pending_count        NUMBER        := 0;
    l_total_groups         NUMBER        := 0;
    l_total_sent           NUMBER        := 0;
    l_total_failed         NUMBER        := 0;
    l_total_errored        NUMBER        := 0;
    -- Track current bank account to flush batch on change
    -- Fusion rejects CE-660462 if multiple bank accounts in one load
    l_current_bank_account VARCHAR2(100) := NULL;
 
-- ----------------------------------------------------------------
-- HTTP
-- ----------------------------------------------------------------
    l_api_response  VARCHAR2(4000);
    l_api_status    NUMBER;
 
-- ================================================================
-- INLINE HELPER: json_str
-- Minimal JSON-safe escaping: handles backslash and double-quote.
-- NULL becomes an empty string.
-- ================================================================
    FUNCTION json_str (p_val IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN REPLACE(
                   REPLACE(NVL(p_val, ''), '\', '\\'),
               '"', '\"');
    END json_str;
 
-- ================================================================
-- INLINE HELPER: reset_batch
-- Free and recreate the payload CLOB; clear id list and counters.
-- Called after every flush so the next batch starts clean.
-- ================================================================
    PROCEDURE reset_batch IS
    BEGIN
        IF DBMS_LOB.ISTEMPORARY(l_payload) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_payload);
        END IF;
        DBMS_LOB.CREATETEMPORARY(l_payload, TRUE);
        l_batch_count := 0;
        l_id_count    := 0;
        l_id_list.DELETE;
        l_first_group := TRUE;
    END reset_batch;
 
-- ================================================================
-- INLINE HELPER: flush_batch
-- Wraps l_payload in the OIC JSON envelope, POSTs to OIC, then
-- bulk-updates every fbdi_line_id in l_id_list with the outcome.
--
--   Success (200/201):
--     send_status = SENT, sent_date = now, oic_response = body
--
--   Failure:
--     send_attempts += 1
--     send_status   = ERROR  if new attempts >= p_max_attempts
--     send_status   = FAILED otherwise (will retry next run)
-- ================================================================
    PROCEDURE flush_batch IS
        l_envelope         CLOB;
        l_errored_in_batch NUMBER := 0;
    BEGIN
        IF l_batch_count = 0 THEN
            RETURN;
        END IF;
 
        -- Wrap payload in the OIC envelope
        DBMS_LOB.CREATETEMPORARY(l_envelope, TRUE);
        DBMS_LOB.APPEND(l_envelope, TO_CLOB('{"reconciliationLines":['));
        DBMS_LOB.APPEND(l_envelope, l_payload);
        DBMS_LOB.APPEND(l_envelope, TO_CLOB(']}'));
 
        DBMS_OUTPUT.PUT_LINE('  → Flushing: '
            || l_batch_count || ' groups | '
            || l_id_count    || ' lines | endpoint: ' || v_endpoint);
 
        -- REST POST to OIC
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
                    p_body        => l_envelope
                ), 1, 4000);
            l_api_status := apex_web_service.g_status_code;
        EXCEPTION
            WHEN OTHERS THEN
                l_api_status   := -1;
                l_api_response := SUBSTR('REST exception: ' || SQLERRM, 1, 4000);
        END;
 
        DBMS_OUTPUT.PUT_LINE('  → HTTP ' || NVL(TO_CHAR(l_api_status), 'NULL'));
 
        IF l_api_status IN (200, 201) THEN
 
            -- ── SUCCESS: stamp all rows SENT ─────────────────────
            FORALL i IN 1 .. l_id_count
                UPDATE xxemr_recon_fbdi_lines
                   SET send_status      = 'SENT',
                       send_attempts    = send_attempts + 1,
                       sent_date        = SYSTIMESTAMP,
                       oic_response     = l_api_response,
                       last_error       = NULL,
                       last_updated_by  = 'OIC_DISPATCHER',
                       last_update_date = SYSTIMESTAMP
                 WHERE fbdi_line_id     = l_id_list(i);
 
            COMMIT;
            l_total_sent := l_total_sent + l_batch_count;
            DBMS_OUTPUT.PUT_LINE('  ✓ Accepted by OIC.');
 
        ELSE
 
            -- ── FAILURE STEP 1: increment attempt counter ─────────
            FORALL i IN 1 .. l_id_count
                UPDATE xxemr_recon_fbdi_lines
                   SET send_attempts    = send_attempts + 1,
                       last_error       = l_api_response,
                       last_updated_by  = 'OIC_DISPATCHER',
                       last_update_date = SYSTIMESTAMP
                 WHERE fbdi_line_id     = l_id_list(i);
 
            -- ── FAILURE STEP 2: promote to ERROR if exhausted ─────
            FORALL i IN 1 .. l_id_count
                UPDATE xxemr_recon_fbdi_lines
                   SET send_status = CASE
                                         WHEN send_attempts >= p_max_attempts
                                         THEN 'ERROR'
                                         ELSE 'FAILED'
                                     END
                 WHERE fbdi_line_id = l_id_list(i);
 
            -- Count groups permanently dead (ERROR on the BS row = 1 per group)
            SELECT COUNT(*)
              INTO l_errored_in_batch
              FROM xxemr_recon_fbdi_lines
             WHERE source_code  = 'BS'
               AND send_status  = 'ERROR'
               AND fbdi_line_id IN (
                       SELECT l_id_list(LEVEL)
                         FROM dual
                        CONNECT BY LEVEL <= l_id_count
                   );
 
            COMMIT;
            l_total_failed  := l_total_failed  + l_batch_count;
            l_total_errored := l_total_errored + l_errored_in_batch;
            DBMS_OUTPUT.PUT_LINE('  ✗ Rejected. HTTP='
                || NVL(TO_CHAR(l_api_status), 'NULL')
                || ' | Permanently errored (no more retries): '
                || l_errored_in_batch);
 
        END IF;
 
        DBMS_LOB.FREETEMPORARY(l_envelope);
 
    END flush_batch;
 
-- ================================================================
-- MAIN BODY
-- ================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('================================================');
    DBMS_OUTPUT.PUT_LINE('xxemr_dispatch_fbdi_to_oic  START : '
        || TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Batch size  : ' || p_batch_size
        || ' groups/call  |  Max attempts : ' || p_max_attempts);
    DBMS_OUTPUT.PUT_LINE('================================================');
 
    -- ----------------------------------------------------------------
    -- STEP 1: Quick-exit if nothing to send
    -- ----------------------------------------------------------------
    SELECT COUNT(DISTINCT recon_reference)
      INTO l_pending_count
      FROM xxemr_recon_fbdi_lines
     WHERE send_status   IN ('PENDING', 'FAILED')
       AND send_attempts  < p_max_attempts;
 
    IF l_pending_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No PENDING/FAILED rows. Exiting — no OIC calls made.');
        DBMS_OUTPUT.PUT_LINE('================================================');
        RETURN;
    END IF;
 
    DBMS_OUTPUT.PUT_LINE('Groups to dispatch : ' || l_pending_count);
 
    -- ----------------------------------------------------------------
    -- STEP 2: Load OIC configuration
    -- ----------------------------------------------------------------
    BEGIN
        SELECT config_value INTO v_endpoint
          FROM apex_recon_config
         WHERE config_key = 'OIC_RECON_ENDPOINT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20501,
                'xxemr_dispatch_fbdi_to_oic: OIC_RECON_ENDPOINT not in APEX_RECON_CONFIG.');
    END;
 
    BEGIN
        SELECT config_value INTO v_username
          FROM apex_recon_config
         WHERE config_key = 'OIC_API_USERNAME';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20502,
                'xxemr_dispatch_fbdi_to_oic: OIC_API_USERNAME not in APEX_RECON_CONFIG.');
    END;
 
    BEGIN
        SELECT config_value INTO v_password
          FROM apex_recon_config
         WHERE config_key = 'OIC_API_PASSWORD';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20503,
                'xxemr_dispatch_fbdi_to_oic: OIC_API_PASSWORD not in APEX_RECON_CONFIG.');
    END;
 
    -- ----------------------------------------------------------------
    -- STEP 3: Initialise first batch payload
    -- ----------------------------------------------------------------
    DBMS_LOB.CREATETEMPORARY(l_payload, TRUE);
 
    -- ----------------------------------------------------------------
    -- STEP 4: Main dispatch loop — one iteration per recon_reference
    -- ----------------------------------------------------------------
    FOR r_ref IN c_refs LOOP
 
        l_total_groups := l_total_groups + 1;
 
        -- ----------------------------------------------------------------
        -- CRITICAL: Fusion rejects CE-660462 if multiple bank accounts
        -- exist in one load request. Flush batch on bank account change.
        -- ----------------------------------------------------------------
        DECLARE
            l_this_bank VARCHAR2(100);
        BEGIN
            SELECT MIN(bank_account_id)
              INTO l_this_bank
              FROM xxemr_recon_fbdi_lines
             WHERE recon_reference = r_ref.recon_reference
               AND source_code     = 'BS';
 
            IF l_current_bank_account IS NULL THEN
                l_current_bank_account := l_this_bank;
            ELSIF l_current_bank_account != NVL(l_this_bank, 'X') THEN
                IF l_batch_count > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('Bank account changed ('
                        || l_current_bank_account || ' to '
                        || l_this_bank || '). Flushing batch...');
                    flush_batch;
                    reset_batch;
                END IF;
                l_current_bank_account := l_this_bank;
            END IF;
        END;
 
        l_batch_count  := l_batch_count  + 1;
 
        DBMS_OUTPUT.PUT_LINE('  [' || l_total_groups || '/'
            || l_pending_count || '] ' || r_ref.recon_reference);
 
        -- Comma between groups in the JSON array
        IF NOT l_first_group THEN
            DBMS_LOB.APPEND(l_payload, TO_CLOB(','));
        END IF;
        l_first_group := FALSE;
        l_first_line  := TRUE;
 
        -- Append every row belonging to this recon_reference
        FOR r_line IN c_lines(r_ref.recon_reference) LOOP
 
            l_id_count            := l_id_count + 1;
            l_id_list(l_id_count) := r_line.fbdi_line_id;
 
            IF NOT l_first_line THEN
                DBMS_LOB.APPEND(l_payload, TO_CLOB(','));
            END IF;
            l_first_line := FALSE;
 
            DBMS_LOB.APPEND(l_payload, TO_CLOB(
                '{'
                || '"sourceId":"'
                    -- BS  → statement_line_id
                    -- AR  → cash_receipt_id   (= source_id on AR rows)
                    -- XT  → ext_txn_id        (= source_id on XT rows)
                    || json_str(TO_CHAR(r_line.source_id))          || '",'
                || '"sourceCode":"'
                    || json_str(r_line.source_code)                 || '",'
                || '"externalReconReference":"'
                    || json_str(r_ref.recon_reference)              || '",'
                || '"bankAccountId":"'
                    || json_str(r_line.bank_account_id)             || '",'
                || '"sourceLineId":""'
                || '}'
            ));
 
        END LOOP;  -- c_lines
 
        -- Flush when batch is full
        IF l_batch_count >= p_batch_size THEN
            DBMS_OUTPUT.PUT_LINE('Batch full (' || p_batch_size || ' groups). Flushing...');
            flush_batch;
            reset_batch;
        END IF;
 
    END LOOP;  -- c_refs
 
    -- ----------------------------------------------------------------
    -- STEP 5: Flush the last (possibly partial) batch
    -- ----------------------------------------------------------------
    IF l_batch_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Flushing final batch (' || l_batch_count || ' groups)...');
        flush_batch;
        reset_batch;
    END IF;
 
    -- ----------------------------------------------------------------
    -- STEP 6: Summary output
    -- ----------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('================================================');
    DBMS_OUTPUT.PUT_LINE('xxemr_dispatch_fbdi_to_oic  END   : '
        || TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Groups found           : ' || l_pending_count);
    DBMS_OUTPUT.PUT_LINE('Groups processed       : ' || l_total_groups);
    DBMS_OUTPUT.PUT_LINE('Successfully SENT      : ' || l_total_sent);
    DBMS_OUTPUT.PUT_LINE('Failed (will retry)    : '
        || GREATEST(l_total_failed - l_total_errored, 0));
    DBMS_OUTPUT.PUT_LINE('Permanently ERROR      : ' || l_total_errored);
    DBMS_OUTPUT.PUT_LINE('================================================');
 
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_LOB.ISTEMPORARY(l_payload) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_payload);
        END IF;
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR in xxemr_dispatch_fbdi_to_oic:');
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
        DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;
 
END xxemr_dispatch_fbdi_to_oic;
/