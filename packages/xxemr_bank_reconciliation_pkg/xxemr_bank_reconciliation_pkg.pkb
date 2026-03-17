CREATE OR REPLACE PACKAGE BODY XXEMR_BANK_RECONCILIATION_PKG AS

-- ================================================================
-- PACKAGE BODY : XXEMR_BANK_RECONCILIATION_PKG
-- Description  : Consolidated Bank Reconciliation package body.
-- Sections:
--   1  Core Utilities            (private)
--   2  Match Group Management    (private)
--   3  Manual Reconciliation     (public)
--   4  AI Match Application      (public)
--   5  External Transaction Processing (public)
--   6  One-to-One AI Matching Engine  (public)
--   7  One-to-Many AI Matching Engine (public)
-- ================================================================

-- ----------------------------------------------------------------
-- FORWARD DECLARATIONS
-- Private procedures declared here so they are visible to all
-- procedures below regardless of definition order.
-- ----------------------------------------------------------------
PROCEDURE log_step (
    p_run_id            IN NUMBER    DEFAULT NULL,
    p_statement_line_id IN NUMBER    DEFAULT NULL,
    p_step_name         IN VARCHAR2,
    p_step_status       IN VARCHAR2,
    p_detail            IN VARCHAR2  DEFAULT NULL
);

PROCEDURE xxemr_fetch_candidate (
    p_source IN  VARCHAR2,
    p_id     IN  NUMBER,
    p_amount OUT NUMBER,
    p_date   OUT DATE
);

PROCEDURE xxemr_insert_fbdi_lines (
    p_statement_line_id IN NUMBER,
    p_candidates        IN VARCHAR2,
    p_recon_method      IN VARCHAR2,
    p_match_group_id    IN NUMBER   DEFAULT NULL,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
);

-- ================================================================
-- SECTION 1 — CORE UTILITIES
-- ================================================================

PROCEDURE xxemr_log (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_log
-- Purpose   : Inserts a log record into XXEMR_BANK_RECON_LOGS.
-- ----------------------------------------------------------------
    p_source      IN VARCHAR2,
    p_log_message IN VARCHAR2
)
IS
BEGIN
    INSERT INTO xxemr_bank_recon_logs (
        source,
        log_message
    )
    VALUES (
        p_source,
        p_log_message
    );
EXCEPTION
    WHEN OTHERS THEN
        NULL; -- Logging must never break the main process
END xxemr_log;


-- ----------------------------------------------------------------
-- PRIVATE: LOG_STEP
-- Purpose : Autonomous transaction logger. Writes to
--           apex_ext_txn_log independently of the calling
--           transaction so that log records are preserved
--           even on ROLLBACK.
-- ----------------------------------------------------------------
PROCEDURE log_step (
    p_run_id            IN NUMBER    DEFAULT NULL,
    p_statement_line_id IN NUMBER    DEFAULT NULL,
    p_step_name         IN VARCHAR2,
    p_step_status       IN VARCHAR2,
    p_detail            IN VARCHAR2  DEFAULT NULL
)
IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    INSERT INTO apex_ext_txn_log (
        run_id, statement_line_id, step_name, step_status, step_detail, step_time
    ) VALUES (
        p_run_id, p_statement_line_id,
        SUBSTR(p_step_name,  1, 100),
        SUBSTR(p_step_status,1,  30),
        SUBSTR(p_detail,     1, 2000),
        SYSTIMESTAMP
    );
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN NULL;
END log_step;


PROCEDURE xxemr_parse_token (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_parse_token
-- Purpose   : Parses a prefixed candidate token into source + ID.
--             Valid prefixes: AR, EX, ST
-- ----------------------------------------------------------------
    p_token  IN  VARCHAR2,
    p_source OUT VARCHAR2,
    p_id     OUT NUMBER
) IS
BEGIN
    IF LENGTH(p_token) < 3 THEN
        RAISE_APPLICATION_ERROR(-20002,
            'Invalid token "' || p_token
            || '" — must be at least 3 characters (2-char prefix + numeric ID)');
    END IF;

    p_source := UPPER(SUBSTR(p_token, 1, 2));
    p_id     := TO_NUMBER(SUBSTR(p_token, 3));

    IF p_source NOT IN ('AR', 'EX', 'ST') THEN
        RAISE_APPLICATION_ERROR(-20003,
            'Unknown source prefix "' || p_source
            || '" in token "' || p_token
            || '" — must be AR, EX, or ST');
    END IF;

END xxemr_parse_token;


PROCEDURE xxemr_fetch_candidate (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_fetch_candidate
-- Purpose   : Retrieves amount + date for a candidate from the
--             appropriate source table (AR, EX, or ST).
-- NOTE      : EX source uses ext_txn_id as PK on
--             xxemr_external_transactions.
-- ----------------------------------------------------------------
    p_source IN  VARCHAR2,
    p_id     IN  NUMBER,
    p_amount OUT NUMBER,
    p_date   OUT DATE
) IS
BEGIN
    IF p_source = 'AR' THEN
        SELECT NVL(amount, 0), NVL(gl_date, SYSDATE)
          INTO p_amount, p_date
          FROM xxemr_ar_cash_receipts
         WHERE cash_receipt_id = p_id;

    ELSIF p_source = 'EX' THEN
        SELECT NVL(amount, 0), NVL(transaction_date, SYSDATE)
          INTO p_amount, p_date
          FROM xxemr_external_transactions
         WHERE ext_txn_id = p_id;      -- ext_txn_id is the PK

    ELSIF p_source = 'ST' THEN
        SELECT NVL(l.amount, 0), NVL(h.statement_date, SYSDATE)
          INTO p_amount, p_date
          FROM xxemr_bank_statement_lines   l
          JOIN xxemr_bank_statement_headers h
            ON l.statement_header_id = h.statement_header_id
         WHERE l.statement_line_id = p_id;
    END IF;

END xxemr_fetch_candidate;


-- ----------------------------------------------------------------
-- PRIVATE: xxemr_insert_fbdi_lines
-- Purpose : Inserts one row per reconciliation participant into
--           XXEMR_RECON_FBDI_LINES, which is the source of truth
--           for OIC FBDI loading.
--
--           Always inserts:
--             1 BS  row  → the bank statement line itself
--             N AR/XT rows → one per candidate token
--
--           Source codes:
--             AR prefix → source_code = 'AR'
--             EX prefix → source_code = 'XT'
--
--           All rows in a group share the same recon_reference
--           generated here (GroupAI001, GroupAI002 ...).
--           send_status defaults to 'PENDING' — OIC polls for these.
-- ----------------------------------------------------------------
PROCEDURE xxemr_insert_fbdi_lines (
    p_statement_line_id IN NUMBER,
    p_candidates        IN VARCHAR2,
    p_recon_method      IN VARCHAR2,
    p_match_group_id    IN NUMBER   DEFAULT NULL,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
)
IS
    l_recon_reference   VARCHAR2(50);
    l_bank_account_id   VARCHAR2(100);
    l_seq_val           NUMBER;
    l_remaining         VARCHAR2(4000);
    l_pos               NUMBER;
    l_token             VARCHAR2(200);
    l_source            VARCHAR2(10);
    l_candidate_id      NUMBER;
BEGIN
    -- Step 1: Fetch bank_account_id for the statement line
    BEGIN
        SELECT bank_account_id
          INTO l_bank_account_id
          FROM xxemr_bank_statement_lines
         WHERE statement_line_id = p_statement_line_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20410,
                'xxemr_insert_fbdi_lines: Statement line not found: '
                || p_statement_line_id);
    END;

    -- Step 2: Generate group reference — GroupAI001, GroupAI002 ...
    SELECT xxemr_recon_group_ref_seq.NEXTVAL INTO l_seq_val FROM DUAL;
    l_recon_reference := 'GroupAI' || LPAD(TO_CHAR(l_seq_val), 3, '0');

    -- Step 3: Insert BS row for the bank statement line
    INSERT INTO xxemr_recon_fbdi_lines (
        fbdi_line_id,
        recon_reference,
        bank_account_id,
        source_code,
        source_id,
        match_group_id,
        statement_line_id,
        recon_method,
        send_status,
        send_attempts,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    ) VALUES (
        xxemr_recon_fbdi_seq.NEXTVAL,
        l_recon_reference,
        l_bank_account_id,
        'BS',
        p_statement_line_id,
        p_match_group_id,
        p_statement_line_id,
        p_recon_method,
        'PENDING',
        0,
        p_user,
        SYSTIMESTAMP,
        p_user,
        SYSTIMESTAMP
    );

    -- Step 4: Insert one row per candidate (AR or XT)
    l_remaining := p_candidates || ':';
    LOOP
        l_pos := INSTR(l_remaining, ':');
        EXIT WHEN l_pos = 0;
        l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
        l_remaining := SUBSTR(l_remaining, l_pos + 1);
        EXIT WHEN l_token IS NULL;

        xxemr_parse_token(l_token, l_source, l_candidate_id);

        INSERT INTO xxemr_recon_fbdi_lines (
            fbdi_line_id,
            recon_reference,
            bank_account_id,
            source_code,
            source_id,
            match_group_id,
            statement_line_id,
            recon_method,
            send_status,
            send_attempts,
            created_by,
            creation_date,
            last_updated_by,
            last_update_date
        ) VALUES (
            xxemr_recon_fbdi_seq.NEXTVAL,
            l_recon_reference,
            l_bank_account_id,
            CASE l_source
                WHEN 'AR' THEN 'AR'
                WHEN 'EX' THEN 'XT'
                ELSE l_source
            END,
            l_candidate_id,
            p_match_group_id,
            p_statement_line_id,
            p_recon_method,
            'PENDING',
            0,
            p_user,
            SYSTIMESTAMP,
            p_user,
            SYSTIMESTAMP
        );
    END LOOP;

    log_step(NULL, p_statement_line_id,
        'FBDI_LINES_INSERTED', 'SUCCESS',
        'Ref: ' || l_recon_reference
        || ' | Method: ' || p_recon_method
        || ' | Candidates: ' || p_candidates);

EXCEPTION
    WHEN OTHERS THEN
        log_step(NULL, p_statement_line_id,
            'FBDI_LINES_INSERT', 'FAILED',
            SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 2000));
        RAISE;
END xxemr_insert_fbdi_lines;


-- ================================================================
-- SECTION 2 — MATCH GROUP MANAGEMENT
-- ================================================================

PROCEDURE xxemr_insert_match_group (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_insert_match_group
-- Purpose   : Creates a Match Group Header + Detail rows for a
--             given Statement Line and set of candidate tokens.
--             Used by manual match, AI match, and EXT processing.
--             Tokens are colon-delimited prefixed IDs,
--             e.g. 'AR12345:AR67890:EX111'
-- ----------------------------------------------------------------
    p_statement_line_id IN NUMBER,
    p_candidates        IN VARCHAR2,
    p_match_type        IN VARCHAR2,
    p_match_score       IN NUMBER,
    p_ranking           IN NUMBER,
    p_amt_diff          IN NUMBER,
    p_date_diff         IN NUMBER,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
) IS
    l_match_group_id   NUMBER;
    l_total_amount     NUMBER        := 0;
    l_candidate_source VARCHAR2(30);
    l_has_ar           BOOLEAN       := FALSE;
    l_has_ext          BOOLEAN       := FALSE;
    l_remaining        VARCHAR2(4000);
    l_pos              NUMBER;
    l_token            VARCHAR2(200);
    l_source           VARCHAR2(10);
    l_candidate_id     NUMBER;
    l_cand_amount      NUMBER;
    l_cand_date        DATE;
BEGIN
    -- -------------------------------------------------------
    -- Pass 1: sum total candidate amount and detect source mix
    -- -------------------------------------------------------
    l_remaining := p_candidates || ':';
    LOOP
        l_pos := INSTR(l_remaining, ':');
        EXIT WHEN l_pos = 0;
        l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
        l_remaining := SUBSTR(l_remaining, l_pos + 1);
        EXIT WHEN l_token IS NULL;

        xxemr_parse_token(l_token, l_source, l_candidate_id);
        xxemr_fetch_candidate(l_source, l_candidate_id, l_cand_amount, l_cand_date);

        l_total_amount := l_total_amount + l_cand_amount;
        IF l_source = 'AR' THEN l_has_ar  := TRUE; END IF;
        IF l_source = 'EX' THEN l_has_ext := TRUE; END IF;
    END LOOP;

    l_candidate_source :=
        CASE
            WHEN l_has_ar AND l_has_ext THEN 'MIXED'
            WHEN l_has_ar               THEN 'AR'
            ELSE                             'EXT'
        END;

    -- -------------------------------------------------------
    -- Insert match group header
    -- -------------------------------------------------------
    INSERT INTO xxemr_match_groups (
        match_group_id, statement_line_id, total_match_amount,
        difference_amount, match_score, match_type, ranking,
        candidate_source, created_date, creation_date, created_by,
        last_update_date, last_updated_by
    ) VALUES (
        xxemr_match_groups_seq.nextval, p_statement_line_id, l_total_amount,
        p_amt_diff, p_match_score, p_match_type, p_ranking,
        l_candidate_source, SYSDATE, SYSTIMESTAMP, p_user,
        SYSTIMESTAMP, p_user
    )
    RETURNING match_group_id INTO l_match_group_id;

    -- -------------------------------------------------------
    -- Pass 2: insert one detail row per candidate
    -- -------------------------------------------------------
    l_remaining := p_candidates || ':';
    LOOP
        l_pos := INSTR(l_remaining, ':');
        EXIT WHEN l_pos = 0;
        l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
        l_remaining := SUBSTR(l_remaining, l_pos + 1);
        EXIT WHEN l_token IS NULL;

        xxemr_parse_token(l_token, l_source, l_candidate_id);
        xxemr_fetch_candidate(l_source, l_candidate_id, l_cand_amount, l_cand_date);

        INSERT INTO xxemr_match_group_details (
            match_group_detail_id, match_group_id, candidate_source,
            candidate_id, amount, individual_score,
            creation_date, created_by, last_update_date, last_updated_by
        ) VALUES (
            xxemr_match_grp_dtl_seq.nextval, l_match_group_id, l_source,
            l_candidate_id, l_cand_amount, p_match_score,
            SYSTIMESTAMP, p_user, SYSTIMESTAMP, p_user
        );
    END LOOP;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20001,
            'xxemr_insert_match_group: Candidate not found. '
            || 'LINE_ID=' || p_statement_line_id
            || ' CANDIDATES=' || p_candidates);
    WHEN OTHERS THEN
        RAISE;
END xxemr_insert_match_group;


PROCEDURE xxemr_update_recon_status (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_update_recon_status
-- Purpose   : Stamps reconciliation status on the Statement Line
--             and all associated AR / EX candidate records.
-- ----------------------------------------------------------------
    p_statement_line_id IN NUMBER,
    p_candidates        IN VARCHAR2,
    p_status            IN VARCHAR2,
    p_match_flag        IN VARCHAR2 DEFAULT 'Y',
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
) IS
    l_remaining    VARCHAR2(4000);
    l_pos          NUMBER;
    l_token        VARCHAR2(200);
    l_source       VARCHAR2(10);
    l_candidate_id NUMBER;
BEGIN
    -- Always update the statement line itself
    UPDATE xxemr_bank_statement_lines
       SET ai_status        = p_status,
           recon_status     = 'REC',
           approval_status  = 'APPROVED',
           match_flag       = p_match_flag,
           last_update_date = SYSTIMESTAMP,
           last_updated_by  = p_user,
           approved_by      = p_user,
           approved_date    = SYSTIMESTAMP
     WHERE statement_line_id = p_statement_line_id;

    -- Iterate candidates and update each source table
    l_remaining := p_candidates || ':';
    LOOP
        l_pos := INSTR(l_remaining, ':');
        EXIT WHEN l_pos = 0;
        l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
        l_remaining := SUBSTR(l_remaining, l_pos + 1);
        EXIT WHEN l_token IS NULL;

        xxemr_parse_token(l_token, l_source, l_candidate_id);

        IF l_source = 'AR' THEN
            UPDATE xxemr_ar_cash_receipts
               SET ai_status        = p_status,
                   match_flag       = p_match_flag,
                   last_update_date = SYSTIMESTAMP,
                   last_updated_by  = p_user
             WHERE cash_receipt_id  = l_candidate_id;

        ELSIF l_source = 'EX' THEN
            UPDATE xxemr_external_transactions
               SET ai_status        = p_status,
                   match_flag       = p_match_flag,
                   last_update_date = SYSTIMESTAMP,
                   last_updated_by  = p_user
             WHERE ext_txn_id       = l_candidate_id;  -- ext_txn_id is the PK

        -- ST tokens: statement line already updated above
        END IF;
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN RAISE;
END xxemr_update_recon_status;


-- ================================================================
-- SECTION 3 — MANUAL RECONCILIATION
-- ================================================================

PROCEDURE xxemr_process_manual_match (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_process_manual_match
-- Purpose   : Full manual reconciliation orchestrator.
--             Validates amounts balance, creates match group,
--             stamps MANUAL_RECON status on all records.
-- ----------------------------------------------------------------
    p_statement_line_id IN NUMBER,
    p_candidates        IN VARCHAR2,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
) IS
    l_stmt_amount  NUMBER;
    l_stmt_date    DATE;
    l_total_amount NUMBER        := 0;
    l_amt_diff     NUMBER        := 0;
    l_date_diff    NUMBER        := 0;
    l_remaining    VARCHAR2(4000);
    l_pos          NUMBER;
    l_token        VARCHAR2(200);
    l_source       VARCHAR2(10);
    l_candidate_id NUMBER;
    l_cand_amount  NUMBER;
    l_cand_date    DATE;
BEGIN
    -- Step 1: Remove any existing match groups for this line
    DELETE FROM xxemr_match_group_details
     WHERE match_group_id IN (
               SELECT match_group_id
                 FROM xxemr_match_groups
                WHERE statement_line_id = p_statement_line_id);

    DELETE FROM xxemr_match_groups
     WHERE statement_line_id = p_statement_line_id;

    -- Step 2: Fetch statement line amount and date
    SELECT NVL(l.amount, 0), NVL(h.statement_date, SYSDATE)
      INTO l_stmt_amount, l_stmt_date
      FROM xxemr_bank_statement_lines   l
      JOIN xxemr_bank_statement_headers h
        ON l.statement_header_id = h.statement_header_id
     WHERE l.statement_line_id = p_statement_line_id;

    -- Step 3: Iterate candidates to compute total amount + max date diff
    l_remaining := p_candidates || ':';
    LOOP
        l_pos := INSTR(l_remaining, ':');
        EXIT WHEN l_pos = 0;
        l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
        l_remaining := SUBSTR(l_remaining, l_pos + 1);
        EXIT WHEN l_token IS NULL;

        xxemr_parse_token(l_token, l_source, l_candidate_id);
        xxemr_fetch_candidate(l_source, l_candidate_id, l_cand_amount, l_cand_date);

        l_total_amount := l_total_amount + l_cand_amount;
        l_date_diff    := GREATEST(l_date_diff, ABS(l_cand_date - l_stmt_date));
    END LOOP;

    -- Step 4: Validate amounts balance exactly
    l_amt_diff := l_stmt_amount - l_total_amount;

    IF l_amt_diff = 0 THEN

        -- Step 5: Insert FBDI staging rows (source of truth for OIC loading).
        --         Done before match group insert and status updates so that
        --         if FBDI insert fails the whole transaction rolls back cleanly.
        xxemr_insert_fbdi_lines(
            p_statement_line_id => p_statement_line_id,
            p_candidates        => p_candidates,
            p_recon_method      => 'MANUAL',
            p_match_group_id    => NULL,   -- match group not yet created at this point
            p_user              => p_user
        );

        -- Step 6: Create match group header + details
        xxemr_insert_match_group(
            p_statement_line_id => p_statement_line_id,
            p_candidates        => p_candidates,
            p_match_type        => 'MANUAL_RECON',
            p_match_score       => 100,
            p_ranking           => 1,
            p_amt_diff          => l_amt_diff,
            p_date_diff         => l_date_diff,
            p_user              => p_user
        );

        -- Step 7: Stamp MANUAL_RECON status on all records
        xxemr_update_recon_status(
            p_statement_line_id => p_statement_line_id,
            p_candidates        => p_candidates,
            p_status            => 'MANUAL_RECON',
            p_match_flag        => 'Y',
            p_user              => p_user
        );

        COMMIT;

    ELSE
        RAISE_APPLICATION_ERROR(-20004,
            'xxemr_process_manual_match: Amounts do not balance. '
            || 'STMT=' || l_stmt_amount
            || ' TOTAL=' || l_total_amount
            || ' DIFF=' || l_amt_diff);
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20001,
            'xxemr_process_manual_match: Statement line or candidate not found. '
            || 'LINE_ID=' || p_statement_line_id);
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END xxemr_process_manual_match;


-- ================================================================
-- SECTION 4 — AI MATCH APPLICATION
-- ================================================================

PROCEDURE xxemr_apply_ai_match (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_apply_ai_match
-- Purpose   : Applies a pre-generated AI match group by stamping
--             AI_RECONCILED status on the line and all candidates.
-- ----------------------------------------------------------------
    p_match_group_id IN NUMBER,
    p_user           IN VARCHAR2 DEFAULT 'SYSTEM'
) IS
    l_statement_line_id NUMBER;
    l_candidates        VARCHAR2(4000);
BEGIN
    -- Step 1: Get statement line for this match group
    SELECT statement_line_id
      INTO l_statement_line_id
      FROM xxemr_match_groups
     WHERE match_group_id = p_match_group_id;

    -- Step 2: Build candidate token list from match group details
    SELECT LISTAGG(candidate_source || candidate_id, ':')
           WITHIN GROUP (ORDER BY candidate_id)
      INTO l_candidates
      FROM xxemr_match_group_details
     WHERE match_group_id = p_match_group_id;

    -- Step 3: Stamp REJECTED on all match groups for this line,
    --         then ACCEPTED on the selected group.
    --
    -- NOTE: This is called by the APEX "Apply Selection" button only.
    --       It records the user's selection.  FBDI insert and OIC
    --       submission happen later in xxemr_confirm_ai_match, which
    --       is triggered by "Process Selected Action" (BPM).
    IF p_match_group_id IS NOT NULL THEN

        UPDATE xxemr_match_groups
           SET user_action = 'REJECTED'
         WHERE statement_line_id = l_statement_line_id;

        UPDATE xxemr_match_groups
           SET user_action = 'ACCEPTED'
         WHERE match_group_id = p_match_group_id;

    END IF;

    COMMIT;

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        ROLLBACK;
        RAISE_APPLICATION_ERROR(-20010,
            'xxemr_apply_ai_match: Match group not found. '
            || 'MATCH_GROUP_ID=' || p_match_group_id);
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END xxemr_apply_ai_match;


-- ================================================================
-- PROCEDURE : xxemr_confirm_ai_match
-- Purpose   : Called by "Process Selected Action" (BPM action).
--             Finds the ACCEPTED match group for the statement line,
--             inserts FBDI staging rows for OIC dispatch, and
--             cleans up superseded EXT groups.
--
--             Kept separate from xxemr_apply_ai_match so that
--             "Apply Selection" (group picker) never triggers an
--             FBDI insert — only the explicit confirm action does.
-- ================================================================
PROCEDURE xxemr_confirm_ai_match (
    p_statement_line_id IN NUMBER,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
) IS
    l_match_group_id  NUMBER;
    l_candidates      VARCHAR2(4000);
BEGIN
    -- Step 1: Find the ACCEPTED match group for this line
    BEGIN
        SELECT match_group_id
          INTO l_match_group_id
          FROM xxemr_match_groups
         WHERE statement_line_id = p_statement_line_id
           AND user_action        = 'ACCEPTED'
           AND ROWNUM             = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20014,
                'xxemr_confirm_ai_match: No ACCEPTED match group found for '
                || 'LINE_ID=' || p_statement_line_id
                || '. User must select a group via Apply Selection first.');
    END;

    -- Step 2: Build candidate token list from match group details
    SELECT LISTAGG(candidate_source || candidate_id, ':')
           WITHIN GROUP (ORDER BY candidate_id)
      INTO l_candidates
      FROM xxemr_match_group_details
     WHERE match_group_id = l_match_group_id;

    -- Step 3: Insert FBDI staging rows — source of truth for OIC.
    --         Done before EXT cleanup so a failure rolls back cleanly.
    xxemr_insert_fbdi_lines(
        p_statement_line_id => p_statement_line_id,
        p_candidates        => l_candidates,
        p_recon_method      => 'AI',
        p_match_group_id    => l_match_group_id,
        p_user              => p_user
    );

    -- Step 4: Clean up any EXT_PENDING / EXT_PARTIAL / EXT_CREATED groups
    --         superseded now that an AI match is confirmed.
    DELETE FROM xxemr_match_group_details
     WHERE match_group_id IN (
               SELECT match_group_id FROM xxemr_match_groups
                WHERE statement_line_id = p_statement_line_id
                  AND match_type IN ('EXT_PENDING','EXT_PARTIAL','EXT_CREATED')
                  AND NVL(user_action,'PENDING') <> 'ACTION_TAKEN'
           );

    DELETE FROM xxemr_match_groups
     WHERE statement_line_id = p_statement_line_id
       AND match_type IN ('EXT_PENDING','EXT_PARTIAL','EXT_CREATED')
       AND NVL(user_action,'PENDING') <> 'ACTION_TAKEN';

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END xxemr_confirm_ai_match;



-- ================================================================
-- SECTION 5 — EXTERNAL TRANSACTION PROCESSING
-- ================================================================

-- ----------------------------------------------------------------
-- PRIVATE: xxemr_create_fusion_ext_transaction
-- Purpose : Calls Oracle Fusion CE cashExternalTransactions REST
--           API to create an external transaction in Fusion.
--           Reads endpoint + credentials from apex_recon_config.
--           Returns fusion transaction ID, HTTP status, raw response.
-- ----------------------------------------------------------------
PROCEDURE xxemr_create_fusion_ext_transaction (
    p_bank_account_name    IN  VARCHAR2,
    p_amount               IN  NUMBER,
    p_txn_type             IN  VARCHAR2,
    p_currency_code        IN  VARCHAR2,
    p_transaction_date     IN  DATE,
    p_reference_num        IN  VARCHAR2,
    p_description          IN  VARCHAR2,
    p_asset_account_combo  IN  VARCHAR2 DEFAULT NULL,
    p_offset_account_combo IN  VARCHAR2 DEFAULT NULL,
    x_fusion_txn_id        OUT VARCHAR2,
    x_api_status           OUT NUMBER,
    x_api_response         OUT CLOB
)
IS
    v_endpoint  VARCHAR2(500);
    v_username  VARCHAR2(200);
    v_password  VARCHAR2(200);
    v_payload   VARCHAR2(4000);
    l_resp_json JSON_OBJECT_T;
BEGIN
    SELECT config_value INTO v_endpoint FROM apex_recon_config WHERE config_key = 'FUSION_CE_EXT_TXN_ENDPOINT';
    SELECT config_value INTO v_username FROM apex_recon_config WHERE config_key = 'FUSION_API_USERNAME';
    SELECT config_value INTO v_password FROM apex_recon_config WHERE config_key = 'FUSION_API_PASSWORD';

    v_payload :=
           '{'
        || '"Amount":'           || p_amount                                                          || ','
        || '"BankAccountName":"' || REPLACE(NVL(p_bank_account_name,''),  '"','\"')                   || '",'
        || '"CurrencyCode":"'    || NVL(p_currency_code,'AED')                                        || '",'
        || '"Description":"'     || REPLACE(SUBSTR(NVL(p_description, ''),1,240), '"','\"')           || '",'
        || '"ReferenceText":"'   || REPLACE(SUBSTR(NVL(p_reference_num,''),1,100), '"','\"')          || '",'
        || '"TransactionType":"' || REPLACE(NVL(p_txn_type,''), '"','\"')                             || '",'
        || '"AccountingFlag":'   || 'false'                                                           || ','
        || '"TransactionDate":"' || TO_CHAR(p_transaction_date,'YYYY-MM-DD')                          || '"'
        || CASE WHEN p_asset_account_combo IS NOT NULL
                THEN ',"AssetAccountCombination":"'
                     || REPLACE(p_asset_account_combo, '"','\"') || '"'
                ELSE ''
           END
        || CASE WHEN p_offset_account_combo IS NOT NULL
                THEN ',"OffsetAccountCombination":"'
                     || REPLACE(p_offset_account_combo, '"','\"') || '"'
                ELSE ''
           END
        || '}';

    apex_web_service.g_request_headers.DELETE;
    apex_web_service.g_request_headers(1).name  := 'Content-Type';
    apex_web_service.g_request_headers(1).value := 'application/json';

    log_step(NULL, NULL, 'FUSION_PAYLOAD', 'DEBUG', v_payload);

    x_api_response := apex_web_service.make_rest_request(
        p_url         => v_endpoint,
        p_http_method => 'POST',
        p_username    => v_username,
        p_password    => v_password,
        p_body        => v_payload
    );

    x_api_status := apex_web_service.g_status_code;

    IF x_api_response IS NULL OR LENGTH(TRIM(x_api_response)) = 0 THEN
        x_api_status   := NVL(x_api_status, -1);
        x_api_response := 'ERROR: Empty response from Fusion API. HTTP Status: ' || x_api_status;
        RETURN;
    END IF;

    IF x_api_status IN (200, 201) THEN
        BEGIN
            l_resp_json := JSON_OBJECT_T.parse(x_api_response);
            IF    l_resp_json.has('ExternalTransactionId')  THEN x_fusion_txn_id := TO_CHAR(l_resp_json.get_Number('ExternalTransactionId'));
            ELSIF l_resp_json.has('TransactionNumber')      THEN x_fusion_txn_id := l_resp_json.get_String('TransactionNumber');
            ELSIF l_resp_json.has('transactionNumber')      THEN x_fusion_txn_id := l_resp_json.get_String('transactionNumber');
            ELSIF l_resp_json.has('CashTransactionNumber')  THEN x_fusion_txn_id := l_resp_json.get_String('CashTransactionNumber');
            ELSIF l_resp_json.has('CashFlowId')             THEN x_fusion_txn_id := TO_CHAR(l_resp_json.get_Number('CashFlowId'));
            END IF;
        EXCEPTION
            WHEN OTHERS THEN x_fusion_txn_id := NULL;
        END;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        x_api_status   := NVL(x_api_status, -1);
        x_api_response := 'apex_web_service error: ' || SUBSTR(SQLERRM, 1, 500);
        RAISE;
END xxemr_create_fusion_ext_transaction;


-- ----------------------------------------------------------------
-- PRIVATE: xxemr_run_keyword_check
-- Purpose : Scans statement line descriptions against
--           XXEMR_KEYWORD_MAPPING and sets external_flag,
--           matched_keyword, is_profit_withdrawal on each line.
--           DAILY mode  — PW keywords only, today's lines.
--           MONTH_END   — all keywords, all pending lines.
-- ----------------------------------------------------------------
PROCEDURE xxemr_run_keyword_check (
    p_bank_account_id IN VARCHAR2,
    p_bank_name       IN VARCHAR2,
    p_run_id          IN NUMBER,
    p_mode            IN VARCHAR2
)
IS
    CURSOR c_lines_daily IS
        SELECT statement_line_id, description, reference_num, amount, statement_date
          FROM XXEMR_BANK_STATEMENT_LINES
         WHERE bank_account_id       = p_bank_account_id
           AND process_date          = TRUNC(SYSDATE)
           AND pw_keyword_check_done = 'N'
         ORDER BY statement_line_id;

    CURSOR c_lines_month_end IS
        SELECT statement_line_id, description, reference_num, amount, statement_date
          FROM XXEMR_BANK_STATEMENT_LINES
         WHERE bank_account_id         = p_bank_account_id
           AND full_keyword_check_done = 'N'
         ORDER BY statement_line_id;

    CURSOR c_kw_pw_only IS
        SELECT keywords              AS keyword_value,
               transaction_code_type AS ext_txn_type,
               is_profit_withdrawal,
               keyword_priority
          FROM XXEMR_KEYWORD_MAPPING
         WHERE enabled_flag         = 'Y'
           AND is_profit_withdrawal = 'Y'
           AND (   bank IS NULL
                OR INSTR(UPPER(p_bank_name), UPPER(bank)) > 0
                OR INSTR(UPPER(bank), UPPER(p_bank_name)) > 0)
         ORDER BY keyword_priority NULLS LAST, keywords;

    CURSOR c_kw_all IS
        SELECT keywords              AS keyword_value,
               transaction_code_type AS ext_txn_type,
               is_profit_withdrawal,
               keyword_priority
          FROM XXEMR_KEYWORD_MAPPING
         WHERE enabled_flag = 'Y'
           AND (   bank IS NULL
                OR INSTR(UPPER(p_bank_name), UPPER(bank)) > 0
                OR INSTR(UPPER(bank), UPPER(p_bank_name)) > 0)
         ORDER BY keyword_priority NULLS LAST, keywords;

    v_ext_flag             VARCHAR2(1)   := 'N';
    v_matched_keyword      VARCHAR2(100) := NULL;
    v_is_profit_withdrawal VARCHAR2(1)   := 'N';

    PROCEDURE process_one_line (
        p_statement_line_id IN NUMBER,
        p_description       IN VARCHAR2
    )
    IS
        v_error_message VARCHAR2(4000);
    BEGIN
        v_ext_flag             := 'N';
        v_matched_keyword      := NULL;
        v_is_profit_withdrawal := 'N';

        SAVEPOINT sp_kw_line;

        BEGIN
            IF p_mode = 'DAILY' THEN
                DECLARE
                    v_best_priority NUMBER := 999999;
                    v_best_length   NUMBER := 0;
                BEGIN
                    FOR kw IN c_kw_pw_only LOOP
                        IF INSTR(UPPER(p_description), UPPER(kw.keyword_value)) > 0 THEN
                            IF    NVL(kw.keyword_priority, 999999) < v_best_priority
                               OR (    NVL(kw.keyword_priority, 999999) = v_best_priority
                                   AND LENGTH(kw.keyword_value) > v_best_length)
                            THEN
                                v_best_priority        := NVL(kw.keyword_priority, 999999);
                                v_best_length          := LENGTH(kw.keyword_value);
                                v_ext_flag             := 'Y';
                                v_matched_keyword      := kw.keyword_value;
                                v_is_profit_withdrawal := kw.is_profit_withdrawal;
                            END IF;
                        END IF;
                    END LOOP;
                END;

                UPDATE XXEMR_BANK_STATEMENT_LINES
                   SET external_flag         = v_ext_flag,
                       matched_keyword       = v_matched_keyword,
                       is_profit_withdrawal  = v_is_profit_withdrawal,
                       pw_keyword_check_done = 'Y',
                       last_updated          = SYSTIMESTAMP
                 WHERE statement_line_id     = p_statement_line_id;

            ELSE
                DECLARE
                    v_best_priority NUMBER := 999999;
                    v_best_length   NUMBER := 0;
                BEGIN
                    FOR kw IN c_kw_all LOOP
                        IF INSTR(UPPER(p_description), UPPER(kw.keyword_value)) > 0 THEN
                            IF    NVL(kw.keyword_priority, 999999) < v_best_priority
                               OR (    NVL(kw.keyword_priority, 999999) = v_best_priority
                                   AND LENGTH(kw.keyword_value) > v_best_length)
                            THEN
                                v_best_priority        := NVL(kw.keyword_priority, 999999);
                                v_best_length          := LENGTH(kw.keyword_value);
                                v_ext_flag             := 'Y';
                                v_matched_keyword      := kw.keyword_value;
                                v_is_profit_withdrawal := kw.is_profit_withdrawal;
                            END IF;
                        END IF;
                    END LOOP;
                END;

                UPDATE XXEMR_BANK_STATEMENT_LINES
                   SET external_flag           =
                           CASE WHEN external_flag = 'Y' THEN 'Y' ELSE v_ext_flag END,
                       matched_keyword         =
                           CASE WHEN external_flag = 'Y' AND matched_keyword IS NOT NULL
                                THEN matched_keyword ELSE v_matched_keyword END,
                       is_profit_withdrawal    =
                           CASE WHEN external_flag = 'Y' AND is_profit_withdrawal = 'Y'
                                THEN 'Y' ELSE v_is_profit_withdrawal END,
                       full_keyword_check_done = 'Y',
                       last_updated            = SYSTIMESTAMP
                 WHERE statement_line_id       = p_statement_line_id;
            END IF;

            COMMIT;

            log_step(p_run_id, p_statement_line_id,
                'KEYWORD_CHECK_' || p_mode,
                CASE v_ext_flag WHEN 'Y' THEN 'EXT_TXN_IDENTIFIED' ELSE 'NOT_EXT_TXN' END,
                'Mode: ' || p_mode || ' | Keyword: ' || NVL(v_matched_keyword,'NONE')
                || ' | PW: ' || v_is_profit_withdrawal);

        EXCEPTION
            WHEN OTHERS THEN
                v_error_message := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000);
                ROLLBACK TO sp_kw_line;
                IF p_mode = 'DAILY' THEN
                    UPDATE XXEMR_BANK_STATEMENT_LINES
                       SET pw_keyword_check_done = 'E', last_updated = SYSTIMESTAMP
                     WHERE statement_line_id = p_statement_line_id;
                ELSE
                    UPDATE XXEMR_BANK_STATEMENT_LINES
                       SET full_keyword_check_done = 'E', last_updated = SYSTIMESTAMP
                     WHERE statement_line_id = p_statement_line_id;
                END IF;
                COMMIT;
                log_step(p_run_id, p_statement_line_id,
                    'KEYWORD_CHECK_' || p_mode, 'FAILED', v_error_message);
        END;
    END process_one_line;

BEGIN
    IF p_mode = 'DAILY' THEN
        FOR rec IN c_lines_daily LOOP
            process_one_line(rec.statement_line_id, rec.description);
        END LOOP;
    ELSE
        FOR rec IN c_lines_month_end LOOP
            process_one_line(rec.statement_line_id, rec.description);
        END LOOP;
    END IF;
END xxemr_run_keyword_check;


-- ----------------------------------------------------------------
-- PUBLIC: xxemr_process_external_transactions
-- ----------------------------------------------------------------
PROCEDURE xxemr_process_external_transactions
IS
    CURSOR c_bank_accounts_daily IS
        SELECT l.bank_account_id,
               MIN(h.bank_name) AS bank_name
          FROM XXEMR_BANK_STATEMENT_LINES   l
          JOIN XXEMR_BANK_STATEMENT_HEADERS h
            ON h.statement_header_id = l.statement_header_id
         WHERE l.process_date          = TRUNC(SYSDATE)
           AND l.pw_keyword_check_done = 'N'
         GROUP BY l.bank_account_id
         ORDER BY l.bank_account_id;

    CURSOR c_bank_accounts_month_end IS
        SELECT l.bank_account_id,
               MIN(h.bank_name) AS bank_name
          FROM XXEMR_BANK_STATEMENT_LINES   l
          JOIN XXEMR_BANK_STATEMENT_HEADERS h
            ON h.statement_header_id = l.statement_header_id
         WHERE (l.full_keyword_check_done = 'N'
                OR (l.external_flag = 'Y' AND l.month_end_check_done = 'N'))
           AND NVL(l.recon_status,'UNR') <> 'REC'
         GROUP BY l.bank_account_id
         ORDER BY l.bank_account_id;

    CURSOR c_pw_lines (p_bank_account_id VARCHAR2) IS
        SELECT statement_line_id, reference_num, amount, currency_code,
               bank_account_id, statement_date, description, matched_keyword
          FROM XXEMR_BANK_STATEMENT_LINES
         WHERE bank_account_id       = p_bank_account_id
           AND external_flag         = 'Y'
           AND is_profit_withdrawal  = 'Y'
           AND pw_keyword_check_done = 'Y'
           AND pw_check_done         = 'N'
           AND process_date          = TRUNC(SYSDATE)
           AND NVL(recon_status,'UNR') <> 'REC'
         ORDER BY statement_line_id;

    CURSOR c_me_lines (p_bank_account_id VARCHAR2) IS
        SELECT statement_line_id, reference_num, amount, currency_code,
               bank_account_id, statement_date, description, matched_keyword,
               is_profit_withdrawal
          FROM XXEMR_BANK_STATEMENT_LINES
         WHERE bank_account_id           = p_bank_account_id
           AND external_flag             = 'Y'
           AND month_end_check_done      = 'N'
           AND NVL(recon_status,'UNR')  <> 'REC'
         ORDER BY statement_line_id;

    v_run_mode           VARCHAR2(20)  := NULL;
    v_manual_flag        VARCHAR2(1)   := 'N';
    v_last_day           DATE          := NULL;
    v_last_work_day      DATE          := NULL;
    v_receipt_found      VARCHAR2(1)   := 'N';
    v_ext_txn_found      VARCHAR2(1)   := 'N';
    v_matched_amount     NUMBER        := NULL;
    v_matched_receipt_id NUMBER        := NULL;
    v_variance           NUMBER        := NULL;
    v_variance_pct       NUMBER        := NULL;
    v_tolerance          NUMBER        := NULL;
    v_fusion_ext_tx_id   VARCHAR2(100) := NULL;
    v_work_count         PLS_INTEGER   := 0;
    v_pw_kw_count        PLS_INTEGER   := 0;
    v_all_kw_count       PLS_INTEGER   := 0;
    v_run_id             NUMBER        := NULL;
    v_error_message      VARCHAR2(4000):= NULL;

    e_no_pw_keywords  EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_no_pw_keywords,  -20101);
    e_no_all_keywords EXCEPTION;
    PRAGMA EXCEPTION_INIT(e_no_all_keywords, -20102);

BEGIN

    /*==========================================================
      BLOCK 1 — DERIVE RUN MODE
    ==========================================================*/
    BEGIN
        SELECT config_value INTO v_manual_flag
          FROM apex_recon_config WHERE config_key = 'MONTH_END_TRIGGER';
    EXCEPTION WHEN NO_DATA_FOUND THEN v_manual_flag := 'N';
    END;

    IF v_manual_flag = 'Y' THEN
        v_run_mode := 'MONTH_END';
    ELSE
        v_last_day      := LAST_DAY(TRUNC(SYSDATE));
        v_last_work_day := v_last_day;
        WHILE TO_CHAR(v_last_work_day,'D') IN ('1','7') LOOP
            v_last_work_day := v_last_work_day - 1;
        END LOOP;
        v_run_mode := CASE TRUNC(SYSDATE) WHEN v_last_work_day THEN 'MONTH_END' ELSE 'DAILY' END;
    END IF;

    /*==========================================================
      BLOCK 2 — PRE-CHECKS
    ==========================================================*/
    IF v_run_mode = 'DAILY' THEN
        SELECT COUNT(*) INTO v_work_count
          FROM XXEMR_BANK_STATEMENT_LINES
         WHERE process_date = TRUNC(SYSDATE) AND pw_keyword_check_done = 'N'
           AND NVL(recon_status,'UNR') <> 'REC';
    ELSE
        SELECT COUNT(*) INTO v_work_count
          FROM XXEMR_BANK_STATEMENT_LINES
         WHERE (full_keyword_check_done = 'N'
                OR (external_flag = 'Y' AND month_end_check_done = 'N'))
           AND NVL(recon_status,'UNR') <> 'REC';
    END IF;

    IF v_work_count = 0 THEN
        log_step(NULL, NULL, 'PRE_CHECK', 'NO_WORK',
            'No unprocessed lines for mode: ' || v_run_mode
            || ' on ' || TO_CHAR(SYSDATE,'DD-MON-YYYY') || '. Process exited cleanly.');
        RETURN;
    END IF;

    SELECT COUNT(*) INTO v_pw_kw_count
      FROM XXEMR_KEYWORD_MAPPING
     WHERE enabled_flag = 'Y' AND is_profit_withdrawal = 'Y';

    IF v_pw_kw_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20101,
            'No active PROFIT WITHDRAWAL keywords found. Process aborted.');
    END IF;

    IF v_run_mode = 'MONTH_END' THEN
        SELECT COUNT(*) INTO v_all_kw_count
          FROM XXEMR_KEYWORD_MAPPING WHERE enabled_flag = 'Y';
        IF v_all_kw_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20102,
                'No active keywords found for month end check. Process aborted.');
        END IF;
    END IF;

    /*==========================================================
      BLOCK 3 — CREATE RUN LOG
    ==========================================================*/
    INSERT INTO apex_bot_run_log (run_type, run_start, run_mode, status, triggered_by)
    VALUES ('EXT_TXN_PROCESS', SYSTIMESTAMP, v_run_mode, 'RUNNING', 'SYSTEM_AUTO')
    RETURNING run_id INTO v_run_id;
    COMMIT;

    log_step(v_run_id, NULL, 'RUN_START', 'SUCCESS',
        'Mode: ' || v_run_mode || ' | Override: ' || v_manual_flag
        || ' | Date: ' || TO_CHAR(SYSDATE,'DD-MON-YYYY')
        || ' | PW keywords: ' || v_pw_kw_count
        || ' | All keywords: ' || NVL(TO_CHAR(v_all_kw_count),'N/A (daily)'));

    /*==========================================================
      BLOCK 4 — MAIN PROCESSING
    ==========================================================*/

    IF v_run_mode = 'DAILY' THEN

        FOR ba IN c_bank_accounts_daily LOOP
            log_step(v_run_id, NULL, 'DAILY_BANK_START', 'INFO',
                'Bank: ' || ba.bank_account_id || ' | BankName: ' || ba.bank_name);

            xxemr_run_keyword_check(ba.bank_account_id, ba.bank_name, v_run_id, 'DAILY');

            FOR pw IN c_pw_lines(ba.bank_account_id) LOOP
                v_receipt_found      := 'N';
                v_ext_txn_found      := 'N';
                v_fusion_ext_tx_id   := NULL;
                v_matched_receipt_id := NULL;

                SAVEPOINT sp_pw_line;
                BEGIN
                    v_matched_amount := NULL;
                    v_variance       := NULL;
                    v_variance_pct   := NULL;
                    v_tolerance      := LEAST(pw.amount * 0.02, 500);

                    /* CHECK 1A: AR RECEIPT — EXACT MATCH */
                    BEGIN
                        SELECT 'Y', amount
                          INTO v_receipt_found, v_matched_amount
                          FROM XXEMR_AR_CASH_RECEIPTS
                         WHERE REMITTANCE_BANK_ACCOUNT_ID = pw.bank_account_id
                           AND receipt_number             = pw.reference_num
                           AND currency_code              = pw.currency_code
                           AND amount                     = pw.amount
                           AND receipt_date               BETWEEN pw.statement_date - 30
                                                              AND pw.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_receipt_found := 'N';
                    END;

                    IF v_receipt_found = 'Y' THEN
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET pw_check_done      = 'Y',
                               pw_action          = 'RECEIPT_EXISTS',
                               recon_status       = 'REC',
                               dashboard_flag     = 'N',
                               dashboard_message  = NULL,
                               match_variance     = 0,
                               match_variance_pct = 0,
                               last_updated       = SYSTIMESTAMP
                         WHERE statement_line_id = pw.statement_line_id;
                        COMMIT;
                        log_step(v_run_id, pw.statement_line_id, 'PW_RECEIPT_CHECK', 'EXACT_MATCH',
                            'Exact receipt match. Ref: ' || pw.reference_num || ' | Amt: ' || pw.amount);
                        GOTO next_pw;
                    END IF;

                    /* CHECK 1B: AR RECEIPT — PARTIAL MATCH */
                    BEGIN
                        SELECT 'Y', amount, cash_receipt_id
                          INTO v_receipt_found, v_matched_amount, v_matched_receipt_id
                          FROM XXEMR_AR_CASH_RECEIPTS
                         WHERE REMITTANCE_BANK_ACCOUNT_ID = pw.bank_account_id
                           AND receipt_number             = pw.reference_num
                           AND currency_code              = pw.currency_code
                           AND ABS(amount - pw.amount)    <= v_tolerance
                           AND receipt_date               BETWEEN pw.statement_date - 30
                                                              AND pw.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_receipt_found := 'N';
                    END;

                    IF v_receipt_found = 'Y' THEN
                        v_variance     := ABS(pw.amount - v_matched_amount);
                        v_variance_pct := ROUND((v_variance / pw.amount) * 100, 2);
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET pw_check_done      = 'Y',
                               pw_action          = 'PARTIAL_MATCH',
                               dashboard_flag     = 'Y',
                               match_variance     = v_variance,
                               match_variance_pct = v_variance_pct,
                               dashboard_message  =
                                   'Profit Withdrawal: Receipt found but amount differs. '
                                   || 'Statement: ' || pw.amount
                                   || ' | Receipt: '    || v_matched_amount
                                   || ' | Difference: ' || v_variance
                                   || ' (' || v_variance_pct || '%). '
                                   || 'Ref: ' || pw.reference_num
                                   || '. Please review and approve or reject.',
                               approval_status    = 'PENDING',
                               last_updated       = SYSTIMESTAMP
                         WHERE statement_line_id = pw.statement_line_id;

                        xxemr_insert_match_group(
                            p_statement_line_id => pw.statement_line_id,
                            p_candidates        => 'AR' || TO_CHAR(v_matched_receipt_id),
                            p_match_type        => 'EXT_PARTIAL',
                            p_match_score       => 0,
                            p_ranking           => 1,
                            p_amt_diff          => v_variance,
                            p_date_diff         => NULL,
                            p_user              => 'SYSTEM'
                        );

                        COMMIT;
                        log_step(v_run_id, pw.statement_line_id, 'PW_RECEIPT_CHECK', 'PARTIAL_MATCH',
                            'Variance: ' || v_variance || ' (' || v_variance_pct || '%). Ref: ' || pw.reference_num
                            || ' | MatchGroup created. ReceiptID: ' || v_matched_receipt_id);
                        GOTO next_pw;
                    END IF;

                    /* CHECK 2A: EXT TRANSACTION — EXACT MATCH */
                    BEGIN
                        SELECT 'Y', fusion_ext_txn_id
                          INTO v_ext_txn_found, v_fusion_ext_tx_id
                          FROM XXEMR_EXTERNAL_TRANSACTIONS
                         WHERE bank_account_id = pw.bank_account_id
                           AND reference_num   = pw.reference_num
                           AND currency_code   = pw.currency_code
                           AND amount          = pw.amount
                           AND transaction_date BETWEEN pw.statement_date - 30
                                                    AND pw.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_ext_txn_found := 'N';
                    END;

                    IF v_ext_txn_found = 'Y' THEN
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET pw_check_done      = 'Y',
                               pw_action          = 'EXT_TXN_EXISTS',
                               recon_status       = 'REC',
                               external_txn_id    = v_fusion_ext_tx_id,
                               dashboard_flag     = 'N',
                               dashboard_message  = NULL,
                               match_variance     = 0,
                               match_variance_pct = 0,
                               last_updated       = SYSTIMESTAMP
                         WHERE statement_line_id = pw.statement_line_id;
                        COMMIT;
                        log_step(v_run_id, pw.statement_line_id, 'PW_EXT_TXN_CHECK', 'EXACT_MATCH',
                            'Exact ext txn match. Fusion ID: ' || v_fusion_ext_tx_id);
                        GOTO next_pw;
                    END IF;

                    /* CHECK 2B: EXT TRANSACTION — PARTIAL MATCH */
                    BEGIN
                        SELECT 'Y', fusion_ext_txn_id, amount
                          INTO v_ext_txn_found, v_fusion_ext_tx_id, v_matched_amount
                          FROM XXEMR_EXTERNAL_TRANSACTIONS
                         WHERE bank_account_id        = pw.bank_account_id
                           AND reference_num           = pw.reference_num
                           AND currency_code           = pw.currency_code
                           AND ABS(amount - pw.amount) <= v_tolerance
                           AND transaction_date        BETWEEN pw.statement_date - 30
                                                           AND pw.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_ext_txn_found := 'N';
                    END;

                    IF v_ext_txn_found = 'Y' THEN
                        v_variance     := ABS(pw.amount - v_matched_amount);
                        v_variance_pct := ROUND((v_variance / pw.amount) * 100, 2);
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET pw_check_done      = 'Y',
                               pw_action          = 'PARTIAL_MATCH',
                               external_txn_id    = v_fusion_ext_tx_id,
                               dashboard_flag     = 'Y',
                               match_variance     = v_variance,
                               match_variance_pct = v_variance_pct,
                               dashboard_message  =
                                   'Profit Withdrawal: External transaction found but amount differs. '
                                   || 'Statement: ' || pw.amount
                                   || ' | Ext Txn: '   || v_matched_amount
                                   || ' | Difference: ' || v_variance
                                   || ' (' || v_variance_pct || '%). '
                                   || 'Fusion ID: ' || v_fusion_ext_tx_id
                                   || '. Please review and approve or reject.',
                               approval_status    = 'PENDING',
                               last_updated       = SYSTIMESTAMP
                         WHERE statement_line_id = pw.statement_line_id;

                        INSERT INTO xxemr_match_groups (
                            match_group_id, statement_line_id, total_match_amount,
                            difference_amount, match_score, match_type, ranking,
                            candidate_source, created_date, creation_date, created_by,
                            last_update_date, last_updated_by
                        ) VALUES (
                            xxemr_match_groups_seq.nextval, pw.statement_line_id, v_matched_amount,
                            v_variance, 0, 'EXT_PARTIAL', 1,
                            'EXT', SYSDATE, SYSTIMESTAMP, 'SYSTEM',
                            SYSTIMESTAMP, 'SYSTEM'
                        );

                        COMMIT;
                        log_step(v_run_id, pw.statement_line_id, 'PW_EXT_TXN_CHECK', 'PARTIAL_MATCH',
                            'Variance: ' || v_variance || ' (' || v_variance_pct || '%). Fusion ID: ' || v_fusion_ext_tx_id
                            || ' | MatchGroup created.');
                        GOTO next_pw;
                    END IF;

                    /* NOTHING FOUND → PENDING_APPROVAL → EXT_PENDING match group */
                    UPDATE XXEMR_BANK_STATEMENT_LINES
                       SET pw_check_done      = 'Y',
                           pw_action          = 'PENDING_APPROVAL',
                           dashboard_flag     = 'Y',
                           match_variance     = NULL,
                           match_variance_pct = NULL,
                           dashboard_message  =
                               'Profit Withdrawal: No receipt or external transaction found for reference '
                               || pw.reference_num || ', amount ' || pw.amount
                               || '. Approval required to create external transaction in Fusion.',
                           approval_status    = 'PENDING',
                           last_updated       = SYSTIMESTAMP
                     WHERE statement_line_id = pw.statement_line_id;

                    INSERT INTO xxemr_match_groups (
                        match_group_id, statement_line_id, total_match_amount,
                        difference_amount, match_score, match_type, ranking,
                        candidate_source, created_date, creation_date, created_by,
                        last_update_date, last_updated_by
                    ) VALUES (
                        xxemr_match_groups_seq.nextval, pw.statement_line_id, 0,
                        pw.amount, 0, 'EXT_PENDING', 1,
                        'EXT', SYSDATE, SYSTIMESTAMP, 'SYSTEM',
                        SYSTIMESTAMP, 'SYSTEM'
                    );

                    COMMIT;
                    log_step(v_run_id, pw.statement_line_id, 'PW_FLAGGED', 'PENDING',
                        'No match found. Ref: ' || pw.reference_num
                        || ' | Amt: ' || pw.amount || ' | Tolerance: ' || v_tolerance
                        || ' | MatchGroup created as EXT_PENDING.');

                    <<next_pw>> NULL;

                EXCEPTION
                    WHEN OTHERS THEN
                        v_error_message := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000);
                        ROLLBACK TO sp_pw_line;
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET pw_check_done = 'E',
                               pw_action     = 'ERROR',
                               last_updated  = SYSTIMESTAMP
                         WHERE statement_line_id = pw.statement_line_id;
                        COMMIT;
                        log_step(v_run_id, pw.statement_line_id,
                            'PW_CHECK_EXCEPTION', 'FAILED', v_error_message);
                END;
            END LOOP;
        END LOOP;


    ELSIF v_run_mode = 'MONTH_END' THEN

        log_step(v_run_id, NULL, 'ME_PASS1_START', 'INFO',
            'Month end Pass 1: keyword check for any new rows today.');

        FOR ba IN c_bank_accounts_daily LOOP
            log_step(v_run_id, NULL, 'ME_PASS1_BANK', 'INFO',
                'Pass 1 - Bank: ' || ba.bank_account_id || ' | BankName: ' || ba.bank_name);
            xxemr_run_keyword_check(ba.bank_account_id, ba.bank_name, v_run_id, 'DAILY');
        END LOOP;

        log_step(v_run_id, NULL, 'ME_PASS1_END', 'SUCCESS', 'Pass 1 complete. Starting Pass 2.');

        FOR ba IN c_bank_accounts_month_end LOOP
            log_step(v_run_id, NULL, 'ME_PASS2_BANK', 'INFO',
                'Pass 2 - Bank: ' || ba.bank_account_id || ' | BankName: ' || ba.bank_name);

            xxemr_run_keyword_check(ba.bank_account_id, ba.bank_name, v_run_id, 'MONTH_END');

            FOR me IN c_me_lines(ba.bank_account_id) LOOP
                v_receipt_found      := 'N';
                v_ext_txn_found      := 'N';
                v_fusion_ext_tx_id   := NULL;
                v_matched_receipt_id := NULL;

                SAVEPOINT sp_me_line;
                BEGIN
                    v_matched_amount := NULL;
                    v_variance       := NULL;
                    v_variance_pct   := NULL;
                    v_tolerance      := LEAST(me.amount * 0.02, 500);

                    /* CHECK 1A: AR RECEIPT — EXACT MATCH */
                    BEGIN
                        SELECT 'Y', amount
                          INTO v_receipt_found, v_matched_amount
                          FROM XXEMR_AR_CASH_RECEIPTS
                         WHERE REMITTANCE_BANK_ACCOUNT_ID = me.bank_account_id
                           AND receipt_number             = me.reference_num
                           AND currency_code              = me.currency_code
                           AND amount                     = me.amount
                           AND receipt_date               BETWEEN me.statement_date - 30
                                                              AND me.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_receipt_found := 'N';
                    END;

                    IF v_receipt_found = 'Y' THEN
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET month_end_check_done = 'Y',
                               month_end_action     = 'RECEIPT_EXISTS',
                               recon_status         = 'REC',
                               dashboard_flag       = 'N',
                               dashboard_message    = NULL,
                               match_variance       = 0,
                               match_variance_pct   = 0,
                               last_updated         = SYSTIMESTAMP
                         WHERE statement_line_id = me.statement_line_id;
                        COMMIT;
                        log_step(v_run_id, me.statement_line_id, 'ME_RECEIPT_CHECK', 'EXACT_MATCH',
                            'Exact receipt match. Ref: ' || me.reference_num || ' | Amt: ' || me.amount);
                        GOTO next_me;
                    END IF;

                    /* CHECK 1B: AR RECEIPT — PARTIAL MATCH */
                    BEGIN
                        SELECT 'Y', amount, cash_receipt_id
                          INTO v_receipt_found, v_matched_amount, v_matched_receipt_id
                          FROM XXEMR_AR_CASH_RECEIPTS
                         WHERE REMITTANCE_BANK_ACCOUNT_ID = me.bank_account_id
                           AND receipt_number             = me.reference_num
                           AND currency_code              = me.currency_code
                           AND ABS(amount - me.amount)    <= v_tolerance
                           AND receipt_date               BETWEEN me.statement_date - 30
                                                              AND me.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_receipt_found := 'N';
                    END;

                    IF v_receipt_found = 'Y' THEN
                        v_variance     := ABS(me.amount - v_matched_amount);
                        v_variance_pct := ROUND((v_variance / me.amount) * 100, 2);
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET month_end_check_done = 'Y',
                               month_end_action     = 'PARTIAL_MATCH',
                               dashboard_flag       = 'Y',
                               match_variance       = v_variance,
                               match_variance_pct   = v_variance_pct,
                               dashboard_message    =
                                   'Month End: Receipt found but amount differs. '
                                   || 'Statement: ' || me.amount
                                   || ' | Receipt: '    || v_matched_amount
                                   || ' | Difference: ' || v_variance
                                   || ' (' || v_variance_pct || '%). '
                                   || CASE me.is_profit_withdrawal
                                          WHEN 'Y' THEN '(Profit Withdrawal) ' ELSE '' END
                                   || 'Ref: ' || me.reference_num
                                   || '. Please review and approve or reject.',
                               approval_status      = 'PENDING',
                               last_updated         = SYSTIMESTAMP
                         WHERE statement_line_id = me.statement_line_id;

                        xxemr_insert_match_group(
                            p_statement_line_id => me.statement_line_id,
                            p_candidates        => 'AR' || TO_CHAR(v_matched_receipt_id),
                            p_match_type        => 'EXT_PARTIAL',
                            p_match_score       => 0,
                            p_ranking           => 1,
                            p_amt_diff          => v_variance,
                            p_date_diff         => NULL,
                            p_user              => 'SYSTEM'
                        );

                        COMMIT;
                        log_step(v_run_id, me.statement_line_id, 'ME_RECEIPT_CHECK', 'PARTIAL_MATCH',
                            'Variance: ' || v_variance || ' (' || v_variance_pct || '%). Ref: ' || me.reference_num
                            || ' | MatchGroup created. ReceiptID: ' || v_matched_receipt_id);
                        GOTO next_me;
                    END IF;

                    /* CHECK 2A: EXT TRANSACTION — EXACT MATCH */
                    BEGIN
                        SELECT 'Y', fusion_ext_txn_id
                          INTO v_ext_txn_found, v_fusion_ext_tx_id
                          FROM XXEMR_EXTERNAL_TRANSACTIONS
                         WHERE bank_account_id  = me.bank_account_id
                           AND reference_num    = me.reference_num
                           AND currency_code    = me.currency_code
                           AND amount           = me.amount
                           AND transaction_date BETWEEN me.statement_date - 30
                                                    AND me.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_ext_txn_found := 'N';
                    END;

                    IF v_ext_txn_found = 'Y' THEN
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET month_end_check_done = 'Y',
                               month_end_action     = 'EXT_TXN_EXISTS',
                               recon_status         = 'REC',
                               external_txn_id      = v_fusion_ext_tx_id,
                               dashboard_flag       = 'N',
                               dashboard_message    = NULL,
                               match_variance       = 0,
                               match_variance_pct   = 0,
                               last_updated         = SYSTIMESTAMP
                         WHERE statement_line_id = me.statement_line_id;
                        COMMIT;
                        log_step(v_run_id, me.statement_line_id, 'ME_EXT_TXN_CHECK', 'EXACT_MATCH',
                            'Exact ext txn match. Fusion ID: ' || v_fusion_ext_tx_id);
                        GOTO next_me;
                    END IF;

                    /* CHECK 2B: EXT TRANSACTION — PARTIAL MATCH */
                    BEGIN
                        SELECT 'Y', fusion_ext_txn_id, amount
                          INTO v_ext_txn_found, v_fusion_ext_tx_id, v_matched_amount
                          FROM XXEMR_EXTERNAL_TRANSACTIONS
                         WHERE bank_account_id        = me.bank_account_id
                           AND reference_num           = me.reference_num
                           AND currency_code           = me.currency_code
                           AND ABS(amount - me.amount) <= v_tolerance
                           AND transaction_date        BETWEEN me.statement_date - 30
                                                           AND me.statement_date + 5
                           AND ROWNUM = 1;
                    EXCEPTION WHEN NO_DATA_FOUND THEN v_ext_txn_found := 'N';
                    END;

                    IF v_ext_txn_found = 'Y' THEN
                        v_variance     := ABS(me.amount - v_matched_amount);
                        v_variance_pct := ROUND((v_variance / me.amount) * 100, 2);
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET month_end_check_done = 'Y',
                               month_end_action     = 'PARTIAL_MATCH',
                               external_txn_id      = v_fusion_ext_tx_id,
                               dashboard_flag       = 'Y',
                               match_variance       = v_variance,
                               match_variance_pct   = v_variance_pct,
                               dashboard_message    =
                                   'Month End: External transaction found but amount differs. '
                                   || 'Statement: ' || me.amount
                                   || ' | Ext Txn: '   || v_matched_amount
                                   || ' | Difference: ' || v_variance
                                   || ' (' || v_variance_pct || '%). '
                                   || CASE me.is_profit_withdrawal
                                          WHEN 'Y' THEN '(Profit Withdrawal) ' ELSE '' END
                                   || 'Fusion ID: ' || v_fusion_ext_tx_id
                                   || '. Please review and approve or reject.',
                               approval_status      = 'PENDING',
                               last_updated         = SYSTIMESTAMP
                         WHERE statement_line_id = me.statement_line_id;

                        INSERT INTO xxemr_match_groups (
                            match_group_id, statement_line_id, total_match_amount,
                            difference_amount, match_score, match_type, ranking,
                            candidate_source, created_date, creation_date, created_by,
                            last_update_date, last_updated_by
                        ) VALUES (
                            xxemr_match_groups_seq.nextval, me.statement_line_id, v_matched_amount,
                            v_variance, 0, 'EXT_PARTIAL', 1,
                            'EXT', SYSDATE, SYSTIMESTAMP, 'SYSTEM',
                            SYSTIMESTAMP, 'SYSTEM'
                        );

                        COMMIT;
                        log_step(v_run_id, me.statement_line_id, 'ME_EXT_TXN_CHECK', 'PARTIAL_MATCH',
                            'Variance: ' || v_variance || ' (' || v_variance_pct || '%). Fusion ID: ' || v_fusion_ext_tx_id
                            || ' | MatchGroup created.');
                        GOTO next_me;
                    END IF;

                    /* NOTHING FOUND → PENDING_APPROVAL → EXT_PENDING match group */
                    UPDATE XXEMR_BANK_STATEMENT_LINES
                       SET month_end_check_done = 'Y',
                           month_end_action     = 'PENDING_APPROVAL',
                           dashboard_flag       = 'Y',
                           match_variance       = NULL,
                           match_variance_pct   = NULL,
                           dashboard_message    =
                               'Month End: No receipt or external transaction found for reference '
                               || me.reference_num || ', amount ' || me.amount
                               || CASE me.is_profit_withdrawal
                                      WHEN 'Y' THEN ' (Profit Withdrawal)' ELSE '' END
                               || '. Approval required to create external transaction in Fusion.',
                           approval_status      = 'PENDING',
                           last_updated         = SYSTIMESTAMP
                     WHERE statement_line_id = me.statement_line_id;

                    INSERT INTO xxemr_match_groups (
                        match_group_id, statement_line_id, total_match_amount,
                        difference_amount, match_score, match_type, ranking,
                        candidate_source, created_date, creation_date, created_by,
                        last_update_date, last_updated_by
                    ) VALUES (
                        xxemr_match_groups_seq.nextval, me.statement_line_id, 0,
                        me.amount, 0, 'EXT_PENDING', 1,
                        'EXT', SYSDATE, SYSTIMESTAMP, 'SYSTEM',
                        SYSTIMESTAMP, 'SYSTEM'
                    );

                    COMMIT;
                    log_step(v_run_id, me.statement_line_id, 'ME_FLAGGED', 'PENDING',
                        'No match found. PW: ' || me.is_profit_withdrawal
                        || ' | Ref: ' || me.reference_num
                        || ' | Amt: ' || me.amount
                        || ' | Tolerance: ' || v_tolerance
                        || ' | MatchGroup created as EXT_PENDING.');

                    <<next_me>> NULL;

                EXCEPTION
                    WHEN OTHERS THEN
                        v_error_message := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000);
                        ROLLBACK TO sp_me_line;
                        UPDATE XXEMR_BANK_STATEMENT_LINES
                           SET month_end_check_done = 'E',
                               month_end_action     = 'ERROR',
                               last_updated         = SYSTIMESTAMP
                         WHERE statement_line_id = me.statement_line_id;
                        COMMIT;
                        log_step(v_run_id, me.statement_line_id,
                            'ME_CHECK_EXCEPTION', 'FAILED', v_error_message);
                END;
            END LOOP;
        END LOOP;

    END IF;

    /*==========================================================
      BLOCK 5 — POST-PROCESSING
    ==========================================================*/
    IF v_run_mode = 'MONTH_END' THEN
        UPDATE apex_recon_config
           SET config_value = 'N',
               last_updated = SYSTIMESTAMP
         WHERE config_key   = 'MONTH_END_TRIGGER';
        log_step(v_run_id, NULL, 'ME_TRIGGER_RESET', 'SUCCESS', 'MONTH_END_TRIGGER reset to N.');
    END IF;

    UPDATE apex_bot_run_log
       SET status  = 'COMPLETED',
           run_end = SYSTIMESTAMP
     WHERE run_id  = v_run_id;
    COMMIT;
    log_step(v_run_id, NULL, 'RUN_END', 'SUCCESS', 'Process completed. Mode: ' || v_run_mode);

EXCEPTION
    WHEN e_no_pw_keywords THEN
        v_error_message := SUBSTR(SQLERRM, 1, 4000);
        IF v_run_id IS NOT NULL THEN
            UPDATE apex_bot_run_log SET status = 'FAILED', error_detail = v_error_message,
                run_end = SYSTIMESTAMP WHERE run_id = v_run_id;
            COMMIT;
        END IF;
        log_step(v_run_id, NULL, 'RUN_END', 'FAILED', v_error_message);
        RAISE;

    WHEN e_no_all_keywords THEN
        v_error_message := DBMS_UTILITY.FORMAT_ERROR_STACK;
        IF v_run_id IS NOT NULL THEN
            UPDATE apex_bot_run_log SET status = 'FAILED', error_detail = v_error_message,
                run_end = SYSTIMESTAMP WHERE run_id = v_run_id;
            COMMIT;
        END IF;
        log_step(v_run_id, NULL, 'RUN_END', 'FAILED', v_error_message);
        RAISE;

    WHEN OTHERS THEN
        v_error_message := SUBSTR(SQLERRM, 1, 4000);
        IF v_run_id IS NOT NULL THEN
            UPDATE apex_bot_run_log SET status = 'FAILED', error_detail = v_error_message,
                run_end = SYSTIMESTAMP WHERE run_id = v_run_id;
            COMMIT;
        END IF;
        log_step(v_run_id, NULL, 'RUN_END', 'FAILED', v_error_message);
        RAISE;

END xxemr_process_external_transactions;


-- ----------------------------------------------------------------
-- PUBLIC: xxemr_create_pw_external_transaction
-- ----------------------------------------------------------------
PROCEDURE xxemr_create_pw_external_transaction (p_statement_line_id IN NUMBER)
IS
    v_api_status        NUMBER         := NULL;
    v_api_response      CLOB           := NULL;
    v_fusion_ext_tx_id  VARCHAR2(100)  := NULL;
    v_error_message     VARCHAR2(4000) := NULL;
    v_stmt              XXEMR_BANK_STATEMENT_LINES%ROWTYPE;
    v_bank_account_name VARCHAR2(500)  := NULL;
    v_bank_name         VARCHAR2(500)  := NULL;
    v_txn_type          VARCHAR2(100)  := NULL;
    v_creation_amount   NUMBER         := NULL;
    v_asset_account     VARCHAR2(500)  := NULL;
    v_offset_account    VARCHAR2(500)  := NULL;
BEGIN
    BEGIN
        SELECT s.* INTO v_stmt
          FROM xxemr_bank_statement_lines s
         WHERE s.statement_line_id = p_statement_line_id
           AND NVL(s.ext_tx_created_flag,'N') <> 'Y'
           AND EXISTS (
                   SELECT 1 FROM xxemr_match_groups g
                    WHERE g.statement_line_id = s.statement_line_id
                      AND g.match_type IN ('EXT_PENDING','EXT_PARTIAL')
               )
        FOR UPDATE NOWAIT;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20201,
                'Statement line ' || p_statement_line_id
                || ' not found in PENDING state. It may have already been approved or rejected.');
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20202,
                'Could not lock statement line ' || p_statement_line_id
                || '. Another session may be processing it. Please try again in a moment.');
    END;

    BEGIN
        SELECT h.bank_account_name, h.bank_name
          INTO v_bank_account_name, v_bank_name
          FROM XXEMR_BANK_STATEMENT_HEADERS h
         WHERE h.statement_header_id = v_stmt.statement_header_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_bank_account_name := NULL;
            v_bank_name         := NULL;
    END;

    UPDATE XXEMR_BANK_STATEMENT_LINES
       SET approval_status = 'APPROVED',
           approved_by    = NVL(V('APP_USER'), 'SYSTEM'),
           approved_date  = SYSDATE,
           last_updated   = SYSTIMESTAMP
     WHERE statement_line_id = p_statement_line_id;
    COMMIT;

    DECLARE
        v_escrow_flag VARCHAR2(10);
    BEGIN
        SELECT NVL(h.escrow_account,'N')
          INTO v_escrow_flag
          FROM XXEMR_BANK_STATEMENT_HEADERS h
         WHERE h.statement_header_id = v_stmt.statement_header_id;

        IF v_escrow_flag = 'Y' THEN
            v_txn_type := v_stmt.trx_type;
        ELSE
            SELECT CASE
                       WHEN INSTR(k.transaction_code_type,'/') > 0
                       THEN SUBSTR(k.transaction_code_type,1,INSTR(k.transaction_code_type,'/')-1)
                       ELSE k.transaction_code_type
                   END
              INTO v_txn_type
              FROM XXEMR_KEYWORD_MAPPING k
             WHERE k.enabled_flag = 'Y'
               AND NVL(k.escrow,'N') = 'N'
               AND UPPER(TRIM(k.keywords)) = UPPER(TRIM(v_stmt.matched_keyword))
               AND (   k.bank IS NULL
                    OR INSTR(UPPER(TRIM(v_bank_name)),UPPER(TRIM(k.bank))) > 0
                    OR INSTR(UPPER(TRIM(k.bank)),UPPER(TRIM(v_bank_name))) > 0)
             ORDER BY k.keyword_priority NULLS LAST
             FETCH FIRST 1 ROW ONLY;
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN v_txn_type := v_stmt.trx_type;
    END;

    v_creation_amount :=
        CASE UPPER(v_stmt.flow_indicator)
            WHEN 'DBIT' THEN -1 * ABS(v_stmt.amount)
            WHEN 'CRDT' THEN      ABS(v_stmt.amount)
            ELSE                       v_stmt.amount
        END;

    -- Fetch asset and offset account combinations from bank details
    BEGIN
        SELECT concatenated_segments, offset_account_combination
          INTO v_asset_account, v_offset_account
          FROM xxemr_bank_details
         WHERE bank_account_id = v_stmt.bank_account_id
           AND ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_asset_account  := NULL;
            v_offset_account := NULL;
    END;

    xxemr_create_fusion_ext_transaction(
        p_bank_account_name    => v_bank_account_name,
        p_amount               => v_creation_amount,
        p_txn_type             => v_txn_type,
        p_currency_code        => v_stmt.currency_code,
        p_transaction_date     => v_stmt.statement_date,
        p_reference_num        => v_stmt.reference_num,
        p_description          => v_stmt.description,
        p_asset_account_combo  => v_asset_account,
        p_offset_account_combo => v_offset_account,
        x_fusion_txn_id        => v_fusion_ext_tx_id,
        x_api_status           => v_api_status,
        x_api_response         => v_api_response
    );

    IF v_api_status IN (200, 201) THEN
        UPDATE XXEMR_BANK_STATEMENT_LINES
           SET pw_action           = 'EXT_TXN_CREATED',
               ext_tx_created_flag = 'Y',
               external_txn_id    = v_fusion_ext_tx_id,
               ai_created_flag    = 'Y',
               dashboard_flag     = 'N',
               dashboard_message  = NULL,
               recon_status       = 'REC',
               last_updated       = SYSTIMESTAMP
         WHERE statement_line_id = p_statement_line_id;

        UPDATE xxemr_match_groups
           SET match_type       = 'EXT_CREATED',
               user_action      = 'ACTION_TAKEN',
               last_update_date = SYSTIMESTAMP,
               last_updated_by  = NVL(V('APP_USER'), 'SYSTEM')
         WHERE statement_line_id = p_statement_line_id
           AND match_type        IN ('EXT_PENDING', 'EXT_PARTIAL');

        COMMIT;
        log_step(NULL, p_statement_line_id, 'PW_CREATE', 'SUCCESS',
            'Fusion ID: ' || v_fusion_ext_tx_id
            || ' | HTTP: '        || v_api_status
            || ' | TxnType: '     || v_txn_type
            || ' | Flow: '        || NVL(v_stmt.flow_indicator,'N/A')
            || ' | CreationAmt: ' || v_creation_amount
            || ' | Bank: '        || v_bank_account_name);
    ELSE
        UPDATE XXEMR_BANK_STATEMENT_LINES
           SET pw_action          = 'CREATE_FAILED',
               approval_status   = 'PENDING',
               dashboard_message = 'Creation failed (HTTP ' || v_api_status
                   || '). Please retry or contact support.',
               last_updated      = SYSTIMESTAMP
         WHERE statement_line_id = p_statement_line_id;
        COMMIT;
        log_step(NULL, p_statement_line_id, 'PW_CREATE', 'FAILED',
            'HTTP: ' || v_api_status || ' | '
            || SUBSTR(DBMS_LOB.SUBSTR(v_api_response, 500, 1), 1, 500));
        RAISE_APPLICATION_ERROR(-20203,
            'Fusion API returned HTTP ' || v_api_status || '. Line reset for retry.');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000);
        BEGIN
            UPDATE XXEMR_BANK_STATEMENT_LINES
               SET pw_action       = 'CREATE_FAILED',
                   approval_status = 'PENDING',
                   last_updated    = SYSTIMESTAMP
             WHERE statement_line_id = p_statement_line_id
               AND pw_action        != 'EXT_TXN_CREATED';
            COMMIT;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
        log_step(NULL, p_statement_line_id, 'PW_CREATE_EXCEPTION', 'FAILED', v_error_message);
        RAISE;
END xxemr_create_pw_external_transaction;


-- ----------------------------------------------------------------
-- PUBLIC: xxemr_create_me_external_transaction
-- ----------------------------------------------------------------
PROCEDURE xxemr_create_me_external_transaction (p_statement_line_id IN NUMBER)
IS
    v_api_status        NUMBER         := NULL;
    v_api_response      CLOB           := NULL;
    v_fusion_ext_tx_id  VARCHAR2(100)  := NULL;
    v_error_message     VARCHAR2(4000) := NULL;
    v_stmt              XXEMR_BANK_STATEMENT_LINES%ROWTYPE;
    v_bank_account_name VARCHAR2(500)  := NULL;
    v_bank_name         VARCHAR2(500)  := NULL;
    v_txn_type          VARCHAR2(100)  := NULL;
    v_creation_amount   NUMBER         := NULL;
    v_asset_account     VARCHAR2(500)  := NULL;
    v_offset_account    VARCHAR2(500)  := NULL;
BEGIN
    BEGIN
        SELECT s.* INTO v_stmt
          FROM xxemr_bank_statement_lines s
         WHERE s.statement_line_id = p_statement_line_id
           AND NVL(s.ext_tx_created_flag,'N') <> 'Y'
           AND EXISTS (
                   SELECT 1 FROM xxemr_match_groups g
                    WHERE g.statement_line_id = s.statement_line_id
                      AND g.match_type IN ('EXT_PENDING','EXT_PARTIAL')
               )
        FOR UPDATE NOWAIT;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20301,
                'Statement line ' || p_statement_line_id
                || ' not found in month end PENDING state. It may have already been processed.');
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20302,
                'Could not lock statement line ' || p_statement_line_id
                || '. Another session may be processing it. Please try again in a moment.');
    END;

    BEGIN
        SELECT h.bank_account_name, h.bank_name
          INTO v_bank_account_name, v_bank_name
          FROM XXEMR_BANK_STATEMENT_HEADERS h
         WHERE h.statement_header_id = v_stmt.statement_header_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_bank_account_name := NULL;
            v_bank_name         := NULL;
    END;

    UPDATE XXEMR_BANK_STATEMENT_LINES
       SET approval_status = 'APPROVED',
           approved_by    = NVL(V('APP_USER'), 'SYSTEM'),
           approved_date  = SYSDATE,
           last_updated   = SYSTIMESTAMP
     WHERE statement_line_id = p_statement_line_id;
    COMMIT;

    DECLARE
        v_escrow_flag VARCHAR2(10);
    BEGIN
        SELECT NVL(h.escrow_account,'N')
          INTO v_escrow_flag
          FROM XXEMR_BANK_STATEMENT_HEADERS h
         WHERE h.statement_header_id = v_stmt.statement_header_id;

        IF v_escrow_flag = 'Y' THEN
            v_txn_type := v_stmt.trx_type;
        ELSE
            SELECT CASE
                       WHEN INSTR(k.transaction_code_type,'/') > 0
                       THEN SUBSTR(k.transaction_code_type,1,INSTR(k.transaction_code_type,'/')-1)
                       ELSE k.transaction_code_type
                   END
              INTO v_txn_type
              FROM XXEMR_KEYWORD_MAPPING k
             WHERE k.enabled_flag = 'Y'
               AND NVL(k.escrow,'N') = 'N'
               AND UPPER(TRIM(k.keywords)) = UPPER(TRIM(v_stmt.matched_keyword))
               AND (   k.bank IS NULL
                    OR INSTR(UPPER(TRIM(v_bank_name)),UPPER(TRIM(k.bank))) > 0
                    OR INSTR(UPPER(TRIM(k.bank)),UPPER(TRIM(v_bank_name))) > 0)
             ORDER BY k.keyword_priority NULLS LAST
             FETCH FIRST 1 ROW ONLY;
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN v_txn_type := v_stmt.trx_type;
    END;

    v_creation_amount :=
        CASE UPPER(v_stmt.flow_indicator)
            WHEN 'DBIT' THEN -1 * ABS(v_stmt.amount)
            WHEN 'CRDT' THEN      ABS(v_stmt.amount)
            ELSE                       v_stmt.amount
        END;

    -- Fetch asset and offset account combinations from bank details
    BEGIN
        SELECT concatenated_segments, offset_account_combination
          INTO v_asset_account, v_offset_account
          FROM xxemr_bank_details
         WHERE bank_account_id = v_stmt.bank_account_id
           AND ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_asset_account  := NULL;
            v_offset_account := NULL;
    END;

    xxemr_create_fusion_ext_transaction(
        p_bank_account_name    => v_bank_account_name,
        p_amount               => v_creation_amount,
        p_txn_type             => v_txn_type,
        p_currency_code        => v_stmt.currency_code,
        p_transaction_date     => v_stmt.statement_date,
        p_reference_num        => v_stmt.reference_num,
        p_description          => v_stmt.description,
        p_asset_account_combo  => v_asset_account,
        p_offset_account_combo => v_offset_account,
        x_fusion_txn_id        => v_fusion_ext_tx_id,
        x_api_status           => v_api_status,
        x_api_response         => v_api_response
    );

    IF v_api_status IN (200, 201) THEN
        UPDATE XXEMR_BANK_STATEMENT_LINES
           SET month_end_action    = 'EXT_TXN_CREATED',
               ext_tx_created_flag = 'Y',
               external_txn_id    = v_fusion_ext_tx_id,
               ai_created_flag    = 'Y',
               dashboard_flag     = 'N',
               dashboard_message  = NULL,
               recon_status       = 'REC',
               last_updated       = SYSTIMESTAMP
         WHERE statement_line_id = p_statement_line_id;

        -- Clean up superseded AI suggestions now that external transaction is created.
        DELETE FROM xxemr_match_group_details
         WHERE match_group_id IN (
                   SELECT match_group_id FROM xxemr_match_groups
                    WHERE statement_line_id = p_statement_line_id
                      AND match_type IN ('ONE_TO_ONE','ONE_TO_MANY')
                      AND NVL(user_action,'PENDING') <> 'ACTION_TAKEN'
               );

        DELETE FROM xxemr_match_groups
         WHERE statement_line_id = p_statement_line_id
           AND match_type IN ('ONE_TO_ONE','ONE_TO_MANY')
           AND NVL(user_action,'PENDING') <> 'ACTION_TAKEN';

        UPDATE xxemr_match_groups
           SET match_type       = 'EXT_CREATED',
               user_action      = 'ACTION_TAKEN',
               last_update_date = SYSTIMESTAMP,
               last_updated_by  = NVL(V('APP_USER'), 'SYSTEM')
         WHERE statement_line_id = p_statement_line_id
           AND match_type        IN ('EXT_PENDING', 'EXT_PARTIAL');

        -- Clean up any ONE_TO_ONE / ONE_TO_MANY AI suggestions that
        -- are now superseded by the external transaction creation.
        DELETE FROM xxemr_match_group_details
         WHERE match_group_id IN (
                   SELECT match_group_id FROM xxemr_match_groups
                    WHERE statement_line_id = p_statement_line_id
                      AND match_type IN ('ONE_TO_ONE','ONE_TO_MANY')
                      AND NVL(user_action,'PENDING') <> 'ACTION_TAKEN'
               );

        DELETE FROM xxemr_match_groups
         WHERE statement_line_id = p_statement_line_id
           AND match_type IN ('ONE_TO_ONE','ONE_TO_MANY')
           AND NVL(user_action,'PENDING') <> 'ACTION_TAKEN';

        COMMIT;
        log_step(NULL, p_statement_line_id, 'ME_CREATE', 'SUCCESS',
            'Fusion ID: ' || v_fusion_ext_tx_id
            || ' | HTTP: '        || v_api_status
            || ' | PW: '          || v_stmt.is_profit_withdrawal
            || ' | TxnType: '     || v_txn_type
            || ' | Flow: '        || NVL(v_stmt.flow_indicator,'N/A')
            || ' | CreationAmt: ' || v_creation_amount
            || ' | Bank: '        || v_bank_account_name);
    ELSE
        UPDATE XXEMR_BANK_STATEMENT_LINES
           SET month_end_action  = 'CREATE_FAILED',
               approval_status  = 'PENDING',
               dashboard_message = 'Creation failed (HTTP ' || v_api_status
                   || '). Please retry or contact support.',
               last_updated     = SYSTIMESTAMP
         WHERE statement_line_id = p_statement_line_id;
        COMMIT;
        log_step(NULL, p_statement_line_id, 'ME_CREATE', 'FAILED',
            'HTTP: ' || v_api_status || ' | '
            || SUBSTR(DBMS_LOB.SUBSTR(v_api_response, 500, 1), 1, 500));
        RAISE_APPLICATION_ERROR(-20303,
            'Fusion API returned HTTP ' || v_api_status || '. Line reset for retry.');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SUBSTR(DBMS_UTILITY.FORMAT_ERROR_STACK, 1, 4000);
        BEGIN
            UPDATE XXEMR_BANK_STATEMENT_LINES
               SET month_end_action = 'CREATE_FAILED',
                   approval_status  = 'PENDING',
                   last_updated     = SYSTIMESTAMP
             WHERE statement_line_id = p_statement_line_id
               AND month_end_action != 'EXT_TXN_CREATED';
            COMMIT;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
        log_step(NULL, p_statement_line_id, 'ME_CREATE_EXCEPTION', 'FAILED', v_error_message);
        RAISE;
END xxemr_create_me_external_transaction;


-- ================================================================
-- SECTION 6 — ONE-TO-ONE AI MATCHING ENGINE
-- ================================================================

PROCEDURE xxemr_suggest_one_to_one_match (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_suggest_one_to_one_match
-- Purpose   : Scores and ranks AR receipt candidates for a single
--             statement line using weighted scoring:
--               Amount similarity    60%
--               Date proximity       25%
--               Reference similarity 15%
--             Returns top-N candidates via ref cursor.
-- Source    : Ported from standalone xxemr_recon_pkg (Doc 3).
-- ----------------------------------------------------------------
    p_statement_line_id IN  NUMBER,
    p_top_n             IN  NUMBER   DEFAULT 3,
    p_date_window_days  IN  NUMBER   DEFAULT 20,
    p_amount_tolerance  IN  NUMBER   DEFAULT 0.5,
    p_result            OUT SYS_REFCURSOR
) IS
    l_stmt_amount     xxemr_bank_statement_lines.amount%TYPE;
    l_stmt_date       xxemr_bank_statement_lines.statement_date%TYPE;
    l_bank_account_id xxemr_bank_statement_lines.bank_account_id%TYPE;
    l_currency_code   xxemr_bank_statement_lines.currency_code%TYPE;
    l_stmt_ref_norm   VARCHAR2(4000);
BEGIN
    BEGIN
        SELECT s.amount,
               TRUNC(s.statement_date),
               s.bank_account_id,
               s.currency_code,
               REGEXP_REPLACE(LOWER(NVL(s.reference_num, '')), '[^a-z0-9]', '')
          INTO l_stmt_amount,
               l_stmt_date,
               l_bank_account_id,
               l_currency_code,
               l_stmt_ref_norm
          FROM xxemr_bank_statement_lines s
         WHERE s.statement_line_id = p_statement_line_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001,
                'xxemr_suggest_one_to_one_match: Statement line not found: '
                || p_statement_line_id);
    END;

    OPEN p_result FOR
        WITH
        ar_raw AS (
            SELECT r.cash_receipt_id,
                   r.amount,
                   r.gl_date,
                   r.receipt_number,
                   REGEXP_REPLACE(
                       LOWER(NVL(r.receipt_number, '')),
                       '[^a-z0-9]', ''
                   ) AS rcpt_ref_norm
              FROM xxemr_ar_cash_receipts r
             WHERE r.remittance_bank_account_id = l_bank_account_id
               AND NVL(r.currency_code, 'NONE')  = NVL(l_currency_code, 'NONE')
               AND TRUNC(r.gl_date)
                       BETWEEN l_stmt_date - p_date_window_days
                           AND l_stmt_date + p_date_window_days
               AND NVL(r.match_flag,  'N') <> 'Y'
               AND NVL(r.recon_flag,  'N') <> 'Y'
               AND r.status NOT IN ('REV')
               AND ABS(l_stmt_amount - r.amount)
                       <= ABS(l_stmt_amount * p_amount_tolerance / 100)
        ),
        ar_candidates AS (
            SELECT
                'AR_RECEIPT'                        AS candidate_source,
                cash_receipt_id                     AS candidate_key,
                amount                              AS candidate_amount,
                ABS(l_stmt_amount - amount)         AS amount_diff,
                ABS(TRUNC(gl_date) - l_stmt_date)   AS date_diff_days,
                CASE
                    WHEN l_stmt_ref_norm IS NULL THEN 0
                    WHEN rcpt_ref_norm   IS NULL THEN 0
                    WHEN rcpt_ref_norm   = ''    THEN 0
                    ELSE UTL_MATCH.EDIT_DISTANCE_SIMILARITY(
                             l_stmt_ref_norm, rcpt_ref_norm)
                END                                 AS reference_score,
                CASE
                    WHEN l_stmt_ref_norm IS NULL THEN 'N'
                    WHEN rcpt_ref_norm   IS NULL THEN 'N'
                    WHEN rcpt_ref_norm   = ''    THEN 'N'
                    WHEN REGEXP_LIKE(
                             l_stmt_ref_norm,
                             '(^|[^0-9a-z])' || rcpt_ref_norm || '([^0-9a-z]|$)'
                         )                     THEN 'Y'
                    ELSE 'N'
                END                                 AS desc_match_flag,
                'AR Receipt one-to-one'             AS explanation
            FROM ar_raw
        ),
        ar_scored AS (
            SELECT
                candidate_source,
                candidate_key,
                candidate_amount,
                amount_diff,
                date_diff_days,
                reference_score,
                desc_match_flag,
                explanation,
                ROUND(
                    (100 - LEAST(100,
                        amount_diff / GREATEST(ABS(l_stmt_amount), 1) * 100
                    )) * 0.60
                    +
                    (100 - LEAST(100,
                        date_diff_days * 100 / GREATEST(p_date_window_days, 1)
                    )) * 0.25
                    +
                    LEAST(100, reference_score) * 0.15,
                    2
                ) AS final_score
            FROM ar_candidates
        )
        SELECT *
          FROM (
            SELECT
                ROW_NUMBER() OVER (
                    ORDER BY final_score    DESC,
                             amount_diff,
                             date_diff_days,
                             reference_score DESC
                )                           AS candidate_rank,
                candidate_source,
                candidate_key,
                candidate_amount,
                amount_diff,
                date_diff_days,
                reference_score,
                desc_match_flag,
                final_score,
                explanation
              FROM ar_scored
          )
         WHERE candidate_rank <= p_top_n
         ORDER BY candidate_rank;

END xxemr_suggest_one_to_one_match;


PROCEDURE xxemr_run_one_to_one_batch (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_run_one_to_one_batch
-- Purpose   : Batch orchestrator for one-to-one AI matching.
--             Purges PENDING suggestions, preserves ACTION_TAKEN.
--             Calls xxemr_suggest_one_to_one_match per line and
--             persists results to match group tables.
-- Source    : Ported from standalone xxemr_recon_pkg (Doc 3).
--             Fix applied: candidate_source added to
--             xxemr_match_group_details INSERT.
-- ----------------------------------------------------------------
    p_bank_account_id  IN  NUMBER,
    p_date_window_days IN  NUMBER   DEFAULT 20,
    p_amount_tolerance IN  NUMBER   DEFAULT 0.5,
    p_top_n            IN  NUMBER   DEFAULT 3,
    p_created_by       IN  VARCHAR2 DEFAULT 'SYSTEM'
) IS
    CURSOR c_stmt_lines IS
        SELECT  l.statement_line_id,
                l.amount,
                l.statement_date
          FROM  xxemr_bank_statement_lines   l
          JOIN  xxemr_bank_statement_headers h
            ON  h.statement_header_id = l.statement_header_id
         WHERE  h.bank_account_id         = p_bank_account_id
           AND  NVL(l.match_flag,  'N')   = 'N'
           AND  NVL(l.recon_status,'UNR') = 'UNR';

    l_result            SYS_REFCURSOR;
    l_candidate_rank    NUMBER;
    l_candidate_source  VARCHAR2(30);
    l_candidate_key     VARCHAR2(4000);
    l_candidate_amount  NUMBER;
    l_amount_diff       NUMBER;
    l_date_diff_days    NUMBER;
    l_reference_score   NUMBER;
    l_desc_match_flag   VARCHAR2(1);
    l_final_score       NUMBER;
    l_explanation       VARCHAR2(4000);
    l_match_group_id    NUMBER;
    l_detail_id         NUMBER;
    l_candidates_found  BOOLEAN;
    l_line_count        NUMBER := 0;
    l_matched_count     NUMBER := 0;
    l_unmatched_count   NUMBER := 0;
    l_suggestion_count  NUMBER := 0;

BEGIN
    DBMS_OUTPUT.PUT_LINE('=================================================');
    DBMS_OUTPUT.PUT_LINE('xxemr_run_one_to_one_batch started : '
        || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Bank Account  : ' || p_bank_account_id);
    DBMS_OUTPUT.PUT_LINE('=================================================');

    -- Purge PENDING / REJECTED — preserve ACTION_TAKEN
    DELETE FROM xxemr_match_group_details d
     WHERE d.match_group_id IN (
               SELECT g.match_group_id
                 FROM xxemr_match_groups g
                WHERE g.statement_line_id IN (
                          SELECT l.statement_line_id
                            FROM xxemr_bank_statement_lines   l
                            JOIN xxemr_bank_statement_headers h
                              ON h.statement_header_id = l.statement_header_id
                           WHERE h.bank_account_id = p_bank_account_id
                      )
                  AND NVL(g.user_action, 'PENDING') <> 'ACTION_TAKEN'
           );

    DELETE FROM xxemr_match_groups g
     WHERE g.statement_line_id IN (
               SELECT l.statement_line_id
                 FROM xxemr_bank_statement_lines   l
                 JOIN xxemr_bank_statement_headers h
                   ON h.statement_header_id = l.statement_header_id
                WHERE h.bank_account_id = p_bank_account_id
           )
       AND NVL(g.user_action, 'PENDING') <> 'ACTION_TAKEN';

    DBMS_OUTPUT.PUT_LINE('Purged PENDING suggestions. ACTION_TAKEN preserved.');

    FOR r_line IN c_stmt_lines LOOP

        l_line_count       := l_line_count + 1;
        l_candidates_found := FALSE;

        DBMS_OUTPUT.PUT_LINE(
            'Processing line : ' || r_line.statement_line_id ||
            ' | Amount : '       || r_line.amount            ||
            ' | Date : '         || TO_CHAR(r_line.statement_date, 'DD-MON-YYYY')
        );

        xxemr_suggest_one_to_one_match(
            p_statement_line_id => r_line.statement_line_id,
            p_top_n             => p_top_n,
            p_date_window_days  => p_date_window_days,
            p_amount_tolerance  => p_amount_tolerance,
            p_result            => l_result
        );

        LOOP
            FETCH l_result INTO
                l_candidate_rank, l_candidate_source, l_candidate_key,
                l_candidate_amount, l_amount_diff, l_date_diff_days,
                l_reference_score, l_desc_match_flag, l_final_score, l_explanation;

            EXIT WHEN l_result%NOTFOUND;

            l_candidates_found := TRUE;
            l_suggestion_count := l_suggestion_count + 1;

            SELECT XXEMR_MATCH_GROUPS_SEQ.NEXTVAL INTO l_match_group_id FROM DUAL;

            INSERT INTO xxemr_match_groups (
                match_group_id, statement_line_id, total_match_amount,
                difference_amount, match_score, match_type, ranking,
                user_action, candidate_source, created_by,
                creation_date, last_updated_by, last_update_date
            ) VALUES (
                l_match_group_id, r_line.statement_line_id, l_candidate_amount,
                l_amount_diff, l_final_score, 'ONE_TO_ONE', l_candidate_rank,
                'PENDING',
                CASE l_candidate_source
                    WHEN 'AR_RECEIPT'   THEN 'AR'
                    WHEN 'EXTERNAL_TXN' THEN 'EXT'
                    ELSE l_candidate_source
                END,
                p_created_by, SYSTIMESTAMP, p_created_by, SYSTIMESTAMP
            );

            SELECT XXEMR_MATCH_GRP_DTL_SEQ.NEXTVAL INTO l_detail_id FROM DUAL;

            INSERT INTO xxemr_match_group_details (
                match_group_detail_id, match_group_id,
                candidate_source,          -- fix: added, required by table
                candidate_id, amount, individual_score,
                created_by, creation_date, last_updated_by, last_update_date
            ) VALUES (
                l_detail_id, l_match_group_id,
                CASE l_candidate_source
                    WHEN 'AR_RECEIPT'   THEN 'AR'
                    WHEN 'EXTERNAL_TXN' THEN 'EXT'
                    ELSE l_candidate_source
                END,
                TO_NUMBER(l_candidate_key), l_candidate_amount, l_reference_score,
                p_created_by, SYSTIMESTAMP, p_created_by, SYSTIMESTAMP
            );

        END LOOP;

        CLOSE l_result;

        IF l_candidates_found THEN
            l_matched_count := l_matched_count + 1;
            DBMS_OUTPUT.PUT_LINE('  → Suggestions inserted.');
        ELSE
            l_unmatched_count := l_unmatched_count + 1;
            DBMS_OUTPUT.PUT_LINE('  → No candidates. Skipping line '
                || r_line.statement_line_id || '.');
        END IF;

    END LOOP;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('=================================================');
    DBMS_OUTPUT.PUT_LINE('xxemr_run_one_to_one_batch complete : '
        || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Lines processed     : ' || l_line_count);
    DBMS_OUTPUT.PUT_LINE('With matches        : ' || l_matched_count);
    DBMS_OUTPUT.PUT_LINE('No matches (skipped): ' || l_unmatched_count);
    DBMS_OUTPUT.PUT_LINE('Total suggestions   : ' || l_suggestion_count);
    DBMS_OUTPUT.PUT_LINE('=================================================');

EXCEPTION
    WHEN OTHERS THEN
        IF l_result%ISOPEN THEN CLOSE l_result; END IF;
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;

END xxemr_run_one_to_one_batch;


-- ================================================================
-- SECTION 7 — ONE-TO-MANY AI MATCHING ENGINE
-- ================================================================

PROCEDURE xxemr_suggest_line_matches (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_suggest_line_matches
-- Purpose   : Scores and ranks AR receipt candidates (including
--             multi-receipt combinations) and optionally external
--             transactions for a single statement line.
--             Recursive CTE builds all combos up to
--             min(p_max_combo_size, 5) receipts (hard cap).
--             Returns top-N candidates via ref cursor.
-- Source    : Ported from standalone xxemr_bank_recon_pkg (Doc 4).
--             Fix: NO_DATA_FOUND guard added on statement line
--             lookup — returns empty cursor rather than propagating
--             the exception to the calling batch loop.
-- ----------------------------------------------------------------
    p_statement_line_id    IN  NUMBER,
    p_top_n                IN  NUMBER   DEFAULT 3,
    p_date_window_days     IN  NUMBER   DEFAULT 15,
    p_amount_tolerance     IN  NUMBER   DEFAULT 0,
    p_max_combo_size       IN  NUMBER   DEFAULT 5,
    p_max_receipt_pool     IN  NUMBER   DEFAULT 15,
    p_include_external_txn IN  VARCHAR2 DEFAULT 'Y',
    p_allow_one_to_many    IN  VARCHAR2 DEFAULT 'Y',
    p_result               OUT SYS_REFCURSOR
) IS
    l_stmt_amount     xxemr_bank_statement_lines.amount%TYPE;
    l_stmt_date       xxemr_bank_statement_lines.statement_date%TYPE;
    l_bank_account_id xxemr_bank_statement_lines.bank_account_id%TYPE;
    l_stmt_ref_norm   VARCHAR2(4000);
BEGIN
    -- NO_DATA_FOUND guard: line may be already REC or not exist.
    -- Return empty cursor so caller loop moves on safely.
    BEGIN
        SELECT s.amount,
               TRUNC(s.statement_date),
               s.bank_account_id,
               REGEXP_REPLACE(LOWER(NVL(s.reference_num, '')), '[^a-z0-9]', '')
          INTO l_stmt_amount, l_stmt_date, l_bank_account_id, l_stmt_ref_norm
          FROM xxemr_bank_statement_lines s
         WHERE s.statement_line_id = p_statement_line_id
           AND NVL(s.recon_status, 'UNR') NOT IN ('REC');
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            OPEN p_result FOR
                SELECT 0, 'NONE', '0', 0, 0, 0, 0, 0, 'N/A', 0
                  FROM dual WHERE 1 = 0;
            RETURN;
    END;

    OPEN p_result FOR
        WITH
        receipt_pool AS (
            SELECT *
              FROM (
                SELECT r.cash_receipt_id,
                       r.amount,
                       TRUNC(r.gl_date)                                          receipt_date,
                       r.receipt_number,
                       ABS(l_stmt_amount - r.amount)                             amount_diff,
                       ABS(TRUNC(r.gl_date) - l_stmt_date)                      date_diff,
                       CASE
                           WHEN l_stmt_ref_norm IS NULL THEN 0
                           WHEN REGEXP_REPLACE(LOWER(NVL(r.receipt_number,'')),
                                               '[^a-z0-9]','') IS NULL THEN 0
                           ELSE UTL_MATCH.EDIT_DISTANCE_SIMILARITY(
                                    l_stmt_ref_norm,
                                    REGEXP_REPLACE(LOWER(NVL(r.receipt_number,'')),
                                                   '[^a-z0-9]',''))
                       END                                                       ref_score,
                       ROW_NUMBER() OVER (
                           ORDER BY
                               ABS(l_stmt_amount - r.amount),
                               ABS(TRUNC(r.gl_date) - l_stmt_date),
                               CASE
                                   WHEN l_stmt_ref_norm IS NULL THEN 0
                                   WHEN REGEXP_REPLACE(LOWER(NVL(r.receipt_number,'')),
                                                       '[^a-z0-9]','') IS NULL THEN 0
                                   ELSE UTL_MATCH.EDIT_DISTANCE_SIMILARITY(
                                            l_stmt_ref_norm,
                                            REGEXP_REPLACE(LOWER(NVL(r.receipt_number,'')),
                                                           '[^a-z0-9]',''))
                               END DESC,
                               r.cash_receipt_id
                       )                                                         rn
                  FROM xxemr_ar_cash_receipts r
                 WHERE r.remittance_bank_account_id = l_bank_account_id
                   AND TRUNC(r.gl_date) BETWEEN l_stmt_date - p_date_window_days
                                            AND l_stmt_date + p_date_window_days
                   AND NVL(r.match_flag,  'N') <> 'Y'
                   AND NVL(r.recon_flag,  'N') <> 'Y'
                   AND r.status NOT IN ('REVERSED')
              )
             WHERE rn <= p_max_receipt_pool
        ),
        -- Recursive combination builder, hard-capped at 5
        combos (last_rn, combo_size, ids, total_amount, max_date_diff, avg_ref_score) AS (
            SELECT rp.rn,
                   1,
                   TO_CHAR(rp.cash_receipt_id),
                   rp.amount,
                   rp.date_diff,
                   rp.ref_score
              FROM receipt_pool rp
            UNION ALL
            SELECT rp.rn,
                   c.combo_size + 1,
                   c.ids || ',' || TO_CHAR(rp.cash_receipt_id),
                   c.total_amount + rp.amount,
                   GREATEST(c.max_date_diff, rp.date_diff),
                   ((c.avg_ref_score * c.combo_size) + rp.ref_score) / (c.combo_size + 1)
              FROM combos c
              JOIN receipt_pool rp ON rp.rn > c.last_rn
             WHERE c.combo_size < CASE
                                      WHEN p_allow_one_to_many = 'Y'
                                      THEN LEAST(p_max_combo_size, 5)
                                      ELSE 1
                                  END
               AND c.total_amount + rp.amount <= l_stmt_amount  -- never over-sum
        ),
        receipt_combo_candidates AS (
            SELECT 'RECEIPT_COMBO'                                              AS candidate_source,
                   ids                                                          AS candidate_key,
                   total_amount                                                 AS candidate_amount,
                   ABS(l_stmt_amount - total_amount)                           AS amount_diff,
                   max_date_diff                                                AS max_date_diff_days,
                   ROUND(avg_ref_score, 2)                                      AS reference_score,
                   ROUND(
                       (100 - LEAST(100, ABS(l_stmt_amount - total_amount)
                                        / GREATEST(ABS(l_stmt_amount), 1) * 100)) * 0.60 +
                       (100 - LEAST(100, max_date_diff * 100
                                        / GREATEST(p_date_window_days, 1)))    * 0.25 +
                       LEAST(100, avg_ref_score)                               * 0.15 -
                       (combo_size - 1) * 2,
                       2
                   )                                                            AS final_score,
                   'Combo size=' || combo_size || ', receipts=' || ids         AS explanation,
                   combo_size
              FROM combos
             WHERE ABS(l_stmt_amount - total_amount) <= p_amount_tolerance
               AND (p_allow_one_to_many = 'Y' OR combo_size = 1)
        ),
        -- Special case: many same-amount receipts summing to statement
        receipt_group_candidates AS (
            SELECT 'RECEIPT_GROUP'                                              AS candidate_source,
                   LISTAGG(cash_receipt_id, ',')
                       WITHIN GROUP (ORDER BY cash_receipt_id)                 AS candidate_key,
                   SUM(amount)                                                  AS candidate_amount,
                   ABS(l_stmt_amount - SUM(amount))                            AS amount_diff,
                   MAX(date_diff)                                               AS max_date_diff_days,
                   ROUND(AVG(ref_score), 2)                                    AS reference_score,
                   ROUND(
                       (100 - LEAST(100, ABS(l_stmt_amount - SUM(amount))
                                        / GREATEST(ABS(l_stmt_amount), 1) * 100)) * 0.65 +
                       (100 - LEAST(100, MAX(date_diff) * 100
                                        / GREATEST(p_date_window_days, 1)))    * 0.20 +
                       LEAST(100, AVG(ref_score))                              * 0.15,
                       2
                   )                                                            AS final_score,
                   'Same-amount grouped receipts, cnt=' || COUNT(*)            AS explanation,
                   COUNT(*)                                                     AS combo_size
              FROM receipt_pool
             WHERE p_allow_one_to_many = 'Y'
             GROUP BY amount
            HAVING ABS(l_stmt_amount - SUM(amount)) <= p_amount_tolerance
               AND COUNT(*) >= 2
        ),
        external_candidates AS (
            SELECT 'EXTERNAL_TXN'                                              AS candidate_source,
                   TO_CHAR(e.ext_txn_id)                                       AS candidate_key,
                   e.amount                                                     AS candidate_amount,
                   ABS(l_stmt_amount - e.amount)                               AS amount_diff,
                   ABS(TRUNC(e.transaction_date) - l_stmt_date)                AS max_date_diff_days,
                   CASE
                       WHEN l_stmt_ref_norm IS NULL THEN 0
                       WHEN REGEXP_REPLACE(LOWER(NVL(e.reference_num,'')),
                                           '[^a-z0-9]','') IS NULL THEN 0
                       ELSE UTL_MATCH.EDIT_DISTANCE_SIMILARITY(
                                l_stmt_ref_norm,
                                REGEXP_REPLACE(LOWER(NVL(e.reference_num,'')),
                                               '[^a-z0-9]',''))
                   END                                                         AS reference_score,
                   ROUND(
                       (100 - LEAST(100, ABS(l_stmt_amount - e.amount)
                                        / GREATEST(ABS(l_stmt_amount), 1) * 100)) * 0.65 +
                       (100 - LEAST(100, ABS(TRUNC(e.transaction_date) - l_stmt_date) * 100
                                        / GREATEST(p_date_window_days, 1)))    * 0.20 +
                       LEAST(100,
                           CASE
                               WHEN l_stmt_ref_norm IS NULL THEN 0
                               WHEN REGEXP_REPLACE(LOWER(NVL(e.reference_num,'')),
                                                   '[^a-z0-9]','') IS NULL THEN 0
                               ELSE UTL_MATCH.EDIT_DISTANCE_SIMILARITY(
                                        l_stmt_ref_norm,
                                        REGEXP_REPLACE(LOWER(NVL(e.reference_num,'')),
                                                       '[^a-z0-9]',''))
                           END
                       )                                                       * 0.15,
                       2
                   )                                                           AS final_score,
                   'External transaction single match'                         AS explanation,
                   1                                                           AS combo_size
              FROM xxemr_external_transactions e
             WHERE p_include_external_txn         = 'Y'
               AND e.bank_account_id              = l_bank_account_id
               AND TRUNC(e.transaction_date) BETWEEN l_stmt_date - p_date_window_days
                                                 AND l_stmt_date + p_date_window_days
               AND NVL(e.recon_status, 'UNR') NOT IN ('REC')
               AND ABS(l_stmt_amount - e.amount)  <= p_amount_tolerance
        ),
        all_candidates AS (
            SELECT * FROM receipt_combo_candidates
            UNION ALL
            SELECT * FROM receipt_group_candidates
            UNION ALL
            SELECT * FROM external_candidates
        )
        SELECT *
          FROM (
            SELECT ROW_NUMBER() OVER (
                       ORDER BY final_score DESC, amount_diff, max_date_diff_days
                   )               AS candidate_rank,
                   candidate_source,
                   candidate_key,
                   candidate_amount,
                   amount_diff,
                   max_date_diff_days,
                   reference_score,
                   final_score,
                   explanation,
                   combo_size
              FROM all_candidates
          )
         WHERE candidate_rank <= p_top_n
         ORDER BY candidate_rank;

END xxemr_suggest_line_matches;


PROCEDURE xxemr_run_batch (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_run_batch
-- Purpose   : Reprocessing batch for one-to-many AI matching over
--             a specified date range and bank account.
--             Deletes PENDING suggestions (preserves ACTION_TAKEN
--             and MANUAL_RECON), calls xxemr_suggest_line_matches
--             per unmatched line, persists results.
-- Source    : Ported from standalone xxemr_bank_recon_pkg (Doc 4).
-- ----------------------------------------------------------------
    p_run_id               IN  NUMBER,
    p_bank_account_id      IN  NUMBER,
    p_stmt_from_date       IN  DATE,
    p_stmt_to_date         IN  DATE,
    p_top_n                IN  NUMBER   DEFAULT 3,
    p_date_window_days     IN  NUMBER   DEFAULT 15,
    p_amount_tolerance     IN  NUMBER   DEFAULT 0,
    p_max_combo_size       IN  NUMBER   DEFAULT 5,
    p_max_receipt_pool     IN  NUMBER   DEFAULT 15,
    p_commit_interval      IN  NUMBER   DEFAULT 200
) IS
    CURSOR c_stmt IS
        SELECT s.statement_line_id,
               NVL(s.external_flag,  'N') external_flag,
               NVL(s.dashboard_flag, 'N') dashboard_flag
          FROM xxemr_bank_statement_lines s
         WHERE s.bank_account_id = p_bank_account_id
           AND TRUNC(s.statement_date)
                   BETWEEN TRUNC(p_stmt_from_date) AND TRUNC(p_stmt_to_date)
           AND NVL(s.match_flag,   'N') <> 'Y'
           AND NVL(s.recon_status, 'UNR') NOT IN ('REC')
           AND NOT EXISTS (
                   SELECT 1 FROM xxemr_match_groups mg
                    WHERE mg.statement_line_id = s.statement_line_id
               );

    l_rc             SYS_REFCURSOR;
    l_rank           NUMBER;
    l_source         VARCHAR2(30);
    l_key            VARCHAR2(4000);
    l_amt            NUMBER;
    l_amt_diff       NUMBER;
    l_date_diff      NUMBER;
    l_ref_score      NUMBER;
    l_final_score    NUMBER;
    l_expl           VARCHAR2(4000);
    l_combo_size     NUMBER;
    l_count          NUMBER := 0;
    l_match_group_id NUMBER;
    l_match_type     VARCHAR2(30);
    l_token          VARCHAR2(200);
    l_pos            NUMBER;
    l_remaining      VARCHAR2(4000);
    l_err_msg        VARCHAR2(1000);
    l_indiv_amount   NUMBER;
    l_indiv_date     DATE;
    l_indiv_source   VARCHAR2(10);
    l_indiv_id       NUMBER;
    l_stmt_amount    NUMBER;
    l_candidates_found BOOLEAN;        -- TRUE when >=1 candidate fetched
BEGIN
    -- Delete PENDING suggestions; preserve ACTION_TAKEN and MANUAL_RECON
    DELETE FROM xxemr_match_group_details
     WHERE match_group_id IN (
               SELECT match_group_id
                 FROM xxemr_match_groups
                WHERE statement_line_id IN (
                          SELECT s.statement_line_id
                            FROM xxemr_bank_statement_lines s
                           WHERE s.bank_account_id = p_bank_account_id
                             AND TRUNC(s.statement_date)
                                     BETWEEN TRUNC(p_stmt_from_date)
                                         AND TRUNC(p_stmt_to_date)
                      )
                  AND NVL(user_action, 'PENDING')
                          NOT IN ('ACTION_TAKEN', 'MANUAL_RECON')
           );

    DELETE FROM xxemr_match_groups
     WHERE statement_line_id IN (
               SELECT s.statement_line_id
                 FROM xxemr_bank_statement_lines s
                WHERE s.bank_account_id = p_bank_account_id
                  AND TRUNC(s.statement_date)
                          BETWEEN TRUNC(p_stmt_from_date) AND TRUNC(p_stmt_to_date)
           )
       AND NVL(user_action, 'PENDING') NOT IN ('ACTION_TAKEN', 'MANUAL_RECON');

    FOR r IN c_stmt LOOP

        BEGIN
            l_candidates_found := FALSE;  -- reset for each statement line
            xxemr_suggest_line_matches(
                p_statement_line_id    => r.statement_line_id,
                p_top_n                => p_top_n,
                p_date_window_days     => p_date_window_days,
                p_amount_tolerance     => p_amount_tolerance,
                p_max_combo_size       => p_max_combo_size,
                p_max_receipt_pool     => p_max_receipt_pool,
                p_include_external_txn => 'Y',
                p_allow_one_to_many    =>
                    CASE WHEN r.external_flag = 'Y' AND r.dashboard_flag = 'Y'
                         THEN 'N' ELSE 'Y' END,
                p_result               => l_rc
            );

            LOOP
                FETCH l_rc INTO l_rank, l_source, l_key, l_amt, l_amt_diff,
                                l_date_diff, l_ref_score, l_final_score, l_expl, l_combo_size;
                EXIT WHEN l_rc%NOTFOUND;
                l_candidates_found := TRUE;

                l_match_type :=
                    CASE
                        WHEN l_source = 'EXTERNAL_TXN'                      THEN 'ONE_TO_ONE'
                        WHEN l_source = 'RECEIPT_GROUP'                      THEN 'ONE_TO_MANY'
                        WHEN l_source = 'RECEIPT_COMBO' AND l_combo_size > 1 THEN 'ONE_TO_MANY'
                        ELSE 'ONE_TO_ONE'
                    END;

                INSERT INTO xxemr_match_groups (
                    match_group_id, statement_line_id, total_match_amount,
                    difference_amount, match_score, match_type, ranking,
                    candidate_source, created_date
                ) VALUES (
                    xxemr_match_groups_seq.nextval, r.statement_line_id,
                    l_amt, l_amt_diff, l_final_score, l_match_type, l_rank,
                    CASE WHEN l_source = 'EXTERNAL_TXN' THEN 'EXT' ELSE 'AR' END,
                    SYSDATE
                )
                RETURNING match_group_id INTO l_match_group_id;

                BEGIN
                    SELECT NVL(amount, 0) INTO l_stmt_amount
                      FROM xxemr_bank_statement_lines
                     WHERE statement_line_id = r.statement_line_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN l_stmt_amount := l_amt;
                END;

                l_remaining := l_key || ',';
                LOOP
                    l_pos       := INSTR(l_remaining, ',');
                    EXIT WHEN l_pos = 0;
                    l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
                    l_remaining := SUBSTR(l_remaining, l_pos + 1);
                    EXIT WHEN l_token IS NULL;

                    IF l_token IS NOT NULL AND REGEXP_LIKE(l_token, '^\d+$') THEN

                        l_indiv_source := CASE WHEN l_source = 'EXTERNAL_TXN' THEN 'EX' ELSE 'AR' END;
                        l_indiv_id     := TO_NUMBER(l_token);

                        BEGIN
                            IF l_indiv_source = 'EX' THEN
                                SELECT NVL(amount, 0), TRUNC(transaction_date)
                                  INTO l_indiv_amount, l_indiv_date
                                  FROM xxemr_external_transactions
                                 WHERE ext_txn_id = l_indiv_id;
                            ELSE
                                SELECT NVL(amount, 0), TRUNC(gl_date)
                                  INTO l_indiv_amount, l_indiv_date
                                  FROM xxemr_ar_cash_receipts
                                 WHERE cash_receipt_id = l_indiv_id;
                            END IF;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                l_indiv_amount := l_amt;
                                l_indiv_date   := SYSDATE;
                        END;

                        INSERT INTO xxemr_match_group_details (
                            match_group_detail_id, match_group_id,
                            candidate_source,
                            candidate_id, amount, individual_score,
                            amount_diff, max_date_diff_days, reference_score
                        ) VALUES (
                            xxemr_match_grp_dtl_seq.nextval, l_match_group_id,
                            l_indiv_source,   -- 'AR' or 'EX' — drives FBDI source_code
                            l_indiv_id, l_indiv_amount, l_final_score,
                            ABS(l_stmt_amount - l_indiv_amount), l_date_diff, l_ref_score
                        );

                    END IF;
                END LOOP;
            END LOOP;

            CLOSE l_rc;
            -- No candidates found → insert NEVER_PROCESSED sentinel so the
            -- line is visible in the UI and won't be re-processed endlessly.
            IF NOT l_candidates_found THEN
                BEGIN
                    SELECT NVL(amount, 0) INTO l_stmt_amount
                      FROM xxemr_bank_statement_lines
                     WHERE statement_line_id = r.statement_line_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN l_stmt_amount := 0;
                END;

                INSERT INTO xxemr_match_groups (
                    match_group_id,      statement_line_id,   total_match_amount,
                    difference_amount,   match_score,         match_type,
                    ranking,             user_action,         candidate_source,
                    created_date
                ) VALUES (
                    xxemr_match_groups_seq.nextval, r.statement_line_id, 0,
                    l_stmt_amount, 0, 'ONE_TO_MANY', 0,
                    'NEVER_PROCESSED', NULL, SYSDATE
                );
            END IF;


        EXCEPTION
            WHEN OTHERS THEN
                IF l_rc%ISOPEN THEN CLOSE l_rc; END IF;
                l_err_msg := SUBSTR('xxemr_run_batch: ' || SQLERRM, 1, 1000);
                INSERT INTO xxemr_ai_ranking_stage (
                    statement_date, bank_account_id, statement_line_id,
                    match_group_id, validation_status, error_message,
                    creation_date, created_by
                ) VALUES (
                    TRUNC(SYSDATE), p_bank_account_id, r.statement_line_id,
                    -1, 'ERROR', l_err_msg, SYSDATE, 'XXEMR_BANK_RECONCILIATION_PKG'
                );
                COMMIT;
        END;

        l_count := l_count + 1;
        IF MOD(l_count, p_commit_interval) = 0 THEN COMMIT; END IF;

    END LOOP;

    COMMIT;

END xxemr_run_batch;


PROCEDURE xxemr_run_auto (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_run_auto
-- Purpose   : Incremental daily one-to-many AI matching.
--             Processes only lines with NO existing match group
--             (NOT EXISTS guard). Rolling 3-month window prevents
--             full table scan. No DELETE performed — incremental.
-- Source    : Ported from standalone xxemr_bank_recon_pkg (Doc 4).
-- ----------------------------------------------------------------
    p_run_id               OUT NUMBER,
    p_top_n                IN  NUMBER   DEFAULT 3,
    p_date_window_days     IN  NUMBER   DEFAULT 15,
    p_amount_tolerance     IN  NUMBER   DEFAULT 0,
    p_max_combo_size       IN  NUMBER   DEFAULT 5,
    p_max_receipt_pool     IN  NUMBER   DEFAULT 15,
    p_commit_interval      IN  NUMBER   DEFAULT 200
) IS
    -- Rolling 3-month window; NOT EXISTS ensures incremental only
    CURSOR c_stmt IS
        SELECT s.statement_line_id,
               NVL(s.external_flag,  'N') external_flag,
               NVL(s.dashboard_flag, 'N') dashboard_flag
          FROM xxemr_bank_statement_lines s
         WHERE NVL(s.match_flag,   'N') <> 'Y'
           AND NVL(s.recon_status, 'UNR') NOT IN ('REC')
           AND s.bank_account_id IS NOT NULL
           AND s.statement_date >= ADD_MONTHS(TRUNC(SYSDATE), -3)
           AND NOT EXISTS (
                   SELECT 1 FROM xxemr_match_groups mg
                    WHERE mg.statement_line_id = s.statement_line_id
               );

    l_rc             SYS_REFCURSOR;
    l_rank           NUMBER;
    l_source         VARCHAR2(30);
    l_key            VARCHAR2(4000);
    l_amt            NUMBER;
    l_amt_diff       NUMBER;
    l_date_diff      NUMBER;
    l_ref_score      NUMBER;
    l_final_score    NUMBER;
    l_expl           VARCHAR2(4000);
    l_combo_size     NUMBER;
    l_count          NUMBER := 0;
    l_match_group_id NUMBER;
    l_match_type     VARCHAR2(30);
    l_token          VARCHAR2(200);
    l_pos            NUMBER;
    l_remaining      VARCHAR2(4000);
    l_err_msg        VARCHAR2(1000);
    l_indiv_amount   NUMBER;
    l_indiv_date     DATE;
    l_indiv_source   VARCHAR2(10);
    l_indiv_id       NUMBER;
    l_stmt_amount    NUMBER;
    l_candidates_found BOOLEAN;        -- TRUE when >=1 candidate fetched
BEGIN
    p_run_id := TO_NUMBER(TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF3'));

    FOR r IN c_stmt LOOP

        BEGIN
            l_candidates_found := FALSE;  -- reset for each statement line
            xxemr_suggest_line_matches(
                p_statement_line_id    => r.statement_line_id,
                p_top_n                => p_top_n,
                p_date_window_days     => p_date_window_days,
                p_amount_tolerance     => p_amount_tolerance,
                p_max_combo_size       => p_max_combo_size,
                p_max_receipt_pool     => p_max_receipt_pool,
                p_include_external_txn => 'Y',
                p_allow_one_to_many    =>
                    CASE WHEN r.external_flag = 'Y' AND r.dashboard_flag = 'Y'
                         THEN 'N' ELSE 'Y' END,
                p_result               => l_rc
            );

            LOOP
                FETCH l_rc INTO l_rank, l_source, l_key, l_amt, l_amt_diff,
                                l_date_diff, l_ref_score, l_final_score, l_expl, l_combo_size;
                EXIT WHEN l_rc%NOTFOUND;
                l_candidates_found := TRUE;

                l_match_type :=
                    CASE
                        WHEN l_source = 'EXTERNAL_TXN'                      THEN 'ONE_TO_ONE'
                        WHEN l_source = 'RECEIPT_GROUP'                      THEN 'ONE_TO_MANY'
                        WHEN l_source = 'RECEIPT_COMBO' AND l_combo_size > 1 THEN 'ONE_TO_MANY'
                        ELSE 'ONE_TO_ONE'
                    END;

                INSERT INTO xxemr_match_groups (
                    match_group_id, statement_line_id, total_match_amount,
                    difference_amount, match_score, match_type, ranking,
                    candidate_source, created_date
                ) VALUES (
                    xxemr_match_groups_seq.nextval, r.statement_line_id,
                    l_amt, l_amt_diff, l_final_score, l_match_type, l_rank,
                    CASE WHEN l_source = 'EXTERNAL_TXN' THEN 'EXT' ELSE 'AR' END,
                    SYSDATE
                )
                RETURNING match_group_id INTO l_match_group_id;

                BEGIN
                    SELECT NVL(amount, 0) INTO l_stmt_amount
                      FROM xxemr_bank_statement_lines
                     WHERE statement_line_id = r.statement_line_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN l_stmt_amount := l_amt;
                END;

                l_remaining := l_key || ',';
                LOOP
                    l_pos       := INSTR(l_remaining, ',');
                    EXIT WHEN l_pos = 0;
                    l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
                    l_remaining := SUBSTR(l_remaining, l_pos + 1);
                    EXIT WHEN l_token IS NULL;

                    IF l_token IS NOT NULL AND REGEXP_LIKE(l_token, '^\d+$') THEN

                        l_indiv_source := CASE WHEN l_source = 'EXTERNAL_TXN' THEN 'EX' ELSE 'AR' END;
                        l_indiv_id     := TO_NUMBER(l_token);

                        BEGIN
                            IF l_indiv_source = 'EX' THEN
                                SELECT NVL(amount, 0), TRUNC(transaction_date)
                                  INTO l_indiv_amount, l_indiv_date
                                  FROM xxemr_external_transactions
                                 WHERE ext_txn_id = l_indiv_id;
                            ELSE
                                SELECT NVL(amount, 0), TRUNC(gl_date)
                                  INTO l_indiv_amount, l_indiv_date
                                  FROM xxemr_ar_cash_receipts
                                 WHERE cash_receipt_id = l_indiv_id;
                            END IF;
                        EXCEPTION
                            WHEN NO_DATA_FOUND THEN
                                l_indiv_amount := l_amt;
                                l_indiv_date   := SYSDATE;
                        END;

                        INSERT INTO xxemr_match_group_details (
                            match_group_detail_id, match_group_id,
                            candidate_source,
                            candidate_id, amount, individual_score,
                            amount_diff, max_date_diff_days, reference_score
                        ) VALUES (
                            xxemr_match_grp_dtl_seq.nextval, l_match_group_id,
                            l_indiv_source,   -- 'AR' or 'EX' — drives FBDI source_code
                            l_indiv_id, l_indiv_amount, l_final_score,
                            ABS(l_stmt_amount - l_indiv_amount), l_date_diff, l_ref_score
                        );

                    END IF;
                END LOOP;
            END LOOP;

            CLOSE l_rc;
            -- No candidates found → insert NEVER_PROCESSED sentinel.
            IF NOT l_candidates_found THEN
                BEGIN
                    SELECT NVL(amount, 0) INTO l_stmt_amount
                      FROM xxemr_bank_statement_lines
                     WHERE statement_line_id = r.statement_line_id;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN l_stmt_amount := 0;
                END;

                INSERT INTO xxemr_match_groups (
                    match_group_id,      statement_line_id,   total_match_amount,
                    difference_amount,   match_score,         match_type,
                    ranking,             user_action,         candidate_source,
                    created_date
                ) VALUES (
                    xxemr_match_groups_seq.nextval, r.statement_line_id, 0,
                    l_stmt_amount, 0, 'ONE_TO_MANY', 0,
                    'NEVER_PROCESSED', NULL, SYSDATE
                );
            END IF;


        EXCEPTION
            WHEN OTHERS THEN
                IF l_rc%ISOPEN THEN CLOSE l_rc; END IF;
                l_err_msg := SUBSTR('xxemr_run_auto: ' || SQLERRM, 1, 1000);
                INSERT INTO xxemr_ai_ranking_stage (
                    statement_date, bank_account_id, statement_line_id,
                    match_group_id, validation_status, error_message,
                    creation_date, created_by
                ) VALUES (
                    TRUNC(SYSDATE), NULL, r.statement_line_id,
                    -1, 'ERROR', l_err_msg, SYSDATE, 'XXEMR_BANK_RECONCILIATION_PKG'
                );
                COMMIT;
        END;

        l_count := l_count + 1;
        IF MOD(l_count, p_commit_interval) = 0 THEN COMMIT; END IF;

    END LOOP;

    COMMIT;

END xxemr_run_auto;


-- ================================================================
-- PROCEDURE : XXEMR_PROCESS_STATEMENT_ACTION
-- Purpose   : Central dispatcher procedure triggered from APEX
--             button. Identifies the user-selected action and
--             routes to the correct processing procedure.
-- Actions   :
--   BEST POSSIBLE MATCH    — highest scoring match group →
--                            xxemr_apply_ai_match
--   CREATE EXT. TRANSACTION — xxemr_create_me_external_transaction
--   MANUAL RECONCILIATION  — xxemr_process_manual_match
-- Errors    : -20010  No match group found for BEST POSSIBLE MATCH
--             -20011  Unknown action
--             -20012  Statement line not found
--             -20013  Candidates required for MANUAL RECONCILIATION
-- ================================================================
PROCEDURE xxemr_process_statement_action (
    p_statement_line_id IN NUMBER,
    p_action            IN VARCHAR2,
    p_candidates        IN VARCHAR2 DEFAULT NULL,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
) IS
    l_match_group_id   NUMBER;
    l_action           VARCHAR2(100) := UPPER(TRIM(p_action));
    l_line_exists      NUMBER        := 0;
BEGIN
    -- Step 1: Validate statement line exists
    SELECT COUNT(*)
      INTO l_line_exists
      FROM xxemr_bank_statement_lines
     WHERE statement_line_id = p_statement_line_id;

    IF l_line_exists = 0 THEN
        RAISE_APPLICATION_ERROR(-20012,
            'XXEMR_PROCESS_STATEMENT_ACTION: Statement line not found. '
            || 'LINE_ID=' || p_statement_line_id);
    END IF;

    -- Step 2: Log the incoming action request
    xxemr_log(
        p_source      => 'STATEMENT_ACTION',
        p_log_message => 'Action received: ' || l_action
                      || ' | LINE_ID='        || p_statement_line_id
                      || ' | USER='           || p_user
    );

    -- Step 3: Route based on action
    IF l_action = 'BEST POSSIBLE MATCH' THEN

        -- Find the ACCEPTED group (set by Apply Selection in the popup).
        -- If the user skipped Apply Selection and clicked BPM directly,
        -- auto-accept the highest scoring group first.
        BEGIN
            SELECT match_group_id
              INTO l_match_group_id
              FROM xxemr_match_groups
             WHERE statement_line_id = p_statement_line_id
               AND user_action        = 'ACCEPTED'
               AND ROWNUM             = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                -- No explicit selection — auto-pick highest score
                BEGIN
                    SELECT match_group_id
                      INTO l_match_group_id
                      FROM (
                            SELECT match_group_id,
                                   RANK() OVER (
                                       ORDER BY match_score DESC,
                                                created_date DESC
                                   ) AS rnk
                              FROM xxemr_match_groups
                             WHERE statement_line_id = p_statement_line_id
                               AND NVL(user_action,'PENDING') NOT IN ('REJECTED')
                           )
                     WHERE rnk = 1;

                    -- Stamp REJECTED/ACCEPTED so confirm proc can find it
                    UPDATE xxemr_match_groups
                       SET user_action = 'REJECTED'
                     WHERE statement_line_id = p_statement_line_id;

                    UPDATE xxemr_match_groups
                       SET user_action = 'ACCEPTED'
                     WHERE match_group_id = l_match_group_id;

                    COMMIT;
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        RAISE_APPLICATION_ERROR(-20010,
                            'XXEMR_PROCESS_STATEMENT_ACTION: No match group found '
                            || 'for BEST POSSIBLE MATCH. '
                            || 'LINE_ID=' || p_statement_line_id);
                END;
        END;

        xxemr_log(
            p_source      => 'STATEMENT_ACTION',
            p_log_message => 'Routing to xxemr_confirm_ai_match. '
                          || 'LINE_ID='        || p_statement_line_id
                          || ' | MATCH_GROUP=' || l_match_group_id
        );

        xxemr_confirm_ai_match(
            p_statement_line_id => p_statement_line_id,
            p_user              => p_user
        );

        xxemr_log(
            p_source      => 'STATEMENT_ACTION',
            p_log_message => 'BEST POSSIBLE MATCH confirmed and FBDI staged. '
                          || 'LINE_ID='        || p_statement_line_id
                          || ' | MATCH_GROUP=' || l_match_group_id
        );

    ELSIF l_action = 'CREATE EXT. TRANSACTION' THEN

        xxemr_log(
            p_source      => 'STATEMENT_ACTION',
            p_log_message => 'Routing to xxemr_create_me_external_transaction. '
                          || 'LINE_ID=' || p_statement_line_id
        );

        xxemr_create_me_external_transaction(
            p_statement_line_id => p_statement_line_id
        );

        xxemr_log(
            p_source      => 'STATEMENT_ACTION',
            p_log_message => 'CREATE EXT. TRANSACTION completed successfully. '
                          || 'LINE_ID=' || p_statement_line_id
        );

    ELSIF l_action = 'MANUAL RECONCILIATION' THEN

        IF p_candidates IS NULL THEN
            RAISE_APPLICATION_ERROR(-20013,
                'XXEMR_PROCESS_STATEMENT_ACTION: p_candidates is required '
                || 'for MANUAL RECONCILIATION. '
                || 'LINE_ID=' || p_statement_line_id);
        END IF;

        xxemr_log(
            p_source      => 'STATEMENT_ACTION',
            p_log_message => 'Routing to xxemr_process_manual_match. '
                          || 'LINE_ID='     || p_statement_line_id
                          || ' | CANDS='    || p_candidates
        );

        xxemr_process_manual_match(
            p_statement_line_id => p_statement_line_id,
            p_candidates        => p_candidates,
            p_user              => p_user
        );

        xxemr_log(
            p_source      => 'STATEMENT_ACTION',
            p_log_message => 'MANUAL RECONCILIATION completed successfully. '
                          || 'LINE_ID='  || p_statement_line_id
                          || ' | CANDS=' || p_candidates
        );

    ELSE
        RAISE_APPLICATION_ERROR(-20011,
            'XXEMR_PROCESS_STATEMENT_ACTION: Unknown action "'
            || p_action || '". Valid values: '
            || 'BEST POSSIBLE MATCH, '
            || 'CREATE EXT. TRANSACTION, '
            || 'MANUAL RECONCILIATION');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        xxemr_log(
            p_source      => 'STATEMENT_ACTION',
            p_log_message => 'ERROR during action '  || l_action
                          || ' | LINE_ID=' || p_statement_line_id
                          || ' | ERROR='   || SQLERRM
        );
        ROLLBACK;
        RAISE;

END xxemr_process_statement_action;


-- ================================================================
-- PROCEDURE : XXEMR_PROCESS_STATEMENT_ACTION_BULK
-- Purpose   : Bulk dispatcher triggered from APEX. Accepts a
--             colon-separated token list where each token contains
--             an action prefix + statement_line_id.
-- Token prefixes:
--   NA   — No Action
--   BPM  — Best Possible Match
--   CET  — Create Ext. Transaction
--   MR   — Manual Reconciliation
-- ================================================================
PROCEDURE xxemr_process_statement_action_bulk (
    p_line_action_tokens IN  VARCHAR2,
    p_candidates         IN  VARCHAR2 DEFAULT NULL,
    p_user               IN  VARCHAR2 DEFAULT 'SYSTEM',
    p_success_count      OUT NUMBER,
    p_error_count        OUT NUMBER,
    p_error_summary      OUT VARCHAR2
) IS
    l_remaining    VARCHAR2(4000);
    l_pos          NUMBER;
    l_token        VARCHAR2(200);
    l_prefix       VARCHAR2(10);
    l_line_id      NUMBER;
    l_action       VARCHAR2(100);
BEGIN
    p_success_count := 0;
    p_error_count   := 0;
    p_error_summary := NULL;

    l_remaining := p_line_action_tokens || ':';
    LOOP
        l_pos := INSTR(l_remaining, ':');
        EXIT WHEN l_pos = 0;

        l_token     := TRIM(SUBSTR(l_remaining, 1, l_pos - 1));
        l_remaining := SUBSTR(l_remaining, l_pos + 1);
        EXIT WHEN l_token IS NULL;

        IF SUBSTR(l_token, 1, 3) = 'BPM' THEN
            l_prefix  := 'BPM';
            l_line_id := TO_NUMBER(SUBSTR(l_token, 4));
            l_action  := 'BEST POSSIBLE MATCH';

        ELSIF SUBSTR(l_token, 1, 3) = 'CET' THEN
            l_prefix  := 'CET';
            l_line_id := TO_NUMBER(SUBSTR(l_token, 4));
            l_action  := 'CREATE EXT. TRANSACTION';

        ELSIF SUBSTR(l_token, 1, 2) = 'MR' THEN
            l_prefix  := 'MR';
            l_line_id := TO_NUMBER(SUBSTR(l_token, 3));
            l_action  := 'MANUAL RECONCILIATION';

        ELSIF SUBSTR(l_token, 1, 2) = 'NA' THEN
            l_prefix  := 'NA';
            l_line_id := TO_NUMBER(SUBSTR(l_token, 3));
            l_action  := 'NO ACTION';

        ELSE
            p_error_count   := p_error_count + 1;
            p_error_summary := p_error_summary
                            || 'TOKEN=' || l_token
                            || ' | ERROR=Unknown prefix. '
                            || 'Valid prefixes: NA, BPM, CET, MR'
                            || ' || ';
            xxemr_log(
                p_source      => 'BULK_ACTION',
                p_log_message => 'Unknown prefix in token: ' || l_token
            );
            CONTINUE;
        END IF;

        IF l_prefix = 'NA' THEN
            xxemr_log(
                p_source      => 'BULK_ACTION',
                p_log_message => 'Skipped — No Action. LINE_ID=' || l_line_id
            );
            CONTINUE;
        END IF;

        IF l_prefix = 'MR' AND p_candidates IS NULL THEN
            p_error_count   := p_error_count + 1;
            p_error_summary := p_error_summary
                            || 'LINE_ID=' || l_line_id
                            || ' | ACTION=' || l_action
                            || ' | ERROR=Candidates required for MANUAL RECONCILIATION'
                            || ' || ';
            xxemr_log(
                p_source      => 'BULK_ACTION',
                p_log_message => 'Skipped — candidates required. LINE_ID=' || l_line_id
            );
            CONTINUE;
        END IF;

        BEGIN
            xxemr_process_statement_action(
                p_statement_line_id => l_line_id,
                p_action            => l_action,
                p_candidates        => p_candidates,
                p_user              => p_user
            );

            p_success_count := p_success_count + 1;

            xxemr_log(
                p_source      => 'BULK_ACTION',
                p_log_message => 'Processed successfully. '
                              || 'LINE_ID=' || l_line_id
                              || ' | ACTION=' || l_action
            );

        EXCEPTION
            WHEN OTHERS THEN
                p_error_count   := p_error_count + 1;
                p_error_summary := p_error_summary
                                || 'LINE_ID=' || l_line_id
                                || ' | ACTION=' || l_action
                                || ' | ERROR='  || SQLERRM
                                || ' || ';
                xxemr_log(
                    p_source      => 'BULK_ACTION',
                    p_log_message => 'Failed. LINE_ID=' || l_line_id
                                  || ' | ACTION=' || l_action
                                  || ' | ERROR=' || SQLERRM
                );
        END;

    END LOOP;

    xxemr_log(
        p_source      => 'BULK_ACTION',
        p_log_message => 'Bulk completed. '
                      || 'SUCCESS=' || p_success_count
                      || ' | ERRORS=' || p_error_count
    );

EXCEPTION
    WHEN OTHERS THEN
        xxemr_log(
            p_source      => 'BULK_ACTION',
            p_log_message => 'Bulk failed unexpectedly. ERROR=' || SQLERRM
        );
        RAISE;

END xxemr_process_statement_action_bulk;


END XXEMR_BANK_RECONCILIATION_PKG;
/