create or replace PACKAGE XXEMR_BANK_RECONCILIATION_PKG AS

-- ================================================================
-- PACKAGE SPEC : XXEMR_BANK_RECONCILIATION_PKG
-- Description  : Consolidated Bank Reconciliation package.
-- Sections:
--   1  Manual Reconciliation
--   2  AI Match Application
--   3  External Transaction Processing
--   4  One-to-One AI Matching Engine
--   5  One-to-Many AI Matching Engine
-- ================================================================


-- ================================================================
-- SECTION 1 — MANUAL RECONCILIATION
-- ================================================================

PROCEDURE xxemr_process_manual_match (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_process_manual_match
-- Purpose   : Full manual reconciliation orchestrator.
--             Validates that candidate amounts balance exactly
--             with the statement line, creates a MANUAL_RECON
--             match group, and stamps MANUAL_RECON status on all
--             associated records.
-- Parameters:
--   p_statement_line_id  — statement line being reconciled
--   p_candidates         — colon-delimited prefixed token list
--                          e.g. 'AR12345:AR67890:EX111'
--   p_user               — user performing the action
-- ----------------------------------------------------------------
    p_statement_line_id IN NUMBER,
    p_candidates        IN VARCHAR2,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
);


-- ================================================================
-- SECTION 2 — AI MATCH APPLICATION
-- ================================================================

PROCEDURE xxemr_apply_ai_match (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_apply_ai_match
-- Purpose   : Applies a pre-generated AI match group by stamping
--             AI_RECONCILED status on the statement line and all
--             candidate records linked via the match group.
-- Parameters:
--   p_match_group_id  — ID of the match group to apply
--   p_user            — user performing the action
-- ----------------------------------------------------------------
    p_match_group_id IN NUMBER,
    p_user           IN VARCHAR2 DEFAULT 'SYSTEM'
);


-- ================================================================
-- SECTION 3 — EXTERNAL TRANSACTION PROCESSING
-- ================================================================

PROCEDURE xxemr_process_external_transactions;
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_process_external_transactions
-- Purpose   : Main batch orchestrator for external transaction
--             processing. Derives run mode (DAILY / MONTH_END)
--             from the calendar and the MONTH_END_TRIGGER config
--             key, runs keyword classification against
--             XXEMR_KEYWORD_MAPPING, then executes a 4-step
--             matching cascade per flagged line:
--               CHECK 1A: AR receipt   exact  → recon_status=REC
--               CHECK 1B: AR receipt   partial → EXT_PARTIAL group
--               CHECK 2A: Ext txn      exact  → recon_status=REC
--               CHECK 2B: Ext txn      partial → EXT_PARTIAL group
--               NOTHING  → PENDING_APPROVAL   → EXT_PENDING group
--             Called by a scheduled DBMS_SCHEDULER job.
-- ----------------------------------------------------------------


PROCEDURE xxemr_create_pw_external_transaction (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_create_pw_external_transaction
-- Purpose   : Called from the APEX UI to approve and create a
--             Fusion external transaction for a PENDING Profit
--             Withdrawal statement line
--             (approval_status='PENDING', pw_action='PENDING_APPROVAL').
--             Calls the Fusion CE cashExternalTransactions REST API
--             and stamps the result on the statement line and match
--             group.
-- Parameters:
--   p_statement_line_id  — statement line to approve and submit
-- ----------------------------------------------------------------
    p_statement_line_id IN NUMBER
);


PROCEDURE xxemr_create_me_external_transaction (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_create_me_external_transaction
-- Purpose   : Called from the APEX UI to approve and create a
--             Fusion external transaction for a PENDING Month-End
--             statement line
--             (approval_status='PENDING', month_end_action='PENDING_APPROVAL').
--             Calls the Fusion CE cashExternalTransactions REST API
--             and stamps the result on the statement line and match
--             group.
-- Parameters:
--   p_statement_line_id  — statement line to approve and submit
-- ----------------------------------------------------------------
    p_statement_line_id IN NUMBER
);


-- ================================================================
-- SECTION 4 — ONE-TO-ONE AI MATCHING ENGINE
-- ================================================================

PROCEDURE xxemr_suggest_one_to_one_match (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_suggest_one_to_one_match
-- Purpose   : Scores and ranks AR receipt candidates for a single
--             statement line using a weighted scoring model:
--               Amount similarity   60%
--               Date proximity      25%
--               Reference similarity 15%
--             Returns up to p_top_n ranked candidates via a
--             ref cursor. Used by xxemr_run_one_to_one_batch and
--             callable directly for single-line evaluation.
-- Parameters:
--   p_statement_line_id  — statement line to match
--   p_top_n              — max candidates to return (default 5)
--   p_date_window_days   — date range ± days (default 7)
--   p_amount_tolerance   — % tolerance on amount (default 0.5)
--   p_result             — ref cursor returning ranked candidates
-- Cursor columns returned (in order):
--   candidate_rank       NUMBER
--   candidate_source     VARCHAR2   ('AR_RECEIPT')
--   candidate_key        VARCHAR2   (cash_receipt_id)
--   candidate_amount     NUMBER
--   amount_diff          NUMBER
--   date_diff_days       NUMBER
--   reference_score      NUMBER
--   desc_match_flag      VARCHAR2   ('Y'/'N')
--   final_score          NUMBER
--   explanation          VARCHAR2
-- ----------------------------------------------------------------
    p_statement_line_id IN  NUMBER,
    p_top_n             IN  NUMBER   DEFAULT 5,
    p_date_window_days  IN  NUMBER   DEFAULT 7,
    p_amount_tolerance  IN  NUMBER   DEFAULT 0.5,
    p_result            OUT SYS_REFCURSOR
);


PROCEDURE xxemr_run_one_to_one_batch (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_run_one_to_one_batch
-- Purpose   : Batch orchestrator for one-to-one AI matching.
--             Iterates all unreconciled statement lines for the
--             given bank account, calls
--             xxemr_suggest_one_to_one_match per line, and
--             persists match group suggestions in
--             xxemr_match_groups / xxemr_match_group_details.
--             Existing PENDING / REJECTED suggestions are purged
--             before reprocessing; ACTION_TAKEN rows are preserved.
-- Parameters:
--   p_bank_account_id    — bank account to process
--   p_date_window_days   — passed to scoring engine (default 7)
--   p_amount_tolerance   — passed to scoring engine (default 0.5)
--   p_top_n              — max suggestions per line (default 5)
--   p_created_by         — audit user (default 'SYSTEM')
-- ----------------------------------------------------------------
    p_bank_account_id  IN  NUMBER,
    p_date_window_days IN  NUMBER   DEFAULT 7,
    p_amount_tolerance IN  NUMBER   DEFAULT 0.5,
    p_top_n            IN  NUMBER   DEFAULT 5,
    p_created_by       IN  VARCHAR2 DEFAULT 'SYSTEM'
);


-- ================================================================
-- SECTION 5 — ONE-TO-MANY AI MATCHING ENGINE
-- ================================================================

PROCEDURE xxemr_suggest_line_matches (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_suggest_line_matches
-- Purpose   : Scores and ranks AR receipt candidates (including
--             multi-receipt combinations) and optionally external
--             transactions for a single statement line.
--             Supports one-to-one and one-to-many match types via
--             recursive CTE combination building, capped at
--             p_max_combo_size (hard ceiling of 5 to prevent
--             CTE explosion).
--             Returns up to p_top_n ranked candidates via a
--             ref cursor.
-- Parameters:
--   p_statement_line_id    — statement line to match
--   p_top_n                — max candidates to return (default 3)
--   p_date_window_days     — date range ± days (default 15)
--   p_amount_tolerance     — absolute amount tolerance (default 0)
--   p_max_combo_size       — max receipts per combo (default 3)
--   p_max_receipt_pool     — receipt pool size fed to CTE (default 60)
--   p_include_external_txn — include external txns 'Y'/'N' (default 'Y')
--   p_allow_one_to_many    — allow multi-receipt combos 'Y'/'N' (default 'Y')
--   p_result               — ref cursor returning ranked candidates
-- Cursor columns returned (in order):
--   candidate_rank         NUMBER
--   candidate_source       VARCHAR2   ('RECEIPT_COMBO','RECEIPT_GROUP','EXTERNAL_TXN')
--   candidate_key          VARCHAR2   (comma-delimited IDs)
--   candidate_amount       NUMBER
--   amount_diff            NUMBER
--   max_date_diff_days     NUMBER
--   reference_score        NUMBER
--   final_score            NUMBER
--   explanation            VARCHAR2
--   combo_size             NUMBER
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
);


PROCEDURE xxemr_run_batch (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_run_batch
-- Purpose   : Reprocessing batch for one-to-many AI matching over
--             a specified date range and bank account. Deletes
--             existing PENDING suggestions (preserves ACTION_TAKEN
--             and MANUAL_RECON rows), then calls
--             xxemr_suggest_line_matches per unmatched line and
--             persists results. Use for deliberate reprocessing;
--             for incremental daily processing use xxemr_run_auto.
-- Parameters:
--   p_run_id               — caller-supplied run identifier
--   p_bank_account_id      — bank account to reprocess
--   p_stmt_from_date       — statement date range start
--   p_stmt_to_date         — statement date range end
--   p_top_n                — max suggestions per line (default 3)
--   p_date_window_days     — passed to scoring engine (default 15)
--   p_amount_tolerance     — passed to scoring engine (default 0)
--   p_max_combo_size       — passed to scoring engine (default 3)
--   p_max_receipt_pool     — passed to scoring engine (default 60)
--   p_commit_interval      — commit every N lines (default 500)
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
);


PROCEDURE xxemr_run_auto (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_run_auto
-- Purpose   : Incremental daily one-to-many AI matching job.
--             Processes only lines that have NO existing match
--             group suggestions (NOT EXISTS guard) within a
--             rolling 3-month window. No DELETE is performed —
--             already-matched lines are untouched. Intended to
--             be called by a scheduled DBMS_SCHEDULER job.
-- Parameters:
--   p_run_id               — OUT: timestamp-based run identifier
--   p_top_n                — max suggestions per line (default 3)
--   p_date_window_days     — passed to scoring engine (default 15)
--   p_amount_tolerance     — passed to scoring engine (default 0)
--   p_max_combo_size       — passed to scoring engine (default 3)
--   p_max_receipt_pool     — passed to scoring engine (default 60)
--   p_commit_interval      — commit every N lines (default 500)
-- ----------------------------------------------------------------
    p_run_id               OUT NUMBER,
    p_top_n                IN  NUMBER   DEFAULT 3,
    p_date_window_days     IN  NUMBER   DEFAULT 15,
    p_amount_tolerance     IN  NUMBER   DEFAULT 0,
    p_max_combo_size       IN  NUMBER   DEFAULT 5,
    p_max_receipt_pool     IN  NUMBER   DEFAULT 15,
    p_commit_interval      IN  NUMBER   DEFAULT 200
);

-- ================================================================
-- SECTION 0 — STATEMENT ACTION ROUTER
-- ================================================================

PROCEDURE xxemr_process_statement_action (
-- ----------------------------------------------------------------
-- PROCEDURE : xxemr_process_statement_action
-- Purpose   : Routes APEX UI actions for a statement line.
--             Supports:
--               BEST POSSIBLE MATCH
--               CREATE EXT. TRANSACTION
--               MANUAL RECONCILIATION
-- ----------------------------------------------------------------
    p_statement_line_id IN NUMBER,
    p_action            IN VARCHAR2,
    p_candidates        IN VARCHAR2 DEFAULT NULL,
    p_user              IN VARCHAR2 DEFAULT 'SYSTEM'
);

PROCEDURE xxemr_process_statement_action_bulk(
    p_line_action_tokens IN  VARCHAR2,
    p_candidates         IN  VARCHAR2 DEFAULT NULL,  
    p_user               IN  VARCHAR2 DEFAULT 'SYSTEM',
    p_success_count      OUT NUMBER,
    p_error_count        OUT NUMBER,
    p_error_summary      OUT VARCHAR2
);
    
END XXEMR_BANK_RECONCILIATION_PKG;
/