create or replace PACKAGE XXEMR_AI_RECON_PKG AS

    PROCEDURE run_daily_recon(
        p_date IN DATE DEFAULT TRUNC(SYSDATE) - 1
    );

    PROCEDURE process_bank(
        p_bank_id IN NUMBER,
        p_date    IN DATE
    );

    FUNCTION call_genai(
        p_payload IN CLOB
    ) RETURN CLOB;

    PROCEDURE apply_rankings(
        p_bank_id IN NUMBER,
        p_date    IN DATE
    );

    PROCEDURE log_error(
        p_procedure IN VARCHAR2,
        p_bank_id   IN NUMBER,
        p_message   IN VARCHAR2
    );

END XXEMR_AI_RECON_PKG;
/