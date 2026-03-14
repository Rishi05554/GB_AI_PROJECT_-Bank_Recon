create or replace PROCEDURE xxemr_dispatch_fbdi_to_oic (
    p_batch_size   IN NUMBER DEFAULT 100,
    p_max_attempts IN NUMBER DEFAULT 2
) AS

    TYPE t_num_list IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

    CURSOR c_refs IS
        SELECT   recon_reference,
                 MIN(creation_date) AS first_created
          FROM   xxemr_recon_fbdi_lines
         WHERE   send_status   IN ('PENDING', 'FAILED')
           AND   send_attempts  < p_max_attempts
         GROUP BY recon_reference
         ORDER BY MIN(creation_date);

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

    v_endpoint    VARCHAR2(500);
    v_username    VARCHAR2(200);
    v_password    VARCHAR2(200);

    l_payload      CLOB;
    l_batch_count  PLS_INTEGER := 0;
    l_id_count     PLS_INTEGER := 0;
    l_id_list      t_num_list;
    l_first_group  BOOLEAN     := TRUE;
    l_first_line   BOOLEAN;

    l_pending_count  NUMBER := 0;
    l_total_groups   NUMBER := 0;
    l_total_sent     NUMBER := 0;
    l_total_failed   NUMBER := 0;
    l_total_errored  NUMBER := 0;

    l_api_response  VARCHAR2(4000);
    l_api_status    NUMBER;

    FUNCTION json_str (p_val IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN REPLACE(
                   REPLACE(NVL(p_val, ''), '\', '\\'),
               '"', '\"');
    END json_str;

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

    PROCEDURE flush_batch IS
        l_envelope         CLOB;
        l_errored_in_batch NUMBER := 0;
    BEGIN
        IF l_batch_count = 0 THEN
            RETURN;
        END IF;

        DBMS_LOB.CREATETEMPORARY(l_envelope, TRUE);
        DBMS_LOB.APPEND(l_envelope, TO_CLOB('{"reconciliationLines":['));
        DBMS_LOB.APPEND(l_envelope, l_payload);
        DBMS_LOB.APPEND(l_envelope, TO_CLOB(']}'));

        DBMS_OUTPUT.PUT_LINE('  Flushing: '
            || l_batch_count || ' groups | '
            || l_id_count    || ' lines');

        BEGIN
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

        DBMS_OUTPUT.PUT_LINE('  HTTP ' || NVL(TO_CHAR(l_api_status), 'NULL'));

        IF l_api_status IN (200, 201) THEN

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
            DBMS_OUTPUT.PUT_LINE('  Accepted by OIC.');

        ELSE

            FORALL i IN 1 .. l_id_count
                UPDATE xxemr_recon_fbdi_lines
                   SET send_attempts    = send_attempts + 1,
                       last_error       = l_api_response,
                       last_updated_by  = 'OIC_DISPATCHER',
                       last_update_date = SYSTIMESTAMP
                 WHERE fbdi_line_id     = l_id_list(i);

            FORALL i IN 1 .. l_id_count
                UPDATE xxemr_recon_fbdi_lines
                   SET send_status = CASE
                                         WHEN send_attempts >= p_max_attempts
                                         THEN 'ERROR'
                                         ELSE 'FAILED'
                                     END
                 WHERE fbdi_line_id = l_id_list(i);

            l_errored_in_batch := 0;
            FOR k IN 1 .. l_id_count LOOP
                DECLARE
                    l_chk NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO l_chk
                      FROM xxemr_recon_fbdi_lines
                     WHERE fbdi_line_id = l_id_list(k)
                       AND source_code  = 'BS'
                       AND send_status  = 'ERROR';
                    l_errored_in_batch := l_errored_in_batch + l_chk;
                END;
            END LOOP;

            COMMIT;
            l_total_failed  := l_total_failed  + l_batch_count;
            l_total_errored := l_total_errored + l_errored_in_batch;
            DBMS_OUTPUT.PUT_LINE('  Failed. HTTP='
                || NVL(TO_CHAR(l_api_status), 'NULL')
                || ' | Permanently errored: ' || l_errored_in_batch);

        END IF;

        DBMS_LOB.FREETEMPORARY(l_envelope);

    END flush_batch;

BEGIN
    DBMS_OUTPUT.PUT_LINE('================================================');
    DBMS_OUTPUT.PUT_LINE('xxemr_dispatch_fbdi_to_oic  START : '
        || TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Batch size: ' || p_batch_size
        || ' groups/call | Max attempts: ' || p_max_attempts);
    DBMS_OUTPUT.PUT_LINE('================================================');

    SELECT COUNT(DISTINCT recon_reference)
      INTO l_pending_count
      FROM xxemr_recon_fbdi_lines
     WHERE send_status   IN ('PENDING', 'FAILED')
       AND send_attempts  < p_max_attempts;

    IF l_pending_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('No PENDING/FAILED rows. Exiting - no OIC calls made.');
        DBMS_OUTPUT.PUT_LINE('================================================');
        RETURN;
    END IF;

    DBMS_OUTPUT.PUT_LINE('Groups to dispatch : ' || l_pending_count);

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

    DBMS_LOB.CREATETEMPORARY(l_payload, TRUE);

    FOR r_ref IN c_refs LOOP

        l_total_groups := l_total_groups + 1;
        l_batch_count  := l_batch_count  + 1;

        DBMS_OUTPUT.PUT_LINE('  [' || l_total_groups || '/'
            || l_pending_count || '] ' || r_ref.recon_reference);

        IF NOT l_first_group THEN
            DBMS_LOB.APPEND(l_payload, TO_CLOB(','));
        END IF;
        l_first_group := FALSE;
        l_first_line  := TRUE;

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
                    || json_str(TO_CHAR(r_line.source_id))      || '",'
                || '"sourceCode":"'
                    || json_str(r_line.source_code)             || '",'
                || '"externalReconReference":"'
                    || json_str(r_ref.recon_reference)          || '",'
                || '"bankAccountId":"'
                    || json_str(r_line.bank_account_id)         || '",'
                || '"sourceLineId":""'
                || '}'
            ));

        END LOOP;

        IF l_batch_count >= p_batch_size THEN
            DBMS_OUTPUT.PUT_LINE('Batch full (' || p_batch_size || ' groups). Flushing...');
            flush_batch;
            reset_batch;
        END IF;

    END LOOP;

    IF l_batch_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('Flushing final batch (' || l_batch_count || ' groups)...');
        flush_batch;
        reset_batch;
    END IF;

    DBMS_OUTPUT.PUT_LINE('================================================');
    DBMS_OUTPUT.PUT_LINE('xxemr_dispatch_fbdi_to_oic  END : '
        || TO_CHAR(SYSTIMESTAMP, 'DD-MON-YYYY HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('Groups found       : ' || l_pending_count);
    DBMS_OUTPUT.PUT_LINE('Successfully SENT  : ' || l_total_sent);
    DBMS_OUTPUT.PUT_LINE('Failed (retry)     : '
        || GREATEST(l_total_failed - l_total_errored, 0));
    DBMS_OUTPUT.PUT_LINE('Permanently ERROR  : ' || l_total_errored);
    DBMS_OUTPUT.PUT_LINE('================================================');

EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_LOB.ISTEMPORARY(l_payload) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_payload);
        END IF;
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('FATAL: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        RAISE;

END xxemr_dispatch_fbdi_to_oic;
/