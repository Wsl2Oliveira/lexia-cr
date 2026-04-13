-- =============================================================================
-- LexIA CR — Casos Judiciais Pendentes (últimos 12 dias)
-- Notebook: https://nubank-e2-general.cloud.databricks.com/editor/notebooks/3144410994819490
-- =============================================================================

-- CMD 1: Visão geral dos casos pendentes
-- Cole este bloco no primeiro command do notebook

SELECT
    -- Data de recebimento (coluna-filtro dos 12 dias)
    ext.official_letter_extraction__created_at                     AS data_recebimento,
    DATEDIFF(day, ext.official_letter_extraction__created_at, current_date()) AS dias_desde_recebimento,

    -- Identificação do ofício
    ol.official_letter__id                                        AS id_oficio,
    CASE ol.official_letter__type
        WHEN 'official_letter_type__block'    THEN 'BLOQUEIO'
        WHEN 'official_letter_type__dismiss'  THEN 'DESBLOQUEIO'
        WHEN 'official_letter_type__transfer' THEN 'TRANSFERÊNCIA'
        ELSE ol.official_letter__type
    END                                                           AS tipo_legivel,
    ol.official_letter__type                                      AS tipo_oficio,
    ol.official_letter__status                                    AS status_oficio,
    ol.official_letter__origin                                    AS origem,

    -- Números de referência
    ext.official_letter_extraction__craft_document_number          AS numero_oficio,
    ext.official_letter_extraction__process_document_number        AS numero_processo,

    -- Órgão / Vara
    ext.official_letter_extraction__court_tribunal_name            AS vara_tribunal,
    ext.official_letter_extraction__organ_name                     AS orgao_nome,
    ext.official_letter_extraction__organ_address                  AS orgao_endereco,

    -- Investigado (PII resolvido)
    name_pii.investigated_information__name                        AS nome_investigado,
    cpf_pii.investigated_information__cpf_cnpj                     AS cpf_cnpj,
    inv.investigated_information__requested_value                  AS valor_solicitado,
    inv.investigated_information__customer_id                      AS customer_id,
    inv.investigated_information__is_customer                      AS is_cliente_nu,

    -- Demais datas e prazos
    ol.official_letter__deadline_date                              AS prazo_oficio,
    ext.official_letter_extraction__final_deadline                 AS prazo_final,
    ext.official_letter_extraction__start_date                     AS data_inicio,
    ext.official_letter_extraction__end_date                       AS data_fim,

    -- Metadados adicionais
    ext.official_letter_extraction__requested_item                 AS item_solicitado,
    ext.official_letter_extraction__is_reiteration                 AS is_reiteracao,
    ext.official_letter_extraction__office_text_observations        AS observacoes,
    ext.official_letter_extraction__zendesk_ticket_number           AS ticket_zendesk,
    ext.official_letter_extraction__response_email                  AS email_resposta,
    ext.official_letter_extraction__status                          AS status_extracao,
    ol.official_letter__submission_id                               AS submission_id

FROM etl.br__dataset.jud_athena_official_letter_extractions ext

INNER JOIN etl.br__dataset.jud_athena_submissions sub
    ON ext.official_letter_extraction__submission_id = sub.submission__id

INNER JOIN etl.br__dataset.jud_athena_official_letters ol
    ON ol.official_letter__submission_id = sub.submission__id

LEFT JOIN etl.br__contract.jud_athena__investigated_information inv
    ON inv.investigated_information__id = ol.investigated_information__id[0]

LEFT JOIN etl.br__contract.jud_athena__investigated_information_name_pii name_pii
    ON name_pii.hash = inv.investigated_information__name

LEFT JOIN etl.br__contract.jud_athena__investigated_information_cpf_cnpj_pii cpf_pii
    ON cpf_pii.hash = inv.investigated_information__cpf_cnpj

WHERE ol.official_letter__type IN (
        'official_letter_type__block',
        'official_letter_type__dismiss',
        'official_letter_type__transfer'
    )
  AND ext.official_letter_extraction__status = 'official_letter_extraction_status__confirmed'
  AND ext.official_letter_extraction__created_at >= current_date() - INTERVAL 12 DAYS

ORDER BY
    ext.official_letter_extraction__created_at DESC,
    ol.official_letter__type

-- =============================================================================
-- CMD 2: Resumo por tipo de ofício (cole em um novo command)
-- =============================================================================

-- SELECT
--     CASE ol.official_letter__type
--         WHEN 'official_letter_type__block'    THEN 'BLOQUEIO'
--         WHEN 'official_letter_type__dismiss'  THEN 'DESBLOQUEIO'
--         WHEN 'official_letter_type__transfer' THEN 'TRANSFERÊNCIA'
--         ELSE ol.official_letter__type
--     END                                             AS tipo,
--     COUNT(*)                                        AS total_casos,
--     SUM(CASE WHEN inv.investigated_information__is_customer THEN 1 ELSE 0 END) AS clientes_nu,
--     SUM(CASE WHEN NOT inv.investigated_information__is_customer OR inv.investigated_information__is_customer IS NULL THEN 1 ELSE 0 END) AS nao_clientes,
--     SUM(CASE WHEN ext.official_letter_extraction__is_reiteration THEN 1 ELSE 0 END) AS reiteracoes,
--     ROUND(AVG(DATEDIFF(day, ext.official_letter_extraction__created_at, current_date())), 1) AS media_dias_pendente
--
-- FROM etl.br__dataset.jud_athena_official_letter_extractions ext
-- INNER JOIN etl.br__dataset.jud_athena_submissions sub
--     ON ext.official_letter_extraction__submission_id = sub.submission__id
-- INNER JOIN etl.br__dataset.jud_athena_official_letters ol
--     ON ol.official_letter__submission_id = sub.submission__id
-- LEFT JOIN etl.br__contract.jud_athena__investigated_information inv
--     ON inv.investigated_information__id = ol.investigated_information__id[0]
--
-- WHERE ol.official_letter__type IN (
--         'official_letter_type__block',
--         'official_letter_type__dismiss',
--         'official_letter_type__transfer'
--     )
--   AND ext.official_letter_extraction__status = 'official_letter_extraction_status__confirmed'
--   AND ext.official_letter_extraction__created_at >= current_date() - INTERVAL 12 DAYS
--
-- GROUP BY 1
-- ORDER BY total_casos DESC
