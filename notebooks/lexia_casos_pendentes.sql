-- =============================================================================
-- LexIA CR — Casos Judiciais Pendentes (deduplicados, últimos 12 dias)
-- Notebook: (link do Databricks)
-- =============================================================================

-- CMD 1: Casos deduplicados (1 linha por processo, triagem mais recente)
-- Cole este bloco no primeiro command do notebook

WITH exploded AS (
    SELECT
        ext.official_letter_extraction__created_at               AS data_recebimento,
        DATEDIFF(day, ext.official_letter_extraction__created_at, current_date()) AS dias_desde_recebimento,

        ol.official_letter__id                                    AS id_oficio,
        CASE ol.official_letter__type
            WHEN 'official_letter_type__block'    THEN 'BLOQUEIO'
            WHEN 'official_letter_type__dismiss'  THEN 'DESBLOQUEIO'
            WHEN 'official_letter_type__transfer' THEN 'TRANSFERÊNCIA'
            ELSE ol.official_letter__type
        END                                                       AS tipo_legivel,
        ol.official_letter__type                                  AS tipo_oficio,
        ol.official_letter__status                                AS status_oficio,

        ext.official_letter_extraction__craft_document_number      AS numero_oficio,
        ext.official_letter_extraction__process_document_number    AS numero_processo,
        ext.official_letter_extraction__court_tribunal_name        AS vara_tribunal,
        ext.official_letter_extraction__organ_name                 AS orgao_nome,

        inv_exploded.inv_id                                        AS investigated_id,
        inv_exploded.inv_pos                                       AS investigado_seq,

        name_pii.investigated_information__name                    AS nome_investigado,
        cpf_pii.investigated_information__cpf_cnpj                 AS cpf_cnpj,
        inv.investigated_information__requested_value              AS valor_solicitado,
        inv.investigated_information__customer_id                  AS customer_id,
        inv.investigated_information__is_customer                  AS is_cliente_nu,

        ext.official_letter_extraction__is_reiteration             AS is_reiteracao,
        ext.official_letter_extraction__confirmed_or_rejected_at   AS triado_em,
        ext.official_letter_extraction__confirmed_or_rejected_by   AS triado_por,
        ext.official_letter_extraction__requested_information       AS info_solicitada,

        SIZE(ol.investigated_information__id)                       AS total_investigados

    FROM etl.br__dataset.jud_athena_official_letter_extractions ext

    INNER JOIN etl.br__dataset.jud_athena_submissions sub
        ON ext.official_letter_extraction__submission_id = sub.submission__id

    INNER JOIN etl.br__dataset.jud_athena_official_letters ol
        ON ol.official_letter__submission_id = sub.submission__id

    LATERAL VIEW POSEXPLODE(ol.investigated_information__id) inv_exploded AS inv_pos, inv_id

    LEFT JOIN etl.br__contract.jud_athena__investigated_information inv
        ON inv.investigated_information__id = inv_exploded.inv_id

    LEFT JOIN etl.br__contract.jud_athena__investigated_information_name_pii name_pii
        ON name_pii.hash = inv.investigated_information__name

    LEFT JOIN etl.br__contract.jud_athena__investigated_information_cpf_cnpj_pii cpf_pii
        ON cpf_pii.hash = inv.investigated_information__cpf_cnpj

    WHERE ext.official_letter_extraction__status = 'official_letter_extraction_status__confirmed'
      AND ol.official_letter__type IN (
          'official_letter_type__block',
          'official_letter_type__dismiss',
          'official_letter_type__transfer'
      )
      AND ext.official_letter_extraction__process_document_number IS NOT NULL
      AND ext.official_letter_extraction__created_at >= current_date() - INTERVAL 12 DAYS
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY numero_processo, investigated_id
            ORDER BY triado_em DESC
        ) AS rn
    FROM exploded
)

SELECT *
FROM ranked
WHERE rn = 1
ORDER BY data_recebimento DESC, numero_processo, investigado_seq

-- =============================================================================
-- CMD 2: Resumo por tipo de ofício (cole em um novo command)
-- =============================================================================

-- WITH ranked AS (
--     SELECT
--         ol.official_letter__type AS tipo,
--         ext.official_letter_extraction__process_document_number AS nr_processo,
--         ext.official_letter_extraction__created_at AS data_recebimento,
--         inv.investigated_information__is_customer AS is_cliente_nu,
--         ext.official_letter_extraction__is_reiteration AS is_reiteracao,
--         ROW_NUMBER() OVER (
--             PARTITION BY ext.official_letter_extraction__process_document_number
--             ORDER BY ext.official_letter_extraction__confirmed_or_rejected_at DESC
--         ) AS rn
--     FROM etl.br__dataset.jud_athena_official_letter_extractions ext
--     INNER JOIN etl.br__dataset.jud_athena_submissions sub
--         ON ext.official_letter_extraction__submission_id = sub.submission__id
--     INNER JOIN etl.br__dataset.jud_athena_official_letters ol
--         ON ol.official_letter__submission_id = sub.submission__id
--     LEFT JOIN etl.br__contract.jud_athena__investigated_information inv
--         ON inv.investigated_information__id = ol.investigated_information__id[0]
--     WHERE ext.official_letter_extraction__status = 'official_letter_extraction_status__confirmed'
--       AND ol.official_letter__type IN ('official_letter_type__block', 'official_letter_type__dismiss')
--       AND ext.official_letter_extraction__process_document_number IS NOT NULL
--       AND ext.official_letter_extraction__created_at >= current_date() - INTERVAL 12 DAYS
-- )
-- SELECT
--     CASE tipo
--         WHEN 'official_letter_type__block'   THEN 'BLOQUEIO'
--         WHEN 'official_letter_type__dismiss' THEN 'DESBLOQUEIO'
--         ELSE tipo
--     END AS tipo_legivel,
--     COUNT(*) AS total_casos,
--     SUM(CASE WHEN is_cliente_nu THEN 1 ELSE 0 END) AS clientes_nu,
--     SUM(CASE WHEN NOT is_cliente_nu OR is_cliente_nu IS NULL THEN 1 ELSE 0 END) AS nao_clientes,
--     SUM(CASE WHEN is_reiteracao THEN 1 ELSE 0 END) AS reiteracoes,
--     ROUND(AVG(DATEDIFF(day, data_recebimento, current_date())), 1) AS media_dias_pendente
-- FROM ranked
-- WHERE rn = 1
-- GROUP BY 1
-- ORDER BY total_casos DESC
