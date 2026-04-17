"""Query jud_athena_* tables in Databricks for pending judicial cases."""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from databricks import sql as dbsql

from lexia.config import settings

log = structlog.get_logger(__name__)

QUERY_CASES = """
SELECT
    ol.official_letter__id                          AS id,
    ol.official_letter__type                        AS tipo_oficio,
    ext.official_letter_extraction__craft_document_number    AS numero_oficio,
    ext.official_letter_extraction__process_document_number  AS numero_processo,
    ol.official_letter__status                       AS status_oficio,
    ol.official_letter__origin                       AS origem,
    ol.official_letter__deadline_date                AS prazo,
    ol.official_letter__submission_id                AS submission_id,

    ext.official_letter_extraction__requested_item           AS item_solicitado,
    ext.official_letter_extraction__court_tribunal_name      AS vara_tribunal,
    ext.official_letter_extraction__organ_name               AS orgao_nome,
    ext.official_letter_extraction__organ_address            AS orgao_endereco,
    ext.official_letter_extraction__response_email           AS email_resposta,
    ext.official_letter_extraction__created_at               AS data_recebimento,
    ext.official_letter_extraction__is_reiteration           AS is_reiteracao,
    ext.official_letter_extraction__office_text_observations AS observacoes,
    ext.official_letter_extraction__zendesk_ticket_number    AS ticket_zendesk,
    ext.official_letter_extraction__final_deadline           AS prazo_final,
    ext.official_letter_extraction__start_date               AS data_inicio,
    ext.official_letter_extraction__end_date                 AS data_fim,

    name_pii.investigated_information__name       AS nome_investigado,
    cpf_pii.investigated_information__cpf_cnpj    AS cpf_cnpj,
    inv.investigated_information__requested_value AS valor_solicitado,
    inv.investigated_information__customer_id     AS customer_id,
    inv.investigated_information__is_customer     AS is_cliente_nu

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
  AND ext.official_letter_extraction__created_at >= current_date() - INTERVAL {days_back} DAYS
"""


@dataclass
class JudicialCase:
    """A single judicial case ready for processing."""

    id: str
    tipo_oficio: str
    numero_oficio: str | None
    numero_processo: str | None
    status_oficio: str | None
    origem: str | None
    prazo: str | None
    submission_id: str | None
    item_solicitado: str | None
    vara_tribunal: str | None
    orgao_nome: str | None
    orgao_endereco: str | None
    email_resposta: str | None
    data_recebimento: str | None
    is_reiteracao: bool
    observacoes: str | None
    ticket_zendesk: str | None
    prazo_final: str | None
    data_inicio: str | None
    data_fim: str | None
    nome_investigado: str | None
    cpf_cnpj: str | None
    valor_solicitado: str | None
    customer_id: str | None
    is_cliente_nu: bool


def fetch_pending_cases(days_back: int | None = None) -> list[JudicialCase]:
    """Fetch pending judicial cases from Databricks."""
    days = days_back or settings.days_back

    log.info("fetching_cases", days_back=days)

    with dbsql.connect(
        server_hostname=settings.databricks_host.replace("https://", ""),
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(QUERY_CASES.format(days_back=days))
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

    cases = []
    for row in rows:
        data = dict(zip(columns, row, strict=False))
        cases.append(
            JudicialCase(
                id=data["id"],
                tipo_oficio=data["tipo_oficio"],
                numero_oficio=data.get("numero_oficio"),
                numero_processo=data.get("numero_processo"),
                status_oficio=data.get("status_oficio"),
                origem=data.get("origem"),
                prazo=str(data.get("prazo", "")),
                submission_id=data.get("submission_id"),
                item_solicitado=data.get("item_solicitado"),
                vara_tribunal=data.get("vara_tribunal"),
                orgao_nome=data.get("orgao_nome"),
                orgao_endereco=data.get("orgao_endereco"),
                email_resposta=data.get("email_resposta"),
                data_recebimento=str(data.get("data_recebimento", "")),
                is_reiteracao=bool(data.get("is_reiteracao")),
                observacoes=data.get("observacoes"),
                ticket_zendesk=data.get("ticket_zendesk"),
                prazo_final=str(data.get("prazo_final", "")),
                data_inicio=str(data.get("data_inicio", "")),
                data_fim=str(data.get("data_fim", "")),
                nome_investigado=data.get("nome_investigado"),
                cpf_cnpj=data.get("cpf_cnpj"),
                valor_solicitado=data.get("valor_solicitado"),
                customer_id=data.get("customer_id"),
                is_cliente_nu=bool(data.get("is_cliente_nu")),
            )
        )

    log.info("cases_fetched", total=len(cases))
    return cases
