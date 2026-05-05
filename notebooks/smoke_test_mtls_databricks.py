# Databricks notebook source
# MAGIC %md
# MAGIC # LexIA-CR — Smoke Test mTLS no Databricks
# MAGIC
# MAGIC **Objetivo**: validar se o cluster Databricks consegue chamar os 5 serviços internos
# MAGIC Nubank que o LexIA-CR usa hoje (Waze, Customers, Crébito/Facade, Rayquaza, Petrificus)
# MAGIC **antes** de migrar o pipeline.
# MAGIC
# MAGIC ## Pré-requisitos
# MAGIC
# MAGIC - Cluster **Single-User (Dedicated)** — `nu_requests` não funciona em Standard/Shared.
# MAGIC - Policy do cluster com `[NEW]-*` ou `databricks-jobs-meta-role` (acesso S3 ao bucket de cert).
# MAGIC - Workspace `nubank-e2-general.cloud.databricks.com`.
# MAGIC
# MAGIC ## Como interpretar o resultado
# MAGIC
# MAGIC | Outcome do service | Significado | Ação |
# MAGIC | --- | --- | --- |
# MAGIC | `2xx`, `4xx (404/400)` | Rede OK + mTLS OK + service respondendo | Pronto pra usar |
# MAGIC | `401`, `403` | Rede OK, mas scope `databricks` não autorizado | PR no auth-policy do service |
# MAGIC | `ConnectTimeout`, `ConnectError` | Rede bloqueada (sem VPC peering) | Ticket Data Platform — ~6-12 sem |
# MAGIC | `cert: AccessDenied` | Cluster sem IAM no S3 do cert | Ticket DATASUP — ~1 sem |
# MAGIC
# MAGIC O Cell 4 (último) consolida tudo em um veredicto final.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1 — Instalar `nu-requests` e reiniciar Python

# COMMAND ----------

# MAGIC %pip install nu-requests==1.1.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Carregar cert mTLS e token Bearer
# MAGIC
# MAGIC `nu_requests` lê o cert `.p12` direto do S3 (`nu-keysets-data-prod/.../databricks.p12`)
# MAGIC e o token Bearer de `nu-keysets-data-prod/tokens/databricks/uber-token.json`.
# MAGIC Se este cell falhar com `AccessDenied`, o cluster está sem IAM pro bucket.

# COMMAND ----------

import os

os.environ["NU_ENV"] = "prod"
os.environ["NU_COUNTRY"] = "br"

from nu_requests.cert import load_certificate
from nu_requests.tokens import default_token_provider
from nu_requests.utils import get_env_info

ENV_INFO = get_env_info(kwargs={"client_type": "services", "service": "databricks"})

try:
    with load_certificate(env_info=ENV_INFO) as _probe_cert:
        _probe_token = default_token_provider.get_token(**ENV_INFO)
        print(f"OK — cert carregado de S3, token Bearer obtido (len={len(_probe_token)})")
except Exception as exc:  # pragma: no cover - smoke test
    print(f"FALHA ao carregar cert/token: {type(exc).__name__}: {exc}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3 — Probar os 5 services internos
# MAGIC
# MAGIC Faz `GET` simples em cada host. Para 4 deles usa shard `tatu` (shard real Nubank,
# MAGIC só pra montar a URL — o que importa é se a conexão TCP+TLS abre, não se a resposta
# MAGIC é semanticamente útil). Se o shard `tatu` não for válido pro service, vai aparecer
# MAGIC `name resolution` no detail — isso ainda é útil pra distinguir "rede bloqueada"
# MAGIC de "shard inválido".

# COMMAND ----------

import time

import httpx

SHARD = "tatu"  # shard real Nubank, usado só pra montar URL (não pra validar dados)

# Hosts extraídos de scripts/run_traced_pipeline.py (linhas 656-848)
ENDPOINTS = [
    ("waze",       "https://prod-global-waze.nubank.com.br/_internal/health"),
    ("customers",  f"https://prod-{SHARD}-customers.nubank.com.br/_internal/health"),
    ("crebito",    f"https://prod-{SHARD}-facade.nubank.com.br/_internal/health"),
    ("rayquaza",   f"https://prod-{SHARD}-rayquaza.nubank.com.br/_internal/health"),
    ("petrificus", f"https://prod-{SHARD}-petrificus-parcialus.nubank.com.br/_internal/health"),
]

results: list[dict] = []
with load_certificate(env_info=ENV_INFO) as cert:
    token = default_token_provider.get_token(**ENV_INFO)
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Correlation-Id": "LEXIA-SMOKE-TEST",
        "User-Agent": "lexia-cr/smoke-test",
    }
    client = httpx.Client(cert=cert, headers=headers, timeout=15.0)
    for name, url in ENDPOINTS:
        t0 = time.monotonic()
        try:
            r = client.get(url)
            elapsed = int((time.monotonic() - t0) * 1000)
            results.append({
                "service": name,
                "url": url,
                "status": r.status_code,
                "kind": "OK",
                "ms": elapsed,
                "detail": (r.text or "")[:120].replace("\n", " "),
            })
        except httpx.ConnectTimeout:
            elapsed = int((time.monotonic() - t0) * 1000)
            results.append({"service": name, "url": url, "status": "-", "kind": "CONNECT_TIMEOUT",
                            "ms": elapsed, "detail": "no route — provavelmente sem VPC peering"})
        except httpx.ConnectError as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            msg = str(exc)[:120]
            kind = "DNS_FAIL" if "name resolution" in msg.lower() or "nodename" in msg.lower() else "CONNECT_ERROR"
            results.append({"service": name, "url": url, "status": "-", "kind": kind,
                            "ms": elapsed, "detail": msg})
        except httpx.ReadTimeout:
            elapsed = int((time.monotonic() - t0) * 1000)
            results.append({"service": name, "url": url, "status": "-", "kind": "READ_TIMEOUT",
                            "ms": elapsed, "detail": "TLS abriu mas response não veio"})
        except Exception as exc:  # pragma: no cover - smoke test
            elapsed = int((time.monotonic() - t0) * 1000)
            results.append({"service": name, "url": url, "status": "-", "kind": type(exc).__name__,
                            "ms": elapsed, "detail": str(exc)[:120]})

print(f"\n{'service':<12} {'status':<7} {'kind':<18} {'ms':<7} detail")
print("-" * 110)
for row in results:
    print(f"{row['service']:<12} {str(row['status']):<7} {row['kind']:<18} {row['ms']:<7} {row['detail']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4 — Veredicto e recomendação
# MAGIC
# MAGIC Classifica cada service em GREEN / YELLOW / RED e devolve a recomendação final
# MAGIC sobre o caminho de migração (Databricks Job vs Tekton/controlinho).

# COMMAND ----------

def classify(row: dict) -> str:
    kind = row["kind"]
    status = row["status"]
    if kind == "OK" and isinstance(status, int):
        if status in (401, 403):
            return "YELLOW"
        if 200 <= status < 600:
            return "GREEN"
    if kind in ("CONNECT_TIMEOUT", "CONNECT_ERROR", "READ_TIMEOUT", "DNS_FAIL"):
        return "RED"
    return "RED"

categorized = [(classify(r), r) for r in results]
greens = [r for cat, r in categorized if cat == "GREEN"]
yellows = [r for cat, r in categorized if cat == "YELLOW"]
reds = [r for cat, r in categorized if cat == "RED"]

print("=" * 70)
print(f"Resumo: {len(greens)} GREEN | {len(yellows)} YELLOW | {len(reds)} RED")
print("=" * 70)

if greens:
    print("\nGREEN — rede + mTLS OK, prontos pra usar:")
    for r in greens:
        print(f"  - {r['service']:<12} status={r['status']} ({r['ms']}ms)")

if yellows:
    print("\nYELLOW — rede OK, mas scope `databricks` não autorizado (PR no auth-policy):")
    for r in yellows:
        print(f"  - {r['service']:<12} status={r['status']}")

if reds:
    print("\nRED — bloqueados (precisam VPC peering ou DNS/IAM fix):")
    for r in reds:
        print(f"  - {r['service']:<12} kind={r['kind']:<18} {r['detail'][:60]}")

print("\n" + "=" * 70)
print("VEREDICTO")
print("=" * 70)

if len(reds) == 0 and len(yellows) == 0:
    print("Databricks Job é viável diretamente. Migração estimada: 1 sprint.")
    print("Próximo passo: empacotar o pipeline como wheel/notebook e criar o Job.")
elif len(reds) == 0 and len(yellows) > 0:
    print(f"Databricks Job é viável após {len(yellows)} PR(s) de auth-policy.")
    print("Próximo passo: PRs nos repos dos services YELLOW + migração paralela.")
elif len(reds) <= 2:
    print(f"Parcialmente viável. {len(reds)} service(s) com rede bloqueada.")
    print("Custo: ~6-12 semanas por ticket de VPC peering com Data Platform.")
    print("Alternativa: migrar pra Tekton (`controlinho` style) — mais rápido.")
else:
    print(f"Inviável a curto prazo. {len(reds)}/5 services bloqueados por rede.")
    print("Recomendação: migrar pra Tekton (`controlinho` style) ao invés de")
    print("Databricks Job. O pod Tekton já vive na malha mTLS interna.")

print("\nDetalhes da pesquisa em: docs/migration-databricks-vs-tekton.md (a criar)")
