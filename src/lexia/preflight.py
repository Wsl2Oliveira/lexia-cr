"""Preflight de credenciais nucli — auto-refresh sem 2FA.

Encapsula a lógica de garantir que o token Bearer do `nucli` está válido
antes do pipeline tentar usar a API mTLS da Nubank. Usa o comando silencioso
`nu auth get-access-token` que renova o access token via refresh-token salvo
em disco, sem prompt 2FA.

Fluxo:
  1. Chama `nu auth get-access-token` (timeout 20s)
  2. Valida exit_code == 0 E presença do marker "Done!" no stderr
  3. Se sucesso → access token foi renovado (ou já estava válido)
  4. Se falha → refresh-token também expirou; precisa rodar `nucli`
     manualmente com 2FA (intervenção esperada ~1x por mês)

Baseado em pattern validado em produção pelo projeto Dossiê RPA.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

NU_BINARY_NAME = "nu"

NU_BINARY_CANDIDATES = (
    Path.home() / "dev/nu/nucli/nu",
    Path("/opt/homebrew/bin/nu"),
    Path("/usr/local/bin/nu"),
)

REFRESH_SUCCESS_MARKER = "Done"

REFRESH_TIMEOUT_SECONDS = 20


def _resolve_nu_binary() -> str:
    """Acha o binário `nu` no PATH ou em locations conhecidas.

    Defensivo contra launchd/cron não herdarem PATH do shell interativo:
    mesmo se ``which nu`` falhar, tenta caminhos absolutos comuns.

    Returns:
        Caminho do binário se encontrado, senão "nu" (subprocess vai
        levantar FileNotFoundError identificável depois).
    """
    found = shutil.which(NU_BINARY_NAME)
    if found:
        return found

    for cand in NU_BINARY_CANDIDATES:
        if cand.is_file() and os.access(cand, os.X_OK):
            return str(cand)

    return NU_BINARY_NAME


def _build_nu_env() -> dict[str, str]:
    """Garante env vars mínimas que o nucli exige.

    O nucli usa scripts shell que dependem de:
      - ``NU_HOME``: base path para certs/tokens/VPN (default ~/dev/nu)
      - ``PATH``: precisa incluir GNU bash 4+ e gawk de Homebrew (nucli não
        funciona com Bash 3.2 nativo do macOS nem com BSD awk)
      - ``HOME``, ``USER``, ``LOGNAME``: básicas que ferramentas Unix esperam

    Quando o pipeline é invocado por launchd/cron, várias dessas podem estar
    ausentes do ``os.environ``. Aqui consolidamos um env mínimo viável,
    preservando o que já está setado.
    """
    env = os.environ.copy()
    env.setdefault("NU_HOME", str(Path.home() / "dev/nu"))
    env.setdefault("HOME", str(Path.home()))
    env.setdefault("USER", os.environ.get("LOGNAME", "wesley.oliveira"))
    env.setdefault("LOGNAME", env["USER"])

    nu_bin_dir = str(Path.home() / "dev/nu/nucli")
    homebrew = "/opt/homebrew/bin"
    usr_local = "/usr/local/bin"
    base_path = env.get("PATH", "/usr/bin:/bin")
    needed = [nu_bin_dir, homebrew, usr_local]
    extras = [p for p in needed if p not in base_path.split(":")]
    if extras:
        env["PATH"] = ":".join([*extras, base_path])
    return env


def ensure_nu_auth(timeout: int = REFRESH_TIMEOUT_SECONDS) -> tuple[bool, str]:
    """Garante que o token Bearer do nucli está válido (auto-refresh sem 2FA).

    Chama ``nu auth get-access-token``, que:
      - Se access token está válido: imprime e retorna 0
      - Se access expirou + refresh válido: renova silenciosamente, retorna 0
      - Se refresh também expirou: retorna != 0 (precisa 2FA manual)

    Args:
        timeout: segundos antes de desistir do subprocess (default 20s).

    Returns:
        (ok, motivo)
        - (True, "renovado" | "ok"): há token válido em disco após chamada
        - (False, "<motivo>"): precisa intervenção manual com `nucli`
    """
    nu = _resolve_nu_binary()
    env = _build_nu_env()
    try:
        result = subprocess.run(
            [nu, "auth", "get-access-token"],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return False, f"timeout (>{timeout}s) em `nu auth get-access-token`"
    except FileNotFoundError:
        return (
            False,
            f"binário `nu` não encontrado (procurei em PATH e {NU_BINARY_CANDIDATES})",
        )

    output = (result.stderr or "") + (result.stdout or "")
    success = result.returncode == 0 and REFRESH_SUCCESS_MARKER in output

    if success:
        return True, "ok"

    snippet = output.strip().splitlines()
    last_lines = " | ".join(snippet[-3:]) if snippet else "(sem output)"
    return False, f"rc={result.returncode} output={last_lines[:200]}"


def ensure_nu_auth_or_raise(timeout: int = REFRESH_TIMEOUT_SECONDS) -> None:
    """Wrapper que levanta RuntimeError com mensagem acionável.

    Usado quando o caller quer abortar imediatamente em caso de falha,
    em vez de inspecionar o tuple. Mensagem inclui ação concreta para
    o operador (rodar `nucli` com 2FA).
    """
    ok, reason = ensure_nu_auth(timeout=timeout)
    if not ok:
        raise RuntimeError(
            "[PREFLIGHT] Auth nucli falhou — refresh-token provavelmente expirou. "
            f"Motivo: {reason}. "
            "Ação: rodar `nucli` no terminal (com 2FA) para regenerar tokens, "
            "depois re-disparar o pipeline."
        )
