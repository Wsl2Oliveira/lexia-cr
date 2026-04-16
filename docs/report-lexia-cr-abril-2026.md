# LexIA CR — Report de Status | Abril 2026

**Projeto:** 175.LexIA — Confecção de Carta-Resposta Judicial via IA
**Responsável:** Wesley Oliveira
**Data:** 14 de abril de 2026

---

## O que é

- Automação que gera cartas-resposta de ofícios judiciais usando inteligência artificial
- Funciona de forma independente, sem depender do UiPath/RPA
- Cobre hoje as filas de **bloqueio** e **desbloqueio** (OOS)

---

## O que já foi entregue

- Pipeline completo funcionando de ponta a ponta
- Busca automática dos casos pendentes no Databricks (últimos 12 dias)
- Consulta automática dos dados do cliente nas bases internas (conta, cartões, saldo, bloqueios)
- IA (Gemini) analisa o caso e escolhe a resposta correta entre 10 macros
- Carta-resposta gerada automaticamente no template padrão e salva como Google Docs no Drive
- Rastreabilidade completa: cada decisão registrada em planilha com 23+ campos
- Busca automática do PDF do ofício original no Drive
- 9 macros de bloqueio e 1 macro de desbloqueio implementadas
- Testado e validado com 5 processos reais

---

## O que mudou em relação ao fluxo anterior

- Não depende mais do UiPath nem de licença
- Não precisa mais do banco Oracle da JD — consulta direto no Databricks
- Consulta mais completa do cliente (conta + todos os cartões, inclusive bloqueados)
- Carta gerada pela IA ao invés de manualmente pelo analista
- Rastreabilidade completa no lugar do bot Thunder

---

## Próximos passos

- Ampliar respostas de desbloqueio (parcial, com transferência pendente, conta cancelada)
- Rodar com volume maior para medir taxa de acerto da IA
- Submeter cartas para validação do time de Quality
- Automatizar execução em horários programados
- Expandir para transferência e solicitação de informações
- Analista passa a apenas validar a carta pronta (~7 min economizados por caso)

---

## Impacto esperado

- Bloqueio: de ~19 min para ~12 min por caso
- Desbloqueio: de ~13 min para ~6 min por caso
- 3 atividades eliminadas por caso (localizar ofício, elaborar carta, salvar no Drive)
- Economia de ~7 minutos por caso

---

## Pontos de atenção

- Credenciais de acesso às APIs expiram periodicamente — renovação manual por enquanto
- Toda carta passa por validação humana antes do envio
- Mudanças nas tabelas do Databricks são acompanhadas ativamente

> **Nota:** O LexIA funciona de forma independente. Uma possível sinergia com o Athena foi discutida em reunião exploratória, mas não faz parte do escopo de entrega atual.
