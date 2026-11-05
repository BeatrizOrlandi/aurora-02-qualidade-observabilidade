# 02 — Qualidade e observabilidade

> **Governança de dados** · Status: **MVP validado**  
> Parte do portfólio [Beatriz Helena — Engenharia de IA](https://beatrizorlandi.github.io/projetos/qualidade-observabilidade.html)

Checks que distinguem sucesso técnico de qualidade de dados, com falhas controladas e recuperação verificável.

**Stack principal:** `Python` · `PySpark` · `Delta Lake` · `Databricks Jobs` · `MLflow`

---

## O Problema

Uma carga pode terminar sem erro e publicar indicadores incorretos. O consumidor precisa continuar vendo a última saída válida quando a nova carga falhar.

### Escopo Entregue

Oito regras operacionais, tabela de checks e uma suíte de 24 asserções de integração. Os ensaios incluem duplicidades, conflitos de payload, eventos atrasados, quarentena e backfill explícito.

---

## Arquitetura e Fluxo

**1. Carga candidata** → **2. Checks por regra** → **3. Gate crítico** → **4. Aprovar ou bloquear** → **5. Preservar publicação** → **6. Evidência**

### Decisões Técnicas

- Separar alertas de regras que bloqueiam publicação.
- Verificar falhas injetadas e valores esperados manualmente, além do caminho de sucesso.
- Preservar resultados e hashes por execução para investigar diferenças.

---

## Resultados Medidos

| Métrica / Indicador | Valor Observado |
| --- | --- |
| **8** | checks operacionais |
| **24/24** | asserções de integração aprovadas |
| **10/10** | testes unitários aprovados |

A rodada final de integração passou nas 24 asserções em 28/10/2026. A correção de um gabarito de conflito e a execução anterior com falha foram preservadas no histórico do laboratório.


---

## Limites Conhecidos e Próximos Passos

### Limites Conhecidos
- Os testes cobrem cenários definidos de um corpus pequeno; não representam garantia de todos os casos de produção.
- Monitoramento de drift, notificações e SLOs operacionais ainda não foram implementados.

### Próximos Incrementos
- Adicionar observação de distribuição com uma referência versionada.
- Formalizar SLOs e um procedimento de investigação para cada alerta.

---

## Estrutura do Repositório

- `informacoes_tecnicas/PLANEJAMENTO.md`: Planejamento técnico detalhado (requisitos, contratos, arquitetura, MLflow e validação).
- `00_PLANEJAMENTO.py`: Versão em notebook Databricks do planejamento técnico.
- Notebooks e módulos em Python/PySpark (quando aplicável ao estágio de implementação do estudo).
- `evidencias/` (quando aplicável): Resumo estático das execuções validadas no Databricks.

## Links

- **Portfólio Completo:** [https://beatrizorlandi.github.io](https://beatrizorlandi.github.io)
- **Estudo de Caso no Site:** [https://beatrizorlandi.github.io/projetos/qualidade-observabilidade.html](https://beatrizorlandi.github.io/projetos/qualidade-observabilidade.html)
- **Planejamento Técnico no Site:** [https://beatrizorlandi.github.io/documentacao/qualidade-observabilidade.html](https://beatrizorlandi.github.io/documentacao/qualidade-observabilidade.html)
