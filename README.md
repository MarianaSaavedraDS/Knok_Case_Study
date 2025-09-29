To run the sql queries: psql -U postgres -d knok -f knok_queries.sql > query_results.txt

## Updates (29/09)

Aproveitei este intervalo de espera para implentar as queries utilizando a ferramenta dbt.

Podem ver o código em:

* `knok_project\models`

No contexto deste desafio, achei interessante aplicar dbt, porque permite transformar os dados de forma estruturada e reprodutível.

Com dbt, é mais fácil criar **modelos, documentação e testes**, o que facilita a colaboração em equipa e garante que dashboards e relatórios estão sempre atualizados com dados limpos e fiáveis. 

Em comparação com a limpeza e agregação feita apenas em notebooks, dbt torna o processo de transformação mais robusto, reutilizável e escalável, simplificando todo o fluxo de análise e reporting.