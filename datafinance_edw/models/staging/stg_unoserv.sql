SELECT
    -- Converte a coluna 'data' de DD/MM/YYYY para DATE.
    -- Para DuckDB (adaptador mencionado no log), strptime é uma boa opção.
    TRY_CAST(strptime(DATA, '%d/%m/%Y') AS DATE) AS data_transacao, -- Renomeando para clareza

    -- Converte a coluna 'código' para INTEGER.
    -- Usar TRY_CAST é recomendado, pois retorna NULL se a conversão falhar,
    -- evitando que o modelo quebre por dados inválidos.
    TRY_CAST(CÓDIGO AS INTEGER) AS codigo_id, -- Confirmar que 'código' está com acento

    -- Converte as colunas 'crédito', 'débito' e 'saldo' para NUMERIC.
    -- NUMERIC (ou DECIMAL) é o tipo apropriado para valores financeiros ou outros números
    -- que exigem precisão decimal.
    -- 1. Removemos o separador de milhar (ponto).
    -- 2. Substituímos a vírgula decimal por um ponto decimal.
    TRY_CAST(REPLACE(REPLACE(CRÉDITO, '.', ''), ',', '.') AS NUMERIC(18, 2)) AS valor_credito, -- Confirmar 'crédito' com acento
    TRY_CAST(REPLACE(REPLACE(DÉBITO, '.', ''), ',', '.') AS NUMERIC(18, 2)) AS valor_debito, -- Confirmar 'débito' com acento
    TRY_CAST(REPLACE(REPLACE(SALDO, '.', ''), ',', '.') AS NUMERIC(18, 2)) AS valor_saldo,

    -- Para incluir todas as outras colunas da sua tabela seed:
    -- Opção 1 (Recomendada com dbt): Use a macro `star` do pacote `dbt_utils`.
    --    Isso requer que o pacote `dbt-utils` esteja instalado no seu projeto dbt
    --    (adicione-o ao seu arquivo `packages.yml` e rode `dbt deps`).
    --    Esta macro selecionará todas as colunas do seed, exceto as já listadas/transformadas acima.
    --    Os nomes na lista 'except' devem ser
    --    os nomes das colunas como são no seed 'unoserv' (após a normalização do dbt,
    --    geralmente minúsculas e com espaços substituídos por underscores). Confirmar acentos e CASO aqui também.
    {{ dbt_utils.star(from=ref('unoserv'), except=["DATA", "CÓDIGO", "CRÉDITO", "DÉBITO", "SALDO"]) }}

    -- Opção 2 (Manual): Se você não usa `dbt_utils` ou prefere ser explícito,
    --    liste todas as outras colunas aqui, separadas por vírgula, usando os nomes normalizados.
    --    Exemplo:
    --    historico,
    --    documento,
    --    cliente_fornecedor
    --    -- Certifique-se de remover a linha com `dbt_utils.star` acima se optar por esta abordagem.

-- Referencia o seed 'unoserv'. O dbt cuidará de encontrar a tabela correta.
FROM {{ ref('unoserv') }}