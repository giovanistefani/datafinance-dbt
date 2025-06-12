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

    -- Selecionando e renomeando as colunas restantes explicitamente
    HISTÓRICO AS historico_transacao, -- Confirmar 'HISTÓRICO' com acento
    DOCUMENTO AS documento_referencia,
    -- Se o nome da coluna original no banco de dados contém espaços/caracteres especiais,
    -- ele precisa ser envolvido em aspas duplas.
    "CLIENTE / FORNECEDOR" AS nome_cliente_fornecedor


-- Referencia o seed 'unoserv'. O dbt cuidará de encontrar a tabela correta.
FROM {{ ref('unoserv') }}