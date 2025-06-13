SELECT
    -- Converte a coluna 'data' de DD/MM/YYYY para DATE.
    TRY_CAST(strptime(DATA, '%d/%m/%Y') AS DATE) AS data_transacao, 
    -- Converte a coluna 'código' para INTEGER.
    TRY_CAST(CÓDIGO AS INTEGER) AS codigo_id, 
    -- Converte as colunas 'crédito', 'débito' e 'saldo' para NUMERIC.
    -- 1. Removendo o separador de milhar (ponto).
    -- 2. Substituíndo a vírgula decimal por um ponto decimal.
    TRY_CAST(REPLACE(REPLACE(CRÉDITO, '.', ''), ',', '.') AS NUMERIC(18, 2)) AS valor_credito,
    TRY_CAST(REPLACE(REPLACE(DÉBITO, '.', ''), ',', '.') AS NUMERIC(18, 2)) AS valor_debito,
    TRY_CAST(REPLACE(REPLACE(SALDO, '.', ''), ',', '.') AS NUMERIC(18, 2)) AS valor_saldo,
    -- Selecionando e renomeando as colunas restantes explicitamente
    HISTÓRICO AS historico_transacao, 
    DOCUMENTO AS documento_referencia,
    "CLIENTE / FORNECEDOR" AS nome_cliente_fornecedor
FROM {{ ref('unoserv') }}