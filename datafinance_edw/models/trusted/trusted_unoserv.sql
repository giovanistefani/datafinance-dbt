
SELECT
    data_transacao,
    codigo_id,
    valor_credito,
    valor_debito,
    valor_saldo,
    historico_transacao,
    documento_referencia,
    nome_cliente_fornecedor
FROM
    {{ ref('stg_unoserv') }}

