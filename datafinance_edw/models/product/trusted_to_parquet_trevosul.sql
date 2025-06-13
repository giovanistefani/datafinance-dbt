{{
  config(materialized='external', location='data/product/trusted_to_product_trevosul.parquet', format='parquet')
}}

-- Este modelo seleciona todos os dados de trusted_unoserv
-- e os materializa como um arquivo Parquet no local especificado.

SELECT
    *
FROM {{ ref('trusted_trevosul') }}
