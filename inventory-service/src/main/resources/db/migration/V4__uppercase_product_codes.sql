-- Contract v2.0.0: product codes are uppercase only (^[A-Z0-9-]{1,50}$). The column collation is
-- case-insensitive, so lowercase codes acted as aliases; normalising cannot collide because the
-- UNIQUE constraint was already case-insensitive. The CHECK guards against writes that bypass the API.
UPDATE products SET code = UPPER(code) WHERE BINARY code <> BINARY UPPER(code);
UPDATE stock_movements SET product_code = UPPER(product_code)
    WHERE BINARY product_code <> BINARY UPPER(product_code);
ALTER TABLE products
    ADD CONSTRAINT ck_products_code_format CHECK (REGEXP_LIKE(code, '^[A-Z0-9-]{1,50}$', 'c'));
