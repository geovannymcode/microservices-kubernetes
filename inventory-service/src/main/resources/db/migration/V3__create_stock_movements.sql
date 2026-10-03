-- One row per applied stock decrease that carried an Idempotency-Key. It is inserted in the same
-- transaction as the UPDATE, so a retried request finds the key and does not decrease twice.
CREATE TABLE stock_movements (
    idempotency_key VARCHAR(64) NOT NULL,
    product_code VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT pk_stock_movements PRIMARY KEY (idempotency_key),
    CONSTRAINT ck_stock_movements_quantity CHECK (quantity > 0)
) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin;
