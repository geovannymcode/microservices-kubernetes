-- Contract v2.1.0: Idempotency-Key accepts 1..100 characters from [A-Za-z0-9._:-] (was 1..64 without . and :).
-- Widening only; V3 is already applied and is never edited. The table charset (ascii, ascii_bin) still holds.
ALTER TABLE stock_movements MODIFY idempotency_key VARCHAR(100) NOT NULL;
