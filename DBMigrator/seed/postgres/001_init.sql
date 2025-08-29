CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE public.types_demo (
  id SERIAL PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  amount NUMERIC(38,9),
  payload BYTEA,
  created_at TIMESTAMP NOT NULL DEFAULT now(),
  notes TEXT,
  meta JSONB
);

INSERT INTO public.types_demo (name, active, amount, payload, notes, meta)
SELECT
  'row_' || g, (g % 2 = 0),
  (g * 1.23456789)::numeric(38,9),
  decode(md5(g::text), 'hex'),
  CASE WHEN g % 5 = 0 THEN repeat('x', 1000) ELSE null END,
  jsonb_build_object('i', g, 's', 'v'||g)
FROM generate_series(1, 15000) AS g;

-- reltuples can be stale unless ANALYZE
ANALYZE public.types_demo;