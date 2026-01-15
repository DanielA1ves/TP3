CREATE TABLE IF NOT EXISTS xml_documents (
  id SERIAL PRIMARY KEY,
  xml_documento XML NOT NULL,
  data_criacao TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  mapper_version TEXT NOT NULL
);
