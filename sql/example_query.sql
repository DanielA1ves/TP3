-- Exemplo: estatisticas de parques por estado e intervalo de datas
SELECT
  COUNT(DISTINCT NULLIF(x.park_code, ''))::int AS park_count,
  AVG(NULLIF(x.temp_mean_c, '')::float) AS avg_temp_mean_c,
  AVG(NULLIF(x.precipitation_sum_mm, '')::float) AS avg_precip_sum_mm
FROM xml_documents d,
XMLTABLE(
  '/ParkData/Parks/Park/Weather/Observation'
  PASSING d.xml_documento
  COLUMNS
    observation_date TEXT PATH 'Date',
    state_code TEXT PATH '../../State/Code',
    park_code TEXT PATH '../../Code',
    temp_mean_c TEXT PATH 'TempMeanC',
    precipitation_sum_mm TEXT PATH 'Precipitation/SumMm'
) AS x
WHERE x.state_code = 'CA'
  AND NULLIF(x.observation_date, '')::date BETWEEN '2025-04-01' AND '2025-04-30';

-- Para datas futuras use PrecipitationProbability:
-- AVG(NULLIF(x.precipitation_probability, '')::float) AS avg_precip_probability
