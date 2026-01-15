const path = require("path");
const express = require("express");
const axios = require("axios");
const FormData = require("form-data");
const {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  DeleteObjectCommand,
} = require("@aws-sdk/client-s3");
const { parse } = require("csv-parse");
const { stringify } = require("csv-stringify/sync");

function parseNonNegativeInt(value, fallback) {
  const parsed = parseInt(value, 10);
  if (Number.isFinite(parsed) && parsed >= 0) {
    return parsed;
  }
  return fallback;
}

function parseBoolean(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "y", "on"].includes(normalized);
}

const config = {
  port: parseInt(process.env.PORT || "3000", 10),
  s3Endpoint: process.env.S3_ENDPOINT,
  s3AccessKey: process.env.S3_ACCESS_KEY,
  s3SecretKey: process.env.S3_SECRET_KEY,
  s3Region: process.env.S3_REGION || "us-east-1",
  s3Bucket: process.env.S3_BUCKET,
  s3Prefix: process.env.S3_PREFIX || "",
  xmlServiceUrl: process.env.XML_SERVICE_URL,
  webhookUrl: process.env.WEBHOOK_URL,
  geocodingApiUrl:
    process.env.GEOCODING_API_URL || "https://nominatim.openstreetmap.org/search",
  geocodingCountry: process.env.GEOCODING_COUNTRY_CODE || "us",
  geocodingMinDelayMs: parseNonNegativeInt(
    process.env.GEOCODING_MIN_DELAY_MS,
    1000
  ),
  nominatimUserAgent:
    process.env.NOMINATIM_USER_AGENT || "tp3-processor/1.0",
  nominatimEmail: (process.env.NOMINATIM_EMAIL || "").trim(),
  weatherArchiveApiUrl:
    process.env.WEATHER_ARCHIVE_API_URL ||
    process.env.WEATHER_API_URL ||
    "https://archive-api.open-meteo.com/v1/archive",
  weatherForecastApiUrl:
    process.env.WEATHER_FORECAST_API_URL ||
    "https://api.open-meteo.com/v1/forecast",
  weatherHistoryDays: parseNonNegativeInt(
    process.env.WEATHER_HISTORY_DAYS,
    30
  ),
  weatherForecastDays: parseNonNegativeInt(
    process.env.WEATHER_FORECAST_DAYS,
    7
  ),
  pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS || "15000", 10),
  deleteAfterProcess: parseBoolean(process.env.DELETE_AFTER_PROCESS, true),
};

if (!config.s3Endpoint || !config.s3AccessKey || !config.s3SecretKey || !config.s3Bucket) {
  console.warn("Missing S3 configuration. Processor will not poll.");
}

const s3Client = new S3Client({
  endpoint: config.s3Endpoint,
  region: config.s3Region,
  credentials: {
    accessKeyId: config.s3AccessKey || "",
    secretAccessKey: config.s3SecretKey || "",
  },
  forcePathStyle: true,
});

const processedKeys = new Set();
let polling = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatDate(date) {
  return date.toISOString().slice(0, 10);
}

function parseFloatOrNull(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }
  const parsed = parseFloat(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeText(value) {
  if (!value) {
    return "";
  }
  return String(value).trim();
}

function extractParkCodeFromUrl(value) {
  if (!value) {
    return "";
  }
  const trimmed = String(value).trim();
  if (!trimmed) {
    return "";
  }
  const withoutHost = trimmed.replace(/^https?:\/\/[^/]+/i, "");
  const parts = withoutHost.split("/").filter(Boolean);
  return parts[0] || "";
}

function normalizeParkRecord(row) {
  const parkCode =
    row.ParkCode ||
    row.park_code ||
    row.parkCode ||
    row.PARKCODE ||
    "";
  const parkName =
    row.ParkName ||
    row.park_name ||
    row.parkName ||
    row.PARKNAME ||
    "";
  const stateCode =
    row.StateCode ||
    row.state_code ||
    row.stateCode ||
    row.STATECODE ||
    "";
  const stateName =
    row.StateName ||
    row.state_name ||
    row.stateName ||
    row.STATENAME ||
    "";
  const location = row.Location || row.location || "";
  const designation = row.Designation || row.designation || "";
  const description = row.Description || row.description || "";
  const parkUrl = row.ParkUrl || row.park_url || row.url || "";
  const crawlDate = row.CrawlDate || row.crawl_date || "";
  const latitude = parseFloatOrNull(row.Latitude || row.latitude || "");
  const longitude = parseFloatOrNull(row.Longitude || row.longitude || "");

  const fallbackCode = extractParkCodeFromUrl(parkUrl);

  return {
    parkCode: normalizeText(parkCode || fallbackCode).toUpperCase(),
    parkName: normalizeText(parkName),
    stateCode: normalizeText(stateCode).toUpperCase(),
    stateName: normalizeText(stateName),
    location: normalizeText(location),
    designation: normalizeText(designation),
    description: normalizeText(description),
    parkUrl: normalizeText(parkUrl),
    crawlDate: normalizeText(crawlDate),
    latitude,
    longitude,
  };
}

function buildGeocodeQuery(park) {
  const parts = [park.parkName];
  if (park.location) {
    parts.push(park.location);
  } else if (park.stateName || park.stateCode) {
    parts.push(park.stateName || park.stateCode);
  }
  if (config.geocodingCountry.toLowerCase() === "us") {
    parts.push("USA");
  }
  return parts.filter(Boolean).join(", ");
}

const geocodeCache = new Map();
let lastGeocodeAt = 0;

async function geocodePark(park) {
  if (!config.geocodingApiUrl || !park.parkName) {
    return null;
  }

  const query = buildGeocodeQuery(park);
  if (!query) {
    return null;
  }

  const cacheKey = query.toLowerCase();
  if (geocodeCache.has(cacheKey)) {
    return geocodeCache.get(cacheKey);
  }

  const sinceLast = Date.now() - lastGeocodeAt;
  if (config.geocodingMinDelayMs > 0 && sinceLast < config.geocodingMinDelayMs) {
    await sleep(config.geocodingMinDelayMs - sinceLast);
  }

  const params = new URLSearchParams({
    format: "json",
    limit: "1",
    addressdetails: "1",
    q: query,
  });
  if (config.geocodingCountry) {
    params.set("countrycodes", config.geocodingCountry);
  }
  if (config.nominatimEmail) {
    params.set("email", config.nominatimEmail);
  }

  let data = null;
  try {
    const response = await axios.get(
      `${config.geocodingApiUrl}?${params.toString()}`,
      {
        timeout: 10000,
        headers: {
          "User-Agent": config.nominatimUserAgent,
        },
      }
    );
    data = Array.isArray(response.data) ? response.data[0] : null;
  } catch (err) {
    console.warn(`Geocoding failed for "${query}"`, err.message);
  } finally {
    lastGeocodeAt = Date.now();
  }

  if (!data) {
    const empty = {
      latitude: null,
      longitude: null,
      county: "",
      municipality: "",
      stateName: "",
      displayName: "",
      source: config.geocodingApiUrl,
    };
    geocodeCache.set(cacheKey, empty);
    return empty;
  }

  const address = data.address || {};
  const municipality =
    address.city ||
    address.town ||
    address.village ||
    address.hamlet ||
    address.municipality ||
    "";
  const county = address.county || address.state_district || "";
  const stateName = address.state || "";

  const result = {
    latitude: parseFloatOrNull(data.lat),
    longitude: parseFloatOrNull(data.lon),
    county,
    municipality,
    stateName,
    displayName: data.display_name || "",
    source: config.geocodingApiUrl,
  };

  geocodeCache.set(cacheKey, result);
  return result;
}

const weatherCache = new Map();

function buildWeatherRange(days) {
  const normalizedDays = Number.isFinite(days) && days > 0 ? days : 1;
  const endDate = new Date();
  const startDate = new Date(endDate);
  startDate.setUTCDate(endDate.getUTCDate() - (normalizedDays - 1));
  return {
    startDate: formatDate(startDate),
    endDate: formatDate(endDate),
    days: normalizedDays,
  };
}

async function fetchWeatherHistory(latitude, longitude) {
  if (
    !config.weatherArchiveApiUrl ||
    config.weatherHistoryDays <= 0 ||
    latitude === null ||
    longitude === null
  ) {
    return [];
  }

  const cacheKey = `${latitude.toFixed(4)},${longitude.toFixed(4)}|archive|${config.weatherHistoryDays}`;
  if (weatherCache.has(cacheKey)) {
    return weatherCache.get(cacheKey);
  }

  const { startDate, endDate } = buildWeatherRange(config.weatherHistoryDays);
  const params = new URLSearchParams({
    latitude: latitude.toString(),
    longitude: longitude.toString(),
    daily: "temperature_2m_mean,precipitation_sum",
    start_date: startDate,
    end_date: endDate,
    timezone: "auto",
  });

  let response = null;
  try {
    response = await axios.get(
      `${config.weatherArchiveApiUrl}?${params.toString()}`,
      {
        timeout: 10000,
      }
    );
  } catch (err) {
    console.warn(
      `Weather API failed for ${latitude},${longitude}`,
      err.message
    );
    const empty = [];
    weatherCache.set(cacheKey, empty);
    return empty;
  }

  const daily = response.data?.daily || {};
  const dates = Array.isArray(daily.time) ? daily.time : [];
  const temps = Array.isArray(daily.temperature_2m_mean)
    ? daily.temperature_2m_mean
    : [];
  const precips = Array.isArray(daily.precipitation_sum)
    ? daily.precipitation_sum
    : [];

  const records = dates.map((date, index) => {
    return {
      date,
      tempMeanC: temps[index] ?? null,
      precipSumMm: precips[index] ?? null,
      precipProbability: null,
      timezone: response.data?.timezone || "",
      source: config.weatherArchiveApiUrl,
    };
  });

  weatherCache.set(cacheKey, records);
  return records;
}

async function fetchWeatherForecast(latitude, longitude) {
  if (
    !config.weatherForecastApiUrl ||
    config.weatherForecastDays <= 0 ||
    latitude === null ||
    longitude === null
  ) {
    return [];
  }

  const cacheKey = `${latitude.toFixed(4)},${longitude.toFixed(4)}|forecast|${config.weatherForecastDays}`;
  if (weatherCache.has(cacheKey)) {
    return weatherCache.get(cacheKey);
  }

  const params = new URLSearchParams({
    latitude: latitude.toString(),
    longitude: longitude.toString(),
    daily: "temperature_2m_mean,precipitation_probability_max",
    forecast_days: String(
      Number.isFinite(config.weatherForecastDays) && config.weatherForecastDays > 0
        ? config.weatherForecastDays
        : 1
    ),
    timezone: "auto",
  });

  let response = null;
  try {
    response = await axios.get(
      `${config.weatherForecastApiUrl}?${params.toString()}`,
      {
        timeout: 10000,
      }
    );
  } catch (err) {
    console.warn(
      `Forecast API failed for ${latitude},${longitude}`,
      err.message
    );
    const empty = [];
    weatherCache.set(cacheKey, empty);
    return empty;
  }

  const daily = response.data?.daily || {};
  const dates = Array.isArray(daily.time) ? daily.time : [];
  const temps = Array.isArray(daily.temperature_2m_mean)
    ? daily.temperature_2m_mean
    : [];
  const probs = Array.isArray(daily.precipitation_probability_max)
    ? daily.precipitation_probability_max
    : [];
  const today = formatDate(new Date());

  const records = dates
    .map((date, index) => ({
      date,
      tempMeanC: temps[index] ?? null,
      precipSumMm: null,
      precipProbability: probs[index] ?? null,
      timezone: response.data?.timezone || "",
      source: config.weatherForecastApiUrl,
    }))
    .filter((record) => record.date > today);

  weatherCache.set(cacheKey, records);
  return records;
}

async function sendToXmlService(csvContent, originalKey) {
  if (!config.xmlServiceUrl) {
    throw new Error("Missing XML_SERVICE_URL");
  }

  const form = new FormData();
  form.append("file", Buffer.from(csvContent), {
    filename: `enriched_${path.basename(originalKey)}`,
    contentType: "text/csv",
  });
  form.append("mapper_version", "v1");
  form.append("request_id", path.basename(originalKey, ".csv"));
  if (config.webhookUrl) {
    form.append("callback_url", config.webhookUrl);
  }

  const response = await axios.post(config.xmlServiceUrl, form, {
    headers: form.getHeaders(),
    timeout: 30000,
  });

  return response.data;
}

async function deleteObject(key) {
  if (!config.deleteAfterProcess) {
    return;
  }
  const deleteCommand = new DeleteObjectCommand({
    Bucket: config.s3Bucket,
    Key: key,
  });
  await s3Client.send(deleteCommand);
}

async function processObject(key) {
  const getCommand = new GetObjectCommand({
    Bucket: config.s3Bucket,
    Key: key,
  });
  const getResponse = await s3Client.send(getCommand);
  const enriched = [];
  const parser = parse({ columns: true, skip_empty_lines: true });
  getResponse.Body.pipe(parser);

  for await (const row of parser) {
    const park = normalizeParkRecord(row);
    if (!park.parkName) {
      continue;
    }

    const geocode = await geocodePark(park);
    const latitude = park.latitude ?? geocode?.latitude ?? null;
    const longitude = park.longitude ?? geocode?.longitude ?? null;
    const weatherDays = [
      ...(await fetchWeatherHistory(latitude, longitude)),
      ...(await fetchWeatherForecast(latitude, longitude)),
    ];

    if (weatherDays.length === 0) {
      continue;
    }

    for (const weather of weatherDays) {
      enriched.push({
        ParkCode: park.parkCode,
        ParkName: park.parkName,
        StateCode: park.stateCode,
        StateName: park.stateName || geocode?.stateName || "",
        Location: park.location,
        Designation: park.designation,
        Description: park.description,
        ParkUrl: park.parkUrl,
        CrawlDate: park.crawlDate,
        ObservationDate: weather.date || park.crawlDate || "",
        Latitude: latitude ?? "",
        Longitude: longitude ?? "",
        County: geocode?.county || "",
        Municipality: geocode?.municipality || "",
        GeoDisplayName: geocode?.displayName || "",
        TempMeanC: weather.tempMeanC ?? "",
        PrecipitationSumMm: weather.precipSumMm ?? "",
        PrecipitationProbability: weather.precipProbability ?? "",
        Timezone: weather.timezone || "",
        GeocodeSource: geocode?.source || "",
        WeatherSource: weather.source || "",
      });
    }
  }

  const csvOut = stringify(enriched, {
    header: true,
    columns: [
      "ParkCode",
      "ParkName",
      "StateCode",
      "StateName",
      "Location",
      "Designation",
      "Description",
      "ParkUrl",
      "CrawlDate",
      "ObservationDate",
      "Latitude",
      "Longitude",
      "County",
      "Municipality",
      "GeoDisplayName",
      "TempMeanC",
      "PrecipitationSumMm",
      "PrecipitationProbability",
      "Timezone",
      "GeocodeSource",
      "WeatherSource",
    ],
  });

  const result = await sendToXmlService(csvOut, key);
  console.log("Uploaded to XML service", result);
  await deleteObject(key);
}

async function pollBucket() {
  if (polling) {
    return;
  }
  if (!config.s3Bucket) {
    return;
  }

  polling = true;
  try {
    const listCommand = new ListObjectsV2Command({
      Bucket: config.s3Bucket,
      Prefix: config.s3Prefix,
    });
    const listResponse = await s3Client.send(listCommand);
    const items = listResponse.Contents || [];

    for (const item of items) {
      const key = item.Key;
      if (!key || !key.endsWith(".csv")) {
        continue;
      }
      if (processedKeys.has(key)) {
        continue;
      }

      console.log(`Processing ${key}`);
      try {
        await processObject(key);
        processedKeys.add(key);
      } catch (err) {
        console.error(`Failed to process ${key}`, err.message);
      }
    }
  } catch (err) {
    console.error("Polling failed", err.message);
  } finally {
    polling = false;
  }
}

const app = express();
app.use(express.json());

app.post("/webhook", (req, res) => {
  console.log("Webhook received", req.body);
  res.json({ ok: true });
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.listen(config.port, () => {
  console.log(`Processor listening on ${config.port}`);
  pollBucket();
  setInterval(pollBucket, config.pollIntervalMs);
});
