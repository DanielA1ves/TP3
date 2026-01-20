const path = require("path");
const express = require("express");
const { ApolloServer } = require("@apollo/server");
const { expressMiddleware } = require("@apollo/server/express4");
const { createClient } = require("./grpcClient");

const port = parseInt(process.env.PORT || "4000", 10);
const target = process.env.GRPC_TARGET || "localhost:9090";

const typeDefs = `#graphql
  type ParkStats {
    state: String!
    parkCount: Int!
    avgTempMeanC: Float!
    avgPrecipitationValue: Float!
  }

  type ParkTemperatureRank {
    parkCode: String!
    parkName: String
    avgTempMeanC: Float!
    avgPrecipitationValue: Float!
    observationCount: Int!
  }

  type DailyStateSummary {
    date: String!
    parkCount: Int!
    avgTempMeanC: Float!
    avgPrecipitationValue: Float!
  }

  type Query {
    parkStats(state: String!, fromDate: String, toDate: String): ParkStats
    topParksByTemp(state: String!, fromDate: String, toDate: String, limit: Int = 5): [ParkTemperatureRank!]!
    dailyStateSummary(state: String!, fromDate: String, toDate: String): [DailyStateSummary!]!
  }
`;

function callUnary(client, method, payload) {
  return new Promise((resolve, reject) => {
    client[method](payload, (err, response) => {
      if (err) {
        return reject(err);
      }
      resolve(response || {});
    });
  });
}

function resolveLimit(limit) {
  const parsed = Number(limit);
  if (Number.isFinite(parsed) && parsed > 0) {
    return Math.min(parsed, 50);
  }
  return 5;
}

const grpcClient = createClient(target);

const resolvers = {
  Query: {
    parkStats: async (_parent, { state, fromDate, toDate }) => {
      const response = await callUnary(grpcClient, "QueryParkStats", {
        state,
        from_date: fromDate || "",
        to_date: toDate || "",
      });
      const parkCount = parseInt(response.park_count, 10);
      return {
        state: response.state || state,
        parkCount: Number.isFinite(parkCount) ? parkCount : 0,
        avgTempMeanC: Number(response.avg_temp_mean_c) || 0,
        avgPrecipitationValue: Number(response.avg_precipitation_value) || 0,
      };
    },
    topParksByTemp: async (_parent, { state, fromDate, toDate, limit }) => {
      const response = await callUnary(grpcClient, "QueryTopParksByTemp", {
        state,
        from_date: fromDate || "",
        to_date: toDate || "",
        limit: resolveLimit(limit),
      });
      const items = Array.isArray(response.items) ? response.items : [];
      return items.map((item) => ({
        parkCode: item.park_code || "",
        parkName: item.park_name || "",
        avgTempMeanC: Number(item.avg_temp_mean_c) || 0,
        avgPrecipitationValue: Number(item.avg_precipitation_value) || 0,
        observationCount: parseInt(item.observation_count, 10) || 0,
      }));
    },
    dailyStateSummary: async (_parent, { state, fromDate, toDate }) => {
      const response = await callUnary(grpcClient, "QueryDailyStateSummary", {
        state,
        from_date: fromDate || "",
        to_date: toDate || "",
      });
      const items = Array.isArray(response.items) ? response.items : [];
      return items.map((item) => ({
        date: item.date || "",
        parkCount: parseInt(item.park_count, 10) || 0,
        avgTempMeanC: Number(item.avg_temp_mean_c) || 0,
        avgPrecipitationValue: Number(item.avg_precipitation_value) || 0,
      }));
    },
  },
};

async function start() {
  const server = new ApolloServer({ typeDefs, resolvers });
  await server.start();

  const app = express();
  app.get("/health", (_req, res) => res.json({ status: "ok" }));
  app.use(express.static(path.join(__dirname, "..", "public")));
  app.use("/graphql", express.json(), expressMiddleware(server));

  app.listen(port, () => {
    console.log(`BI service listening on ${port}`);
  });
}

start().catch((err) => {
  console.error("Failed to start BI service", err);
  process.exit(1);
});
