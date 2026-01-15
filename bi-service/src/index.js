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

  type Query {
    parkStats(state: String!, fromDate: String, toDate: String): ParkStats
  }
`;

const resolvers = {
  Query: {
    parkStats: (_parent, { state, fromDate, toDate }, { grpcClient }) =>
      new Promise((resolve, reject) => {
        grpcClient.QueryIncidentStats(
          {
            borough: state,
            from_date: fromDate || "",
            to_date: toDate || "",
          },
          (err, response) => {
            if (err) {
              return reject(err);
            }
            const parkCount = parseInt(response.incident_count, 10);
            resolve({
              state: response.borough,
              parkCount: Number.isFinite(parkCount) ? parkCount : 0,
              avgTempMeanC: response.avg_inspections_30d,
              avgPrecipitationValue: response.avg_not_approved_rate,
            });
          }
        );
      }),
  },
};

async function start() {
  const grpcClient = createClient(target);
  const server = new ApolloServer({ typeDefs, resolvers });
  await server.start();

  const app = express();
  app.get("/health", (_req, res) => res.json({ status: "ok" }));
  app.use(express.static(path.join(__dirname, "..", "public")));
  app.use(
    "/graphql",
    express.json(),
    expressMiddleware(server, {
      context: async () => ({ grpcClient }),
    })
  );

  app.listen(port, () => {
    console.log(`BI service listening on ${port}`);
  });
}

start().catch((err) => {
  console.error("Failed to start BI service", err);
  process.exit(1);
});
