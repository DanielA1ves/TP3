const grpc = require("@grpc/grpc-js");
const protoLoader = require("@grpc/proto-loader");
const path = require("path");

const protoPath = path.join(__dirname, "..", "proto", "xmlservice.proto");

const packageDefinition = protoLoader.loadSync(protoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const xmlservice = grpc.loadPackageDefinition(packageDefinition).xmlservice;

function createClient(target) {
  return new xmlservice.XmlService(target, grpc.credentials.createInsecure());
}

module.exports = { createClient };
