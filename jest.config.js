let moduleNameMapping = {};

module.exports = {
  coverageProvider: "v8",
  preset: 'ts-jest',
  testEnvironment: 'node',
  testPathIgnorePatterns: [
    "/node_modules/",
  ],
  moduleNameMapper: moduleNameMapping,
};
