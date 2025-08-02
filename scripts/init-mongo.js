db.createUser({
  user: "hvacuser",
  pwd: "hvacpass",
  roles: [
    { role: "readWrite", db: "hvac_db" },
    { role: "readAnyDatabase", db: "admin" }
  ]
});