"use strict";
const fs = require("fs");
const pg = require("pg");
const axios = require("axios");

const config = {
    connectionString: "postgres://candidate:<password>@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
    ssl: {
        rejectUnauthorized: true,
        ca: fs
            .readFileSync("<file_source>")
            .toString(),
    },
};

const conn = new pg.Client(config);

conn.connect((err) => {
    if (err) throw err;
    console.log("Connected to the database");
});

const createTableQuery = `
    CREATE TABLE IF NOT EXISTS gevorggasparyan (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        data JSONB NOT NULL
    );
`;

conn.query(createTableQuery, (err, res) => {
    if (err) throw err;
    console.log("Table is ready");
});

const fetchAndInsertCharacters = async () => {
    try {
        let page = 1;
        const queries = [];
        while (true) {
            try {
                const response = await fetch(`https://rickandmortyapi.com/api/character?page=${page}`);
                if (response.status === 404) {
                    console.log("No more pages to fetch.");
                    break;
                }
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                const data = await response.json();
                const results = data.results;
                if (!results || results.length === 0) break; // Stops fetching if no results
                for (let character of results) {
                    console.log(`Saving character: ${JSON.stringify(character, null, 2)}`);
                    const insertQuery = `
                        INSERT INTO characters (name, data)
                        VALUES ($1, $2)
                        RETURNING id;
                    `;
                    const values = [character.name, character];
                    queries.push(conn.query(insertQuery, values));
                }
                page++;
            } catch (error) {
                console.error("Error fetching data from API:", error);
                break;
            }
        }
        await Promise.all(queries);
    } catch (error) {
        console.error("Error fetching data from API:", error);
    } finally {
        conn.end((err) => {
            if (err) throw err;
            console.log("Disconnected from the database");
        });
    }
};

fetchAndInsertCharacters();
