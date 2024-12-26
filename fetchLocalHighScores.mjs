import mysql from 'mysql2/promise';

// RDS configuration (mock values)
const dbConfig = {
  host: 'mock-database-host.com', // Mock database host
  user: 'mock-user',             // Mock username
  password: 'mock-password',     // Mock password
  database: 'mock-database',     // Mock database name
  port: 3306,                    // Default MySQL port
};

const TOP_LIMIT = 20; // Limit to the top 20 entries

export const handler = async (event) => {
  console.log("Received event with query parameters: ", event.queryStringParameters);

  const type = event.queryStringParameters.type;
  const playerId = event.queryStringParameters.playerId; // Fetch playerId from query params
  const countryCode = event.queryStringParameters.countryCode; // Fetch countryCode from query params

  if (type === "localHighScores") {
    console.log("Fetching local high score leaderboard for country code: ", countryCode);

    try {
      // Create a connection to the database
      const connection = await mysql.createConnection(dbConfig);

      // Query to get the top high scores for the specified country
      const [rows] = await connection.execute(
        `SELECT p.playerId, p.pName, h.highScore 
         FROM Players p
         JOIN PlayerActivity h ON p.playerId = h.playerId
         WHERE p.countryCode = ? 
         ORDER BY h.highScore DESC 
         LIMIT ?`,
        [countryCode, TOP_LIMIT]
      );

      console.log(`Fetched ${rows.length} high scores for country code ${countryCode}.`);

      // Determine player's rank and own high score
      let rank = -1;
      let playerHighScore = -1;
      let playerInTop = false;

      // Check if the player is in the top scores
      for (let i = 0; i < rows.length; i++) {
        if (rows[i].playerId === playerId) {
          rank = i + 1; // Rank is 1-based
          playerHighScore = rows[i].highScore;
          playerInTop = true;
          break;
        }
      }

      // If the player is not in the top scores, get their rank and score separately
      if (!playerInTop) {
        const [playerRow] = await connection.execute(
          `SELECT h.highScore 
           FROM Players p
           JOIN PlayerActivity h ON p.playerId = h.playerId
           WHERE p.playerId = ? AND p.countryCode = ?`,
          [playerId, countryCode]
        );

        if (playerRow.length > 0) {
          playerHighScore = playerRow[0].highScore;

          // Optimized query to get the player's rank using an indexed `highScore` column and countryCode filter
          const [[{ rank: playerRank }]] = await connection.execute(
            `SELECT COUNT(*) + 1 AS rank 
             FROM PlayerActivity h
             JOIN Players p ON p.playerId = h.playerId
             WHERE h.highScore > ? AND p.countryCode = ?`,
            [playerHighScore, countryCode]
          );

          rank = playerRank;
        }
      }

      // Close the database connection
      await connection.end();

      // Prepare the top players list
      const topPlayers = rows.map((player, index) => ({
        playerId: player.playerId,
        pName: player.pName || "Unknown",
        highScore: player.highScore,
        rank: index + 1,
      }));

      // Include the player's own score if they're not in the top scores
      if (!playerInTop && rank !== -1) {
        topPlayers.push({
          playerId,
          pName: "Unknown",
          highScore: playerHighScore,
          rank: rank,
        });

        console.log(`Player ${playerId} is outside the top ${TOP_LIMIT} for country ${countryCode} but added with rank: ${rank}`);
      }

      return sendResponse(200, {
        op: "GET",
        status: "OK",
        topScores: topPlayers,
        ownRank: rank,
        ownHighScore: playerHighScore,
      });
    } catch (error) {
      console.error(`Error querying MySQL for local high score leaderboard: ${error}`);
      return sendResponse(500, { message: "Internal server error" });
    }
  }

  return sendResponse(400, { message: "Invalid request type" });
};

// Helper function to send responses
function sendResponse(statusCode, message) {
  return {
    statusCode: statusCode,
    body: JSON.stringify(message),
  };
}
