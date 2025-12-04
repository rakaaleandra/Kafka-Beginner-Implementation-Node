require('dotenv').config();

async function test() {
  const apiKey = process.env.API_KEY;
  const id = 'A416908';
  const url = `https://api.waqi.info/feed/${id}/?token=${apiKey}`;

  const response = await fetch(url);
  console.log("Status:", response.status);
  console.log("Result:", await response.text());
}

test();