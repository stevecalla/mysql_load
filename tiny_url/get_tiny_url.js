// https://tinyurl.com/app/dev
const urlsArray = require('./url_list');
const { urls_array } = require('./url_list');

// setup .env file
const dotenv = require('dotenv');
dotenv.config({path: "../.env"}); 

// set tinyURL path
const TINY_URL_PATH_CREATE = `https://api.tinyurl.com/create?api_token=${process.env.TINY_URL_KEY}`;
const TINY_URL_PATH_BULK = `https://api.tinyurl.com/bulk?api_token=${process.env.TINY_URL_KEY}`;
const TINY_URL_PATH_GET_LIST = `https://api.tinyurl.com/urls/available?from=2024-05-27%2009%3A00%3A00%20CDT&to=2024-09-23%2014%3A00%3A00%20CDT`;

// TINY URL API CALL
const getTinyURL_create = async (token, data = {}) => {
//   const encodedURI = createURL(token);

  const response = await fetch(TINY_URL_PATH_CREATE, {
    method: "POST",
    mode: "cors",
    cache: "no-cache",
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
    },
    redirect: "follow",
    referrerPolicy: "no-referrer",
    body: JSON.stringify({
    //   url: encodedURI.encodedURI,
      url: "https://www.example.com/my-really-long-link-that-I-need-to-shorten/84378949",
      domain: "tiny.one",
    }),
  });

  return response.json(); // parses JSON response into native JavaScript objects
};

// STEP #0: CREATE ARRAY OF BULK URLS
const create_bulk_url = async() => {
    console.log('array length =', urls_array.length);

    const bulk_urls = urls_array.map(url => {
        return {
          operation: "create",
          metadata: [],
          url: url,
          domain: "tinyurl.com",
        };
    });

    const result_object = { items: bulk_urls }; // THE API ERRORS IF USING A JSON OBJECT

    return result_object;
};

// STEP #1: CREATE BULK TINY URLS
const create_bulk_tiny_urls = async (bulk_urls) => {
    console.log(bulk_urls);
    const response = await fetch(TINY_URL_PATH_BULK, {
      method: "POST",
      mode: "cors",
      cache: "no-cache",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
      },
      redirect: "follow",
      referrerPolicy: "no-referrer",
      body: JSON.stringify(bulk_urls),
    });
  
    return response.json(); // parses JSON response into native JavaScript objects
};

// TINY URL API CALL
const getTinyURL_get_list = async () => {

    const response = await fetch(TINY_URL_PATH_GET_LIST, {
    method: "GET",
    mode: "cors",
    cache: "no-cache",
    credentials: "same-origin",
    headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.TINY_URL_KEY}`, // Assuming you have an API token
    },
    redirect: "follow",
    referrerPolicy: "no-referrer",
    });

    return response.json(); // parses JSON response into native JavaScript objects
};

// TINY URL API CALL
const getTinyURL_get_details = async (alias) => {

    let url = `https://api.tinyurl.com/alias/tinyurl.com/${alias}`;

    const response = await fetch(url, {
    method: "GET",
    mode: "cors",
    cache: "no-cache",
    credentials: "same-origin",
    headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.TINY_URL_KEY}`, // Assuming you have an API token
    },
    redirect: "follow",
    referrerPolicy: "no-referrer",
    });

    return response.json(); // parses JSON response into native JavaScript objects
};

// Call getTinyURL asynchronously using an async function
const getResult = async () => {
    try {
    //   let result_create = await getTinyURL_create();
    //   console.log(result_create);

    // STEP #0: CREATE ARRAY OF BULK URLS
    // let bulk_urls = await create_bulk_url();
    // console.log(bulk_urls);

    // STEP #1: CREATE BULK TINY URLS
    // let result_tiny_urls = await create_bulk_tiny_urls(bulk_urls);
    // console.log(result_tiny_urls);

    // USED THE BULK DOWNLOAD ICON IN TINY URL TO GET THE LIST OF URLS CREATED
    // THUS DID NOT FINISH THE CODE BELOW

    // STEP #2: GET LIST OF TINY URLS
    //   let result_list = await getTinyURL_get_list();
    //   console.log(result_list);

    // STEP #3: GET DETAILS FOR EACH SHORT URL
    //   console.log(result_list.length);
    //   let { data } = result_list;

    //   console.log(data.length);
    //   for(let i = 0; i < data.length; i++) {
    //       let result_details = await getTinyURL_get_details(data[i].alias);
    //       let { url, tiny_url } = result_details.data;
    //       console.log(url, ' ' , tiny_url);
    //   }

    } catch (error) {
      console.error("Error fetching tiny URL:", error);
    }
  };
  
getResult(); // Call the async function to fetch and log the result

// EXAMPLE OF BULK_URL BODY CREATED IN STEP #0 ABOVE
// const bulk_url = {
//   "items": [
//     {
//       "operation": "create",
//       "metadata": [],
//       "url": "https://www.example.com/my-really-long-link-that-I-need-to-shorten/84378949",
//       "domain": "tinyurl.com",
//     },
//     {
//       "operation": "create",
//       "metadata": [],
//       "url": "https://www.example.com/my-really-long-link-that-I-need-to-shorten/84378950",
//       "domain": "tinyurl.com",
//     },
//     {
//       "operation": "create",
//       "metadata": [],
//       "url": "https://www.example.com/my-really-long-link-that-I-need-to-shorten/84378951",
//       "domain": "tinyurl.com",
//     },
//   ]
// };
