/**
 * Sends a POST request to the specified API URL with a given JSON body.
 *
 * @param {string} apiurl - The URL of the API endpoint.
 * @param {object} body - The JSON object to be sent in the request body.
 * @returns {Promise<Response>} - The fetch API response promise.
 */
async function sendPostRequest(apiurl, body) {
    // Convert the body object to a JSON string
    const bodyString = JSON.stringify(body);
  
    // Create the request payload with headers and body
    const requestPayload = {
      resource: '/',
      path: '/',
      httpMethod: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      requestContext: {},
      body: bodyString,
      isBase64Encoded: false,
    };
  
    try {
      // Send the POST request using fetch
      const response = await fetch(apiurl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestPayload),
      });
  
      // Check if the response is okay (status in the range 200-299)
      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
      }
  
      // Parse and return the response JSON
      return await response.json();
    } catch (error) {
      console.error('Error making POST request:', error);
      throw error;
    }
  }
  