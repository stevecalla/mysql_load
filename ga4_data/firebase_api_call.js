// npm install googleapis@105 @google-cloud/local-auth@2.1.0 --save
// npm install @google-analytics/data
// https://www.youtube.com/watch?v=oRUpAqYqROQ
// https://stackoverflow.com/questions/77102135/how-to-get-google-analytics-report-api-ga4-for-node-js

// const { authenticate } = require('@google-cloud/local-auth');
const { initializeApp } = require('firebase-admin/app');
const { getAnalytics } = require('firebase/analytics');
const EZHIRE_FIREBASE_CREDENTIALS_PATH = require('./auth_certs/test15092015-1069-firebase-adminsdk-jy7kn-f77d1ee68d.json'); // auth via Firebase, Project Settings, Service Accounts

// If modifying these scopes, delete token.json.
const SCOPES = ['https://www.googleapis.com/auth/admin.directory.user.readonly'];

// query
const propertyId = '277875935670';
const request = { "property": `properties/${propertyId}`, "dimensions": [{ "name": "date" }], "metrics": [{ "name": "sessions" }, { "name": "sessionConversionRate" }, { "name": "sessionConversionRate:form_initiation_website" }, { "name": "sessionConversionRate:form_submission" }, { "name": "sessionConversionRate:form_submission_website" }, { "name": "sessionConversionRate:purchase" }, { "name": "sessionConversionRate:thanks" }], "dateRanges": [{ "startDate": "30daysAgo", "endDate": "yesterday" }], "limit": "10" };

// Runs a Google Analytics report for a given property id
async function runReport(propertyId) {

    // Imports the Google Analytics Data API client library.
    const { BetaAnalyticsDataClient } = require('@google-analytics/data');

    // const analyticsDataClient = new BetaAnalyticsDataClient(
    //     // { credentials: CALLA_GA4_CREDENTIALS_PATH }
    //     { credentials: EZHIRE_FIREBASE_CREDENTIALS_PATH }
    // );

    // Initialize Firebase
    // const app = initializeApp(EZHIRE_FIREBASE_CREDENTIALS_PATH);


// Initialize Analytics and get a reference to the service
    const analytics = getAnalytics(EZHIRE_FIREBASE_CREDENTIALS_PATH);

    // Runs a simple report.
    async function runReport() {
        const [response] = await analyticsDataClient.runReport(request);

        console.log('Report result:');
        // console.log(response);

        // Column headers
        const { name: date } = response.dimensionHeaders[0];
        const { metricHeaders: columnTitles } = response;
        const columns = [date, ...columnTitles.map(header => header.name)];

        // Log table header
        console.log(columns.join('\t'));

        // Log each row
        const test = response.rows.map(row => {
            const dateString = row.dimensionValues[0].value;
            const year = dateString.slice(0, 4);
            const month = dateString.slice(4, 6);
            const day = dateString.slice(6, 8);
            const formattedDate = `${year}-${month}-${day}`;

            const [a, b, c, d, e, f, g] = row.metricValues;

            return {
                date: formattedDate,
                "sessions": a.value,
                // "sessions2": b.value,
                // "sessions3": c.value,
                // "sessions4": d.value,
                // "sessions5": e.value,
                // "sessions6": f.value,
                // "sessions7": g.value,
            }

        });
        
        test.sort(function (a, b) {
            return new Date(b.date) - new Date(a.date)
        });
        
        console.log(test);
    }

    await runReport();
}

runReport().then().catch(console.error);